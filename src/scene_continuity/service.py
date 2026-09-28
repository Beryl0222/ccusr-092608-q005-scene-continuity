"""镜头连续性工单服务：事件溯源 + 投影 + 命令原子提交。

不变量：
- 命令处理器是纯函数：只读投影状态，只产出事件规格，不直接修改状态；
  所有状态变化都经过 ``_apply`` 投影，事件日志是唯一事实来源。
- 同一进程锁内完成“检查 + 追加事件”，两个剪辑方案对同一候选的锁竞争
  只有一方能追加 ``SHOT_LOCKED``，即原子裁决。
- 新事件标识由幂等键确定性派生，恢复后重放未完成命令不会产生重复事实。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Any, Literal

from .clock import Clock
from .errors import (
    BudgetExceeded,
    CandidateIsolationConflict,
    DomainError,
    DuplicateCandidate,
    InvalidState,
    LockConflict,
    NotFound,
    PermissionDenied,
    ScheduleConflict,
    ValidationError,
)
from .store import EventStore, StoredEvent

Role = Literal["observer", "continuity_lead", "producer", "editor"]

# 角色面貌 / 服装道具 / 空间方向 / 动作承接
DIMENSIONS = frozenset({"appearance", "wardrobe_prop", "screen_direction", "action_carry"})


# --------------------------------------------------------------------------- #
# 投影状态
# --------------------------------------------------------------------------- #


@dataclass
class CandidateState:
    candidate_ref: str
    clip_ref: str
    batch_ref: str
    content_hash: str
    shot_ref: str
    scene_ref: str
    summary: str
    received_at: datetime
    withdrawn: bool = False
    withdraw_reason: str | None = None
    handles_to: set[str] = field(default_factory=set)  # 本候选 -> 下游候选
    handles_from: set[str] = field(default_factory=set)  # 上游候选 -> 本候选
    ticket_refs: set[str] = field(default_factory=set)


@dataclass
class TicketState:
    ticket_ref: str
    shot_ref: str
    candidate_ref: str
    dimension: str
    evidence: str
    opened_at: datetime
    status: Literal["open", "reopened", "approved", "closed"] = "open"
    reopen_count: int = 0
    root_candidate_ref: str | None = None
    regen_request_ref: str | None = None
    estimated_cost: float = 0.0
    delivered: bool = False


@dataclass
class RegenRequestState:
    request_ref: str
    ticket_ref: str
    shot_ref: str
    reason: str
    estimated_cost: float
    deadline: datetime
    requested_by: str
    status: Literal["pending", "approved", "rejected"] = "pending"
    decided_by: str | None = None


@dataclass
class EditState:
    edit_ref: str
    plan_ref: str
    deadline: datetime
    # 同一方案对同一镜头的最新锁定（历史在事件日志中）
    locks: dict[str, str] = field(default_factory=dict)


@dataclass
class ReleaseState:
    release_ref: str
    edit_ref: str
    revision: int
    delivered_at: datetime
    selections: dict[str, str]
    revision_note: str


@dataclass
class ObservationRecord:
    at: datetime
    by: str | None
    dimension: str
    evidence: str
    candidate_ref: str | None
    shot_ref: str | None
    kind: str  # observation / issue / constraint


@dataclass
class StreamState:
    scene_ref: str | None = None
    frozen: bool = False
    frozen_at: datetime | None = None
    budget_total: float = 0.0
    budget_used: float = 0.0
    delivery_deadline: datetime | None = None
    candidates: dict[str, CandidateState] = field(default_factory=dict)
    clips: dict[str, list[str]] = field(default_factory=dict)
    tickets: dict[str, TicketState] = field(default_factory=dict)
    regen_requests: dict[str, RegenRequestState] = field(default_factory=dict)
    edits: dict[str, EditState] = field(default_factory=dict)
    # shot_ref -> (edit_ref, candidate_ref)：镜头级锁的唯一裁决点
    shot_locks: dict[str, tuple[str, str]] = field(default_factory=dict)
    releases: list[ReleaseState] = field(default_factory=list)
    observations: list[ObservationRecord] = field(default_factory=list)
    versions: dict[tuple[str, str], int] = field(default_factory=dict)

    def candidate(self, ref: str) -> CandidateState:
        try:
            return self.candidates[ref]
        except KeyError:
            raise NotFound(f"候选 {ref} 不存在")

    def ticket(self, ref: str) -> TicketState:
        try:
            return self.tickets[ref]
        except KeyError:
            raise NotFound(f"工单 {ref} 不存在")

    def edit(self, ref: str) -> EditState:
        try:
            return self.edits[ref]
        except KeyError:
            raise NotFound(f"剪辑方案 {ref} 不存在")

    def valid_locks(self, edit_ref: str) -> dict[str, str]:
        """剔除被撤销候选的失效锁；交付只承认仍然有效的选择。"""
        edit = self.edits[edit_ref]
        return {
            shot: cand
            for shot, cand in edit.locks.items()
            if cand in self.candidates and not self.candidates[cand].withdrawn
            and self.shot_locks.get(shot) == (edit_ref, cand)
        }


def _release_locks(state: StreamState, candidate_refs: set[str]) -> None:
    """释放引用了已失效候选的镜头锁（事件历史仍保留，不影响溯源）。"""
    for shot, holder in list(state.shot_locks.items()):
        edit_ref, locked = holder
        if locked in candidate_refs:
            state.shot_locks.pop(shot, None)
            edit = state.edits.get(edit_ref)
            if edit is not None:
                edit.locks.pop(shot, None)


def _apply(state: StreamState, event: StoredEvent) -> StreamState:
    state.versions[(event.aggregate_type, event.aggregate_id)] = event.version
    p = event.payload
    t = event.event_type

    if t == "BASELINE_FROZEN":
        state.scene_ref = event.aggregate_id
        state.frozen = True
        state.frozen_at = event.occurred_at
        state.budget_total = float(p.get("budget_total", 0.0))
        if p.get("delivery_deadline"):
            state.delivery_deadline = datetime.fromisoformat(p["delivery_deadline"])

    elif t == "CANDIDATE_RECEIVED":
        c = CandidateState(
            candidate_ref=event.aggregate_id,
            clip_ref=p["clip_ref"],
            batch_ref=p["batch_ref"],
            content_hash=p["content_hash"],
            shot_ref=p["shot_ref"],
            scene_ref=p.get("scene_ref", ""),
            summary=p.get("summary", ""),
            received_at=event.occurred_at,
        )
        state.candidates[c.candidate_ref] = c
        state.clips.setdefault(c.clip_ref, []).append(c.candidate_ref)

    elif t == "CONTINUITY_EDGE_LINKED":
        frm = state.candidate(p["from_candidate_ref"])
        to = state.candidate(p["to_candidate_ref"])
        frm.handles_to.add(to.candidate_ref)
        to.handles_from.add(frm.candidate_ref)

    elif t == "OBSERVATION_RECORDED":
        state.observations.append(
            ObservationRecord(
                event.occurred_at, p.get("recorded_by"), p["dimension"], p["evidence"],
                p.get("candidate_ref"), p.get("shot_ref"), "observation",
            )
        )

    elif t == "CONSTRAINT_CONFIRMED":
        state.observations.append(
            ObservationRecord(
                event.occurred_at, p.get("recorded_by"), "constraint", p["constraint"],
                p.get("candidate_ref"), p.get("shot_ref"), "constraint",
            )
        )

    elif t == "ISSUE_RECORDED":
        state.observations.append(
            ObservationRecord(
                event.occurred_at, p.get("recorded_by"), p["dimension"], p["evidence"],
                p.get("candidate_ref"), p.get("shot_ref"), "issue",
            )
        )
        ref = event.aggregate_id
        state.tickets[ref] = TicketState(
            ticket_ref=ref,
            shot_ref=p["shot_ref"],
            candidate_ref=p["candidate_ref"],
            dimension=p["dimension"],
            evidence=p["evidence"],
            opened_at=event.occurred_at,
        )
        state.candidate(p["candidate_ref"]).ticket_refs.add(ref)

    elif t == "TICKET_REOPENED":
        tk = state.ticket(p["root_ticket_ref"])
        tk.status = "reopened"
        tk.reopen_count += 1
        tk.root_candidate_ref = p["root_candidate_ref"]

    elif t == "REGEN_REQUESTED":
        r = RegenRequestState(
            request_ref=event.aggregate_id,
            ticket_ref=p["ticket_ref"],
            shot_ref=p["shot_ref"],
            reason=p.get("reason", ""),
            estimated_cost=float(p["estimated_cost"]),
            deadline=datetime.fromisoformat(p["deadline"]),
            requested_by=p.get("requested_by", ""),
        )
        state.regen_requests[r.request_ref] = r
        tk = state.ticket(r.ticket_ref)
        tk.regen_request_ref = r.request_ref
        tk.estimated_cost = r.estimated_cost

    elif t == "REGEN_APPROVED":
        r = state.regen_requests[p["request_ref"]]
        r.status = "approved"
        r.decided_by = p.get("decided_by")
        state.budget_used += r.estimated_cost
        state.ticket(r.ticket_ref).status = "approved"

    elif t == "REGEN_REJECTED":
        r = state.regen_requests[p["request_ref"]]
        r.status = "rejected"
        r.decided_by = p.get("decided_by")

    elif t == "EDIT_OPENED":
        state.edits[p["edit_ref"]] = EditState(
            edit_ref=p["edit_ref"],
            plan_ref=p["plan_ref"],
            deadline=datetime.fromisoformat(p["deadline"]),
        )

    elif t == "SHOT_LOCKED":
        shot = p["shot_ref"]
        state.shot_locks[shot] = (p["edit_ref"], p["candidate_ref"])
        state.edit(p["edit_ref"]).locks[shot] = p["candidate_ref"]

    elif t == "CANDIDATE_WITHDRAWN":
        c = state.candidate(p["candidate_ref"])
        c.withdrawn = True
        c.withdraw_reason = p["reason"]
        _release_locks(state, {c.candidate_ref})

    elif t == "REWORK_PROPAGATED":
        # 受连锁影响的下游已选片段失去入点，释放其镜头锁；
        # 曾经的选择仍完整保留在事件日志中，供最终镜头解释使用。
        _release_locks(state, set(p.get("affected_candidate_refs", [])))

    elif t == "RELEASE_DELIVERED":
        release = ReleaseState(
            release_ref=p["release_ref"],
            edit_ref=p["edit_ref"],
            revision=int(p["revision"]),
            delivered_at=event.occurred_at,
            selections=dict(p["selections"]),
            revision_note=p.get("revision_note", ""),
        )
        state.releases.append(release)
        for shot in release.selections:
            for tk in state.tickets.values():
                if tk.shot_ref == shot:
                    tk.delivered = True

    return state


# --------------------------------------------------------------------------- #
# 命令与回执
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Command:
    name: str
    actor: str
    role: Role
    args: dict[str, Any]
    idempotency_key: str


@dataclass(frozen=True)
class EventSpec:
    event_type: str
    aggregate_type: str
    aggregate_id: str
    payload: dict[str, Any]


@dataclass
class Receipt:
    ok: bool
    result: dict[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    error_message: str | None = None
    events: list[StoredEvent] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "result": self.result,
            "error_code": self.error_code,
            "error_message": self.error_message,
        }


def _require(condition: bool, message: str, error: type[DomainError] = ValidationError) -> None:
    if not condition:
        raise error(message)


def _dt(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError("时间必须携带时区")
    return parsed


# --------------------------------------------------------------------------- #
# 服务
# --------------------------------------------------------------------------- #


class ContinuityService:
    def __init__(self, clock: Clock, store: EventStore | None = None, *,
                 scene_ref: str = "scene-001") -> None:
        self.clock = clock
        self.scene_ref = scene_ref
        self.store = store if store is not None else EventStore()
        self.state = self._fold()
        self._lock = Lock()
        self._queue: deque[Command] = deque()
        self._receipts: dict[str, Receipt] = {}
        self._commands: dict[str, Command] = {}

    @staticmethod
    def _signature(command: Command) -> tuple[str, str, Role, tuple[tuple[str, Any], ...]]:
        return command.name, command.actor, command.role, tuple(sorted(command.args.items()))

    # ----- 队列与恢复 -----
    def enqueue(self, command: Command) -> None:
        self._queue.append(command)

    def restore_queue(self, commands: list[Command]) -> list[Receipt]:
        """服务恢复后继续未完成队列。

        已完成命令（事件日志中已有其幂等键）直接跳过；未完成命令照常执行。
        同一条未完成命令重放时，事件标识与签名不变，存储按幂等键去重。
        """
        done = self.store.completed_keys() | set(self._receipts)
        for command in commands:
            if command.idempotency_key not in done:
                self._queue.append(command)
        return self.drain_pending()

    def drain_pending(self) -> list[Receipt]:
        return [self.submit(self._queue.popleft()) for _ in range(len(self._queue))]

    # ----- 提交 -----
    def submit(self, command: Command) -> Receipt:
        with self._lock:
            cached = self._receipts.get(command.idempotency_key)
            if cached is not None:
                original = self._commands[command.idempotency_key]
                if self._signature(command) != self._signature(original):
                    conflict = Receipt(
                        False,
                        error_code="idempotency_key_mismatch",
                        error_message=f"幂等键 {command.idempotency_key!r} 已用于不同命令，拒绝覆盖",
                    )
                    return conflict
                return cached
            try:
                specs, result = self._dispatch(command)
                next_versions = dict(self.state.versions)
                stored = []
                for index, spec in enumerate(specs):
                    version = next_versions.get((spec.aggregate_type, spec.aggregate_id), 0) + 1
                    next_versions[(spec.aggregate_type, spec.aggregate_id)] = version
                    stored.append(
                        self.store.append(
                            event_id=f"evt-{command.idempotency_key}-{index}",
                            event_type=spec.event_type,
                            aggregate_type=spec.aggregate_type,
                            aggregate_id=spec.aggregate_id,
                            occurred_at=self.clock.now(),
                            version=version,
                            payload=spec.payload,
                            idempotency_key=f"{command.idempotency_key}#{index}",
                        )
                    )
                self.state = self._fold()
                receipt = Receipt(True, result, events=stored)
            except DomainError as exc:
                receipt = Receipt(False, error_code=exc.code, error_message=exc.message)
            self._receipts[command.idempotency_key] = receipt
            self._commands[command.idempotency_key] = command
            return receipt

    def _fold(self) -> StreamState:
        state = StreamState()
        for event in self.store.events():
            _apply(state, event)
        return state

    @staticmethod
    def _role(command: Command, allowed: frozenset[Role]) -> None:
        if command.role not in allowed:
            raise PermissionDenied(f"角色 {command.role} 无权执行 {command.name}")

    def _dispatch(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        if command.name == "freeze_baseline":
            return self._cmd_freeze_baseline(command)
        handler = getattr(self, f"_cmd_{command.name}", None)
        if handler is None:
            raise ValidationError(f"未知命令 {command.name}")
        _require(self.state.frozen, "场次基线尚未冻结", InvalidState)
        return handler(command)

    def _spec(self, command: Command, prefix: str, event_type: str,
              aggregate_type: str, payload: dict[str, Any]) -> EventSpec:
        """新实体标识由幂等键确定性派生，保证重放同一命令产生同一事件。"""
        return EventSpec(
            event_type=event_type,
            aggregate_type=aggregate_type,
            aggregate_id=f"{prefix}-{command.idempotency_key}",
            payload=payload,
        )

    # ----- 基线 -----
    def _cmd_freeze_baseline(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"continuity_lead"}))
        a = command.args
        _require(not self.state.frozen, "基线已冻结", InvalidState)
        _require(float(a.get("budget_total", -1)) >= 0, "预算不能为负")
        payload: dict[str, Any] = {"budget_total": float(a["budget_total"])}
        if a.get("delivery_deadline") is not None:
            payload["delivery_deadline"] = _dt(a["delivery_deadline"]).isoformat()
        spec = EventSpec(
            "BASELINE_FROZEN", "scene_baseline", a["scene_ref"], payload
        )
        return [spec], {"scene_ref": a["scene_ref"]}

    # ----- 候选 -----
    def _cmd_receive_candidate(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"observer", "continuity_lead", "editor"}))
        a = command.args
        clip_ref, batch_ref, content_hash = a["clip_ref"], a["batch_ref"], a["content_hash"]
        _require(clip_ref and batch_ref and content_hash,
                 "clip_ref/batch_ref/content_hash 必填")
        # 片段标识相同，但摘要(content_hash)或来源批次不同 => 隔离为独立候选
        for existing_ref in self.state.clips.get(clip_ref, []):
            existing = self.state.candidates[existing_ref]
            if existing.content_hash == content_hash and existing.batch_ref == batch_ref:
                raise DuplicateCandidate(
                    f"片段 {clip_ref} 在批次 {batch_ref} 已登记为候选 {existing_ref}，"
                    "回执重放应复用而非重复登记"
                )
            # 批次或摘要不同：调用方必须显式给出新的候选标识来隔离
            explicit = a.get("candidate_ref")
            if not explicit or explicit == existing_ref:
                raise CandidateIsolationConflict(
                    f"片段 {clip_ref} 已登记，且来源批次/摘要不同"
                    f"（已有批次 {existing.batch_ref}/{existing.content_hash[:8]}），"
                    "必须提供新的 candidate_ref 隔离登记，禁止覆盖原候选"
                )
        candidate_ref = a.get("candidate_ref") or f"cand-{command.idempotency_key}"
        _require(candidate_ref not in self.state.candidates, "候选标识冲突", DuplicateCandidate)
        spec = EventSpec(
            "CANDIDATE_RECEIVED", "shot_candidate", candidate_ref,
            {
                "clip_ref": clip_ref,
                "batch_ref": batch_ref,
                "content_hash": content_hash,
                "shot_ref": a["shot_ref"],
                "scene_ref": a.get("scene_ref", self.state.scene_ref),
                "summary": a.get("summary", ""),
            },
        )
        return [spec], {"candidate_ref": candidate_ref, "clip_ref": clip_ref}

    def _cmd_link_edge(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"observer", "continuity_lead"}))
        a = command.args
        frm, to = self.state.candidate(a["from_candidate_ref"]), self.state.candidate(a["to_candidate_ref"])
        _require(frm.candidate_ref != to.candidate_ref, "承接边不能连接同一候选")
        _require(to.candidate_ref not in frm.handles_to, "承接边已存在", InvalidState)
        _require(frm.candidate_ref not in self._downstream(to.candidate_ref), "承接边不能成环")
        _require(not frm.withdrawn and not to.withdrawn, "承接边不能连接已撤销候选", InvalidState)
        spec = self._spec(
            command, "edge", "CONTINUITY_EDGE_LINKED", "shot_candidate",
            {"from_candidate_ref": frm.candidate_ref, "to_candidate_ref": to.candidate_ref},
        )
        return [spec], {"edge_ref": spec.aggregate_id}

    def _downstream(self, root: str) -> set[str]:
        seen: set[str] = set()
        stack = [root]
        while stack:
            cur = stack.pop()
            for nxt in self.state.candidate(cur).handles_to:
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return seen

    # ----- 观察 / 工单 / 约束 -----
    def _cmd_record_observation(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"observer", "continuity_lead"}))
        a = command.args
        _require(a["dimension"] in DIMENSIONS, f"维度必须是 {sorted(DIMENSIONS)}")
        spec = self._spec(
            command, "obs", "OBSERVATION_RECORDED", "continuity_issue",
            {
                "shot_ref": a.get("shot_ref"),
                "candidate_ref": a.get("candidate_ref"),
                "dimension": a["dimension"],
                "evidence": a["evidence"],
                "recorded_by": command.actor,
            },
        )
        return [spec], {"observation_ref": spec.aggregate_id}

    def _cmd_open_ticket(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"observer", "continuity_lead"}))
        a = command.args
        self.state.candidate(a["candidate_ref"])
        _require(a["dimension"] in DIMENSIONS, f"维度必须是 {sorted(DIMENSIONS)}")
        ticket_ref = a.get("ticket_ref") or f"ticket-{command.idempotency_key}"
        _require(ticket_ref not in self.state.tickets, "工单已存在", DuplicateCandidate)
        spec = EventSpec(
            "ISSUE_RECORDED", "continuity_issue", ticket_ref,
            {
                "shot_ref": a["shot_ref"],
                "candidate_ref": a["candidate_ref"],
                "dimension": a["dimension"],
                "evidence": a["evidence"],
                "recorded_by": command.actor,
            },
        )
        return [spec], {"ticket_ref": ticket_ref}

    def _cmd_confirm_constraint(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"continuity_lead"}))
        a = command.args
        spec = self._spec(
            command, "constraint", "CONSTRAINT_CONFIRMED", "continuity_issue",
            {
                "constraint": a["constraint"],
                "shot_ref": a.get("shot_ref"),
                "candidate_ref": a.get("candidate_ref"),
                "recorded_by": command.actor,
            },
        )
        return [spec], {"constraint_ref": spec.aggregate_id}

    # ----- 撤销：沿承接边精确重开受影响工单 -----
    def _cmd_withdraw_candidate(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"continuity_lead"}))
        a = command.args
        root = self.state.candidate(a["candidate_ref"])
        _require(not root.withdrawn, "候选已撤销", InvalidState)

        affected = self._downstream(root.candidate_ref)  # 含根，沿 from->to 精确传播
        affected.add(root.candidate_ref)

        specs: list[EventSpec] = [
            EventSpec(
                "CANDIDATE_WITHDRAWN", "shot_candidate", root.candidate_ref,
                {"candidate_ref": root.candidate_ref, "reason": a["reason"]},
            )
        ]
        reopened: list[str] = []
        seen_tickets: set[str] = set()
        for cand_ref in sorted(affected):
            for ticket_ref in sorted(self.state.candidate(cand_ref).ticket_refs):
                tk = self.state.ticket(ticket_ref)
                if ticket_ref in seen_tickets or tk.delivered:
                    # 已交付版本保留原选择；其工单不重开，走新版本流程
                    continue
                seen_tickets.add(ticket_ref)
                reopened.append(ticket_ref)
                specs.append(
                    EventSpec(
                        "TICKET_REOPENED", "continuity_issue", ticket_ref,
                        {
                            "root_ticket_ref": ticket_ref,
                            "root_candidate_ref": root.candidate_ref,
                            "reason": a["reason"],
                        },
                    )
                )
        specs.append(
            EventSpec(
                "REWORK_PROPAGATED", "shot_candidate", root.candidate_ref,
                {
                    "root_candidate_ref": root.candidate_ref,
                    "affected_candidate_refs": sorted(affected),
                    "reopened_ticket_refs": reopened,
                    "reason": a["reason"],
                },
            )
        )
        return specs, {
            "reopened_ticket_refs": reopened,
            "affected_candidate_refs": sorted(affected),
        }

    # ----- 重生成：请求 / 制片人裁决 -----
    def _cmd_request_regen(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"continuity_lead"}))
        a = command.args
        tk = self.state.ticket(a["ticket_ref"])
        _require(tk.status in {"open", "reopened"}, "工单当前状态不允许请求重生成", InvalidState)
        _require(float(a["estimated_cost"]) >= 0, "预估费用不能为负")
        deadline = _dt(a["deadline"])
        if self.state.delivery_deadline is not None:
            _require(
                deadline <= self.state.delivery_deadline,
                f"重生成期限 {deadline.isoformat()} 晚于交付期限，交期不可行",
                ScheduleConflict,
            )
        spec = self._spec(
            command, "regen", "REGEN_REQUESTED", "continuity_issue",
            {
                "request_ref": f"regen-{command.idempotency_key}",
                "ticket_ref": tk.ticket_ref,
                "shot_ref": tk.shot_ref,
                "reason": a.get("reason", ""),
                "estimated_cost": float(a["estimated_cost"]),
                "deadline": deadline.isoformat(),
                "requested_by": command.actor,
            },
        )
        return [spec], {"request_ref": spec.aggregate_id}

    def _decide_regen(self, command: Command, approve: bool) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"producer"}))
        a = command.args
        req = self.state.regen_requests.get(a["request_ref"])
        _require(req is not None, "重生成请求不存在", NotFound)
        _require(req.status == "pending", "该请求已裁决", InvalidState)
        if approve:
            remaining = self.state.budget_total - self.state.budget_used
            _require(
                req.estimated_cost <= remaining + 1e-9,
                f"预估 {req.estimated_cost} 超出剩余预算 {remaining:.2f}",
                BudgetExceeded,
            )
            _require(
                self.clock.now() <= req.deadline,
                f"当前时间已晚于该重生成可行期限 {req.deadline.isoformat()}",
                ScheduleConflict,
            )
            event_type, extra = "REGEN_APPROVED", {}
        else:
            event_type, extra = "REGEN_REJECTED", {"reason": a.get("reason", "未说明")}
        spec = EventSpec(
            event_type, "continuity_issue", req.request_ref,
            {
                "request_ref": req.request_ref,
                "ticket_ref": req.ticket_ref,
                "decided_by": command.actor,
                **extra,
            },
        )
        return [spec], {"request_ref": req.request_ref, "decision": "approved" if approve else "rejected"}

    def _cmd_approve_regen(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        return self._decide_regen(command, True)

    def _cmd_reject_regen(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        return self._decide_regen(command, False)

    # ----- 剪辑方案与原子锁片 -----
    def _cmd_open_edit(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"editor", "continuity_lead"}))
        a = command.args
        edit_ref = a["edit_ref"]
        _require(edit_ref not in self.state.edits, "剪辑方案已存在", DuplicateCandidate)
        deadline = _dt(a["deadline"])
        spec = EventSpec(
            "EDIT_OPENED", "edit_release", edit_ref,
            {"edit_ref": edit_ref, "plan_ref": a["plan_ref"], "deadline": deadline.isoformat()},
        )
        return [spec], {"edit_ref": edit_ref, "plan_ref": a["plan_ref"]}

    def _cmd_lock_shot(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"editor"}))
        a = command.args
        edit = self.state.edit(a["edit_ref"])
        cand = self.state.candidate(a["candidate_ref"])
        _require(not cand.withdrawn, "候选已撤销，无法锁片", LockConflict)
        _require(self.clock.now() <= edit.deadline, "剪辑方案挑片期限已过", ScheduleConflict)

        shot_ref = a["shot_ref"]
        _require(cand.shot_ref == shot_ref,
                 f"候选 {cand.candidate_ref} 属于镜头 {cand.shot_ref}，不能锁入 {shot_ref}",
                 ValidationError)
        current = self.state.shot_locks.get(shot_ref)
        if current is not None and current[0] != edit.edit_ref:
            # 并行方案占用：原子裁决点，后到者失败
            raise LockConflict(
                f"镜头 {shot_ref} 已被并行剪辑方案 {current[0]} 锁定为 {current[1]}"
            )
        # 同一候选不得同时服务两个最终镜头（即使镜头不同）
        for other_shot, holder in self.state.shot_locks.items():
            other_edit, other_cand = holder
            if other_cand == cand.candidate_ref and other_edit != edit.edit_ref:
                raise LockConflict(
                    f"候选 {cand.candidate_ref} 已被并行方案 {other_edit} 用于镜头 {other_shot}"
                )
        if current == (edit.edit_ref, cand.candidate_ref):
            return [], {"shot_ref": shot_ref, "candidate_ref": cand.candidate_ref, "noop": True}

        spec = EventSpec(
            "SHOT_LOCKED", "edit_release", edit.edit_ref,
            {
                "edit_ref": edit.edit_ref,
                "plan_ref": edit.plan_ref,
                "shot_ref": shot_ref,
                "candidate_ref": cand.candidate_ref,
            },
        )
        return [spec], {"shot_ref": shot_ref, "candidate_ref": cand.candidate_ref}

    # ----- 交付：版本不可变 -----
    def _cmd_deliver_release(self, command: Command) -> tuple[list[EventSpec], dict[str, Any]]:
        self._role(command, frozenset({"editor", "continuity_lead"}))
        a = command.args
        edit = self.state.edit(a["edit_ref"])
        selections = self.state.valid_locks(edit.edit_ref)
        _require(selections, "剪辑方案没有有效的镜头锁，无法交付", InvalidState)
        for cand_ref in selections.values():
            _require(not self.state.candidate(cand_ref).withdrawn,
                     "交付选择包含已撤销候选", InvalidState)
        _require(
            self.state.delivery_deadline is None or self.clock.now() <= self.state.delivery_deadline,
            "已超过交付期限", ScheduleConflict,
        )
        revision = max(
            (r.revision for r in self.state.releases if r.edit_ref == edit.edit_ref), default=0
        ) + 1
        release_ref = a.get("release_ref") or f"release-{edit.edit_ref}-r{revision}"
        spec = EventSpec(
            "RELEASE_DELIVERED", "edit_release", release_ref,
            {
                "release_ref": release_ref,
                "edit_ref": edit.edit_ref,
                "revision": revision,
                "selections": dict(selections),
                "revision_note": a.get("revision_note", ""),
            },
        )
        return [spec], {"release_ref": release_ref, "revision": revision, "selections": dict(selections)}

    # ----- 查询 -----
    def status(self) -> dict[str, Any]:
        s = self.state
        return {
            "scene_ref": s.scene_ref,
            "frozen": s.frozen,
            "budget_total": s.budget_total,
            "budget_used": s.budget_used,
            "budget_remaining": s.budget_total - s.budget_used,
            "candidates": {
                ref: {
                    "clip_ref": c.clip_ref,
                    "batch_ref": c.batch_ref,
                    "shot_ref": c.shot_ref,
                    "withdrawn": c.withdrawn,
                    "handles_to": sorted(c.handles_to),
                }
                for ref, c in s.candidates.items()
            },
            "tickets": {
                ref: {
                    "shot_ref": t.shot_ref,
                    "status": t.status,
                    "reopen_count": t.reopen_count,
                    "delivered": t.delivered,
                    "root_candidate_ref": t.root_candidate_ref,
                }
                for ref, t in s.tickets.items()
            },
            "pending_regen": [r.request_ref for r in s.regen_requests.values() if r.status == "pending"],
            "shot_locks": {k: {"edit_ref": v[0], "candidate_ref": v[1]} for k, v in s.shot_locks.items()},
            "releases": [
                {
                    "release_ref": r.release_ref,
                    "revision": r.revision,
                    "selections": r.selections,
                    "revision_note": r.revision_note,
                }
                for r in s.releases
            ],
        }

    def overdue(self) -> dict[str, list[str]]:
        """可控时钟推进后的挑片/复核/交付逾期判定。"""
        now = self.clock.now()
        delivered_edits = {r.edit_ref for r in self.state.releases}
        delivery_overdue = bool(
            self.state.delivery_deadline
            and now > self.state.delivery_deadline
            and not self.state.releases
        )
        return {
            "regen_decisions": [
                r.request_ref for r in self.state.regen_requests.values()
                if r.status == "pending" and now > r.deadline
            ],
            "edit_selection": [
                e.edit_ref for e in self.state.edits.values()
                if now > e.deadline and e.edit_ref not in delivered_edits
            ],
            "delivery": [self.state.scene_ref or "stream"] if delivery_overdue else [],
        }
