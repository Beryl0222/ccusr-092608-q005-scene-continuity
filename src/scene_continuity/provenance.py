"""最终镜头采用链解释。

沿事件日志重建某镜头/交付选择经过的候选、人工判断与连锁返工：
候选登记 -> 观察/约束 -> 撤销 -> 沿承接边重开工单 -> 重生成请求与裁决 -> 锁片 -> 交付。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .service import ContinuityService
from .store import StoredEvent


@dataclass
class AdoptionStep:
    order: int
    at: str
    kind: str
    summary: str
    detail: dict[str, Any]


def explain_final_shot(service: ContinuityService, shot_ref: str) -> dict[str, Any]:
    """返回某镜头当前/最终选择及其完整采用链。"""
    state = service.state
    events = service.store.events()

    # 该镜头的全部相关事件
    timeline: list[tuple[StoredEvent, str, dict[str, Any]]] = []
    for event in events:
        p = event.payload
        keep = False
        detail: dict[str, Any] = {}
        if event.event_type == "CANDIDATE_RECEIVED" and p.get("shot_ref") == shot_ref:
            keep, detail = True, {
                "candidate_ref": event.aggregate_id,
                "clip_ref": p["clip_ref"],
                "batch_ref": p["batch_ref"],
                "summary": p.get("summary", ""),
            }
        elif event.event_type in {"OBSERVATION_RECORDED", "ISSUE_RECORDED", "CONSTRAINT_CONFIRMED"} \
                and p.get("shot_ref") == shot_ref:
            keep = True
            detail = {
                "candidate_ref": p.get("candidate_ref"),
                "dimension": p.get("dimension", "constraint"),
                "evidence": p.get("evidence", p.get("constraint", "")),
                "by": p.get("recorded_by"),
                "ticket_ref": event.aggregate_id if event.event_type == "ISSUE_RECORDED" else None,
            }
        elif event.event_type == "TICKET_REOPENED":
            # 与该镜头工单有关的连锁重开
            tk = state.tickets.get(p.get("root_ticket_ref"))
            if tk is not None and tk.shot_ref == shot_ref:
                keep, detail = True, {
                    "ticket_ref": p["root_ticket_ref"],
                    "root_candidate_ref": p["root_candidate_ref"],
                    "reason": p.get("reason"),
                    "reopen_count": tk.reopen_count,
                }
        elif event.event_type == "REWORK_PROPAGATED":
            affected_candidates = {
                ref for ref in state.candidates
                if state.candidates[ref].shot_ref == shot_ref
            }
            if set(p.get("affected_candidate_refs", [])) & affected_candidates:
                keep, detail = True, {
                    "root_candidate_ref": p["root_candidate_ref"],
                    "reopened_ticket_refs": p.get("reopened_ticket_refs", []),
                    "reason": p.get("reason"),
                }
        elif event.event_type in {"REGEN_REQUESTED", "REGEN_APPROVED", "REGEN_REJECTED"}:
            if p.get("shot_ref") == shot_ref or _regen_touches_shot(state, p, shot_ref):
                keep = True
                detail = {k: v for k, v in p.items() if k != "shot_ref"}
        elif event.event_type == "SHOT_LOCKED" and p.get("shot_ref") == shot_ref:
            keep, detail = True, {
                "edit_ref": p["edit_ref"],
                "candidate_ref": p["candidate_ref"],
            }
        elif event.event_type == "CANDIDATE_WITHDRAWN":
            cand = state.candidates.get(p.get("candidate_ref"))
            if cand is not None and cand.shot_ref == shot_ref:
                keep, detail = True, {
                    "candidate_ref": p["candidate_ref"],
                    "reason": p.get("reason"),
                }
        elif event.event_type == "RELEASE_DELIVERED" and shot_ref in p.get("selections", {}):
            keep, detail = True, {
                "release_ref": p["release_ref"],
                "revision": p.get("revision"),
                "candidate_ref": p["selections"][shot_ref],
                "revision_note": p.get("revision_note", ""),
            }
        if keep:
            timeline.append((event, event.event_type, detail))

    labels = {
        "CANDIDATE_RECEIVED": "候选登记",
        "OBSERVATION_RECORDED": "人工观察",
        "CONSTRAINT_CONFIRMED": "跨镜头约束确认",
        "ISSUE_RECORDED": "差异登记/开工单",
        "CANDIDATE_WITHDRAWN": "候选淘汰",
        "REWORK_PROPAGATED": "沿承接边连锁返工",
        "TICKET_REOPENED": "工单重开",
        "REGEN_REQUESTED": "申请重生成",
        "REGEN_APPROVED": "制片人批准重生成",
        "REGEN_REJECTED": "制片人拒绝重生成",
        "SHOT_LOCKED": "锁片",
        "RELEASE_DELIVERED": "交付版本",
    }
    steps = [
        AdoptionStep(
            order=i + 1,
            at=event.occurred_at.isoformat(),
            kind=event.event_type,
            summary=labels.get(event.event_type, event.event_type),
            detail=detail,
        )
        for i, (event, _kind, detail) in enumerate(timeline)
    ]

    current = state.shot_locks.get(shot_ref)
    releases = [
        {
            "release_ref": r.release_ref,
            "revision": r.revision,
            "candidate_ref": r.selections.get(shot_ref),
            "revision_note": r.revision_note,
            "delivered_at": r.delivered_at.isoformat(),
        }
        for r in state.releases
        if shot_ref in r.selections
    ]
    return {
        "shot_ref": shot_ref,
        "current_lock": {"edit_ref": current[0], "candidate_ref": current[1]} if current else None,
        "delivered_revisions": releases,
        "steps": [vars(s) for s in steps],
    }


def _regen_touches_shot(state, payload: dict[str, Any], shot_ref: str) -> bool:
    req = state.regen_requests.get(payload.get("request_ref"))
    return req is not None and req.shot_ref == shot_ref
