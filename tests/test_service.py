import json
import sys
import threading
import unittest
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from scene_continuity import (
    Command,
    ContinuityService,
    ManualClock,
)
from scene_continuity.contracts import validate_event
from scene_continuity.provenance import explain_final_shot
from scene_continuity.store import EventStore

T0 = "2026-09-25T09:00:00+08:00"
DELIVERY = "2026-09-30T18:00:00+08:00"


def cmd(name, actor, role, args, key):
    return Command(name, actor, role, args, key)


def event_dict(event):
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "aggregate_type": event.aggregate_type,
        "aggregate_id": event.aggregate_id,
        "occurred_at": event.occurred_at.isoformat(),
        "version": event.version,
        "payload": event.payload,
    }


class ServiceScenarioTests(unittest.TestCase):
    def setUp(self):
        self.schema = json.loads((ROOT / "contracts/domain.schema.json").read_text(encoding="utf-8"))
        self.clock = ManualClock()
        self.svc = ContinuityService(self.clock, scene_ref="scene-001")

    def ok(self, receipt):
        self.assertTrue(receipt.ok, msg=f"{receipt.error_code}: {receipt.error_message}")
        return receipt

    def expect_error(self, receipt, code):
        self.assertFalse(receipt.ok)
        self.assertEqual(code, receipt.error_code)

    def freeze(self, budget=1000.0):
        return self.ok(self.svc.submit(cmd(
            "freeze_baseline", "lead", "continuity_lead",
            {"scene_ref": "scene-001", "budget_total": budget, "delivery_deadline": DELIVERY},
            "freeze",
        )))

    def receive(self, key, candidate_ref, clip, batch, hsh, shot, **extra):
        return self.ok(self.svc.submit(cmd(
            "receive_candidate", "obs", "observer",
            {"candidate_ref": candidate_ref, "clip_ref": clip, "batch_ref": batch,
             "content_hash": hsh, "shot_ref": shot, **extra},
            key,
        )))

    # ------------------------------------------------------------------ #
    def test_roles_and_baseline(self):
        self.expect_error(
            self.svc.submit(cmd("freeze_baseline", "p", "producer",
                                {"scene_ref": "s", "budget_total": 1}, "k")),
            "permission_denied",
        )
        self.freeze()
        # 基线未冻结前不能收候选；冻结后观察者可以
        self.expect_error(
            ContinuityService(self.clock).submit(cmd(
                "receive_candidate", "o", "observer",
                {"candidate_ref": "c1", "clip_ref": "f1", "batch_ref": "b1",
                 "content_hash": "h1", "shot_ref": "s1"}, "x")),
            "invalid_state",
        )
        self.receive("r1", "c1", "f1", "b1", "h1", "s1")
        # 观察者不能确认跨镜头约束、不能批准重生成、不能锁片
        self.expect_error(self.svc.submit(cmd(
            "confirm_constraint", "o", "observer", {"constraint": "视线向右"}, "deny1")),
            "permission_denied")

    def test_receipt_replay_is_idempotent(self):
        self.freeze()
        c = cmd("receive_candidate", "o", "observer",
                {"candidate_ref": "c1", "clip_ref": "f1", "batch_ref": "b1",
                 "content_hash": "h1", "shot_ref": "s1"}, "idem-1")
        first = self.ok(self.svc.submit(c))
        n_after_first = len(self.svc.store)
        second = self.svc.submit(c)
        self.ok(second)
        self.assertEqual(first.result, second.result)
        self.assertEqual(len(self.svc.store), n_after_first)
        # 同一幂等键用于不同命令必须被拒绝
        self.expect_error(self.svc.submit(cmd(
            "record_observation", "o", "observer",
            {"dimension": "appearance", "evidence": "发型变了"}, "idem-1")),
            "idempotency_key_mismatch")
    def test_clip_identity_isolation(self):
        self.freeze()
        self.receive("r1", "c1", "clip-A", "batch-1", "hash-v1", "s1")
        # 完全重复 => 重复候选错误
        self.expect_error(self.svc.submit(cmd(
            "receive_candidate", "o", "observer",
            {"candidate_ref": "c1", "clip_ref": "clip-A", "batch_ref": "batch-1",
             "content_hash": "hash-v1", "shot_ref": "s1"}, "r2")),
            "duplicate_candidate")
        # 同 clip_ref 但来源批次/摘要不同，且未显式隔离 => 拒绝覆盖
        self.expect_error(self.svc.submit(cmd(
            "receive_candidate", "o", "observer",
            {"candidate_ref": "c1", "clip_ref": "clip-A", "batch_ref": "batch-2",
             "content_hash": "hash-v2", "shot_ref": "s1"}, "r3")),
            "candidate_isolation_conflict")
        # 显式新候选标识 => 隔离共存
        self.receive("r4", "c2", "clip-A", "batch-2", "hash-v2", "s1")
        status = self.svc.status()
        self.assertEqual(["c1", "c2"], sorted(status["candidates"]))

    def test_withdraw_propagates_exactly_along_edges(self):
        self.freeze()
        self.receive("a", "cA", "clip-A", "b1", "hA", "s1")
        self.receive("b", "cB", "clip-B", "b1", "hB", "s2")
        self.receive("c", "cC", "clip-C", "b1", "hC", "s3")
        self.receive("d", "cD", "clip-D", "b1", "hD", "s9")  # 无关节点
        # A -> B -> C 动作承接
        for key, frm, to in [("e1", "cA", "cB"), ("e2", "cB", "cC")]:
            self.ok(self.svc.submit(cmd("link_edge", "o", "observer",
                                        {"from_candidate_ref": frm, "to_candidate_ref": to}, key)))
        self.ok(self.svc.submit(cmd("open_ticket", "o", "observer",
                                    {"ticket_ref": "tB", "shot_ref": "s2", "candidate_ref": "cB",
                                     "dimension": "screen_direction", "evidence": "出入画反了"}, "tb")))
        self.ok(self.svc.submit(cmd("open_ticket", "o", "observer",
                                    {"ticket_ref": "tC", "shot_ref": "s3", "candidate_ref": "cC",
                                     "dimension": "action_carry", "evidence": "手接不上"}, "tc")))
        self.ok(self.svc.submit(cmd("open_ticket", "o", "observer",
                                    {"ticket_ref": "tD", "shot_ref": "s9", "candidate_ref": "cD",
                                     "dimension": "appearance", "evidence": "无关差异"}, "td")))
        result = self.ok(self.svc.submit(cmd(
            "withdraw_candidate", "lead", "continuity_lead",
            {"candidate_ref": "cA", "reason": "导演淘汰 A"}, "w1"))).result
        self.assertEqual(["tB", "tC"], result["reopened_ticket_refs"])
        self.assertEqual(["cA", "cB", "cC"], result["affected_candidate_refs"])
        tickets = self.svc.status()["tickets"]
        self.assertEqual("reopened", tickets["tB"]["status"])
        self.assertEqual("reopened", tickets["tC"]["status"])
        self.assertEqual("open", tickets["tD"]["status"])
        self.assertEqual(1, tickets["tB"]["reopen_count"])

    def test_delivered_ticket_is_never_reopened_but_version_is_kept(self):
        self.freeze()
        self.receive("a", "cA", "clip-A", "b1", "hA", "s1")
        self.receive("b", "cB", "clip-B", "b1", "hB", "s2")
        self.ok(self.svc.submit(cmd("link_edge", "o", "observer",
                                    {"from_candidate_ref": "cA", "to_candidate_ref": "cB"}, "e1")))
        self.ok(self.svc.submit(cmd("open_ticket", "o", "observer",
                                    {"ticket_ref": "tB", "shot_ref": "s2", "candidate_ref": "cB",
                                     "dimension": "wardrobe_prop", "evidence": "道具不一致"}, "tb")))
        self.ok(self.svc.submit(cmd("open_edit", "ed", "editor",
                                    {"edit_ref": "E1", "plan_ref": "plan-A",
                                     "deadline": "2026-09-26T18:00:00+08:00"}, "oe")))
        self.ok(self.svc.submit(cmd("lock_shot", "ed", "editor",
                                    {"edit_ref": "E1", "shot_ref": "s2", "candidate_ref": "cB"}, "l1")))
        r1 = self.ok(self.svc.submit(cmd(
            "deliver_release", "lead", "continuity_lead",
            {"edit_ref": "E1", "revision_note": "初版交付"}, "d1"))).result
        self.assertEqual(1, r1["revision"])

        # 交付后撤销上游：已交付工单不重开，锁失效，原交付版本原样保留
        result = self.ok(self.svc.submit(cmd(
            "withdraw_candidate", "lead", "continuity_lead",
            {"candidate_ref": "cA", "reason": "角色面貌漂移"}, "w2"))).result
        self.assertEqual([], result["reopened_ticket_refs"])
        releases = self.svc.status()["releases"]
        self.assertEqual(1, len(releases))
        self.assertEqual("cB", releases[0]["selections"]["s2"])
        self.assertNotIn("s2", self.svc.status()["shot_locks"])

        # 新候选补位并交付 r2；r1 仍在
        self.receive("b2", "cB2", "clip-B2", "b2", "hB2", "s2")
        self.ok(self.svc.submit(cmd("lock_shot", "ed", "editor",
                                    {"edit_ref": "E1", "shot_ref": "s2", "candidate_ref": "cB2"}, "l2")))
        r2 = self.ok(self.svc.submit(cmd(
            "deliver_release", "lead", "continuity_lead",
            {"edit_ref": "E1", "revision_note": "s2 因连锁返工替换为 cB2"}, "d2"))).result
        self.assertEqual(2, r2["revision"])
        refs = {r["release_ref"] for r in self.svc.status()["releases"]}
        self.assertIn(r1["release_ref"], refs)

    def test_parallel_edit_lock_is_atomic(self):
        self.freeze()
        self.receive("a", "c1", "clip-1", "b1", "h1", "s1")
        self.receive("b", "c2", "clip-2", "b1", "h2", "s1")
        for key, ref in [("oe1", "E1"), ("oe2", "E2")]:
            self.ok(self.svc.submit(cmd("open_edit", "ed", "editor",
                                        {"edit_ref": ref, "plan_ref": "p",
                                         "deadline": "2026-09-26T18:00:00+08:00"}, key)))

        outcomes = []

        def lock(edit, cand, key):
            outcomes.append(self.svc.submit(cmd(
                "lock_shot", "ed", "editor",
                {"edit_ref": edit, "shot_ref": "s1", "candidate_ref": cand}, key)))

        t1 = threading.Thread(target=lock, args=("E1", "c1", "L1"))
        t2 = threading.Thread(target=lock, args=("E2", "c2", "L2"))
        t1.start(); t2.start(); t1.join(); t2.join()
        oks = [r for r in outcomes if r.ok]
        fails = [r for r in outcomes if not r.ok]
        self.assertEqual(1, len(oks))
        self.assertEqual(1, len(fails))
        self.assertEqual("lock_conflict", fails[0].error_code)
        # 同一方案重复锁同候选：幂等重放，不产生新事件
        winner = oks[0].result["candidate_ref"]
        winner_edit = "E1" if winner == "c1" else "E2"
        winner_key = "L1" if winner_edit == "E1" else "L2"
        events_before = len(self.svc.store)
        again = self.ok(self.svc.submit(cmd(
            "lock_shot", "ed", "editor",
            {"edit_ref": winner_edit, "shot_ref": "s1", "candidate_ref": winner}, winner_key)))
        self.assertEqual(winner, again.result["candidate_ref"])
        self.assertEqual(events_before, len(self.svc.store))

    def test_budget_and_schedule_guard_regen(self):
        self.freeze(budget=1000.0)
        self.receive("a", "c1", "clip-1", "b1", "h1", "s1")
        self.ok(self.svc.submit(cmd("open_ticket", "o", "observer",
                                    {"ticket_ref": "t1", "shot_ref": "s1", "candidate_ref": "c1",
                                     "dimension": "appearance", "evidence": "脸不对"}, "t1")))
        # 制片人不能替连续性负责人提请求
        self.expect_error(self.svc.submit(cmd(
            "request_regen", "p", "producer",
            {"ticket_ref": "t1", "estimated_cost": 600,
             "deadline": "2026-09-28T12:00:00+08:00"}, "g0")),
            "permission_denied")
        req1 = self.ok(self.svc.submit(cmd(
            "request_regen", "lead", "continuity_lead",
            {"ticket_ref": "t1", "estimated_cost": 600,
             "deadline": "2026-09-28T12:00:00+08:00", "reason": "重新生成脸部"}, "g1"))).result
        # 超过交期的重生成请求直接不可行
        self.receive("a2", "c2", "clip-2", "b1", "h2", "s2")
        self.ok(self.svc.submit(cmd("open_ticket", "o", "observer",
                                    {"ticket_ref": "t2", "shot_ref": "s2", "candidate_ref": "c2",
                                     "dimension": "appearance", "evidence": "脸不对"}, "t2")))
        self.expect_error(self.svc.submit(cmd(
            "request_regen", "lead", "continuity_lead",
            {"ticket_ref": "t2", "estimated_cost": 10,
             "deadline": "2026-10-01T12:00:00+08:00"}, "g2")),
            "schedule_conflict")
        # 连续性负责人不能自批
        self.expect_error(self.svc.submit(cmd(
            "approve_regen", "lead", "continuity_lead",
            {"request_ref": req1["request_ref"]}, "ap0")),
            "permission_denied")
        self.ok(self.svc.submit(cmd("approve_regen", "p", "producer",
                                    {"request_ref": req1["request_ref"]}, "ap1")))
        self.assertEqual(600.0, self.svc.status()["budget_used"])
        # 第二笔 500：只剩 400，预算外制片人无法批准
        self.ok(self.svc.submit(cmd(
            "request_regen", "lead", "continuity_lead",
            {"ticket_ref": "t2", "estimated_cost": 500,
             "deadline": "2026-09-29T12:00:00+08:00"}, "g3")))
        req2 = self.svc.status()["pending_regen"][0]
        self.expect_error(self.svc.submit(cmd(
            "approve_regen", "p", "producer", {"request_ref": req2}, "ap2")),
            "budget_exceeded")
        # 另一笔预算内但期限很紧的请求：时钟推过其可行期限后同样不能批准
        self.receive("a3", "c3", "clip-3", "b1", "h3", "s3")
        self.ok(self.svc.submit(cmd("open_ticket", "o", "observer",
                                    {"ticket_ref": "t3", "shot_ref": "s3", "candidate_ref": "c3",
                                     "dimension": "wardrobe_prop", "evidence": "道具不对"}, "t3")))
        self.ok(self.svc.submit(cmd(
            "request_regen", "lead", "continuity_lead",
            {"ticket_ref": "t3", "estimated_cost": 10,
             "deadline": "2026-09-26T12:00:00+08:00"}, "g4")))
        req3 = [r for r in self.svc.status()["pending_regen"] if r != req2][0]
        self.clock.advance(timedelta(days=2))
        self.expect_error(self.svc.submit(cmd(
            "approve_regen", "p", "producer", {"request_ref": req3}, "ap3")),
            "schedule_conflict")

    def test_controlled_clock_overdue(self):
        self.freeze()
        self.receive("a", "c1", "clip-1", "b1", "h1", "s1")
        self.ok(self.svc.submit(cmd("open_edit", "ed", "editor",
                                    {"edit_ref": "E1", "plan_ref": "p",
                                     "deadline": "2026-09-26T18:00:00+08:00"}, "oe")))
        self.assertEqual({"regen_decisions": [], "edit_selection": [], "delivery": []},
                         self.svc.overdue())
        self.clock.advance(timedelta(days=2))
        self.assertIn("E1", self.svc.overdue()["edit_selection"])
        # 逾期方案无法锁片
        self.expect_error(self.svc.submit(cmd(
            "lock_shot", "ed", "editor",
            {"edit_ref": "E1", "shot_ref": "s1", "candidate_ref": "c1"}, "lx")),
            "schedule_conflict")
        self.clock.advance(timedelta(days=10))
        self.assertIn("scene-001", self.svc.overdue()["delivery"])

    def test_queue_recovery_resumes_unfinished(self):
        store = EventStore()
        clock = ManualClock()
        svc = ContinuityService(clock, store, scene_ref="scene-001")
        commands = [
            cmd("freeze_baseline", "lead", "continuity_lead",
                {"scene_ref": "scene-001", "budget_total": 1000, "delivery_deadline": DELIVERY}, "freeze"),
            cmd("receive_candidate", "o", "observer",
                {"candidate_ref": "c1", "clip_ref": "f1", "batch_ref": "b1",
                 "content_hash": "h1", "shot_ref": "s1"}, "recv"),
            cmd("open_edit", "ed", "editor",
                {"edit_ref": "E1", "plan_ref": "p",
                 "deadline": "2026-09-26T18:00:00+08:00"}, "oe"),
            cmd("lock_shot", "ed", "editor",
                {"edit_ref": "E1", "shot_ref": "s1", "candidate_ref": "c1"}, "lock"),
        ]
        # “崩溃”前只完成前两条
        svc.submit(commands[0]); svc.submit(commands[1])
        events_before = len(store)

        # 服务恢复：新实例重放事件日志 + 继续未完成队列
        recovered = ContinuityService(clock, store, scene_ref="scene-001")
        receipts = recovered.restore_queue(commands)
        self.assertTrue(all(r.ok for r in receipts))
        self.assertEqual(events_before + 2, len(store))  # 仅补执行 open_edit/lock
        self.assertEqual("c1", recovered.status()["shot_locks"]["s1"]["candidate_ref"])

        # 再次恢复：已完成命令全部跳过，不产生重复事件
        recovered2 = ContinuityService(clock, store, scene_ref="scene-001")
        recovered2.restore_queue(commands)
        self.assertEqual(events_before + 2, len(store))

    def test_emitted_events_satisfy_contract(self):
        """把全链路产出的事件逐一过契约校验。"""
        self.freeze()
        self.receive("a", "c1", "clip-1", "b1", "h1", "s1")
        self.receive("b", "c2", "clip-2", "b1", "h2", "s2")
        self.ok(self.svc.submit(cmd("link_edge", "o", "observer",
                                    {"from_candidate_ref": "c1", "to_candidate_ref": "c2"}, "e1")))
        self.ok(self.svc.submit(cmd("record_observation", "o", "observer",
                                    {"shot_ref": "s1", "candidate_ref": "c1",
                                     "dimension": "appearance", "evidence": "发型变化"}, "obs")))
        self.ok(self.svc.submit(cmd("confirm_constraint", "lead", "continuity_lead",
                                    {"shot_ref": "s2", "candidate_ref": "c2",
                                     "constraint": "角色必须从画面右侧入画"}, "cc")))
        self.ok(self.svc.submit(cmd("open_ticket", "o", "observer",
                                    {"ticket_ref": "t1", "shot_ref": "s2", "candidate_ref": "c2",
                                     "dimension": "screen_direction", "evidence": "方向反了"}, "t1")))
        self.ok(self.svc.submit(cmd(
            "request_regen", "lead", "continuity_lead",
            {"ticket_ref": "t1", "estimated_cost": 100,
             "deadline": "2026-09-28T12:00:00+08:00"}, "g1")))
        req = self.svc.status()["pending_regen"][0]
        self.ok(self.svc.submit(cmd("approve_regen", "p", "producer",
                                    {"request_ref": req}, "ap1")))
        self.ok(self.svc.submit(cmd("open_edit", "ed", "editor",
                                    {"edit_ref": "E1", "plan_ref": "p",
                                     "deadline": "2026-09-26T18:00:00+08:00"}, "oe")))
        self.ok(self.svc.submit(cmd("lock_shot", "ed", "editor",
                                    {"edit_ref": "E1", "shot_ref": "s2", "candidate_ref": "c2"}, "lk")))
        self.ok(self.svc.submit(cmd("deliver_release", "lead", "continuity_lead",
                                    {"edit_ref": "E1", "revision_note": "首版"}, "dl")))
        self.ok(self.svc.submit(cmd("withdraw_candidate", "lead", "continuity_lead",
                                    {"candidate_ref": "c1", "reason": "淘汰"}, "wd")))
        seen = set()
        for event in self.svc.store.events():
            issues = validate_event(event_dict(event), self.schema)
            self.assertEqual([], issues, msg=f"{event.event_type}: {issues}")
            seen.add(event.event_type)
        # 新事件类型都被契约枚举覆盖
        self.assertIn("EDIT_OPENED", seen)
        self.assertIn("CONTINUITY_EDGE_LINKED", seen)

    def test_final_shot_adoption_is_explainable(self):
        self.freeze()
        self.receive("a1", "c1", "clip-1", "b1", "h1", "s2")
        self.ok(self.svc.submit(cmd("record_observation", "o", "observer",
                                    {"shot_ref": "s2", "candidate_ref": "c1",
                                     "dimension": "appearance", "evidence": "脸型漂移"}, "obs1")))
        self.ok(self.svc.submit(cmd("open_ticket", "o", "observer",
                                    {"ticket_ref": "t1", "shot_ref": "s2", "candidate_ref": "c1",
                                     "dimension": "appearance", "evidence": "脸型漂移"}, "t1")))
        self.ok(self.svc.submit(cmd(
            "withdraw_candidate", "lead", "continuity_lead",
            {"candidate_ref": "c1", "reason": "剪辑师淘汰 c1"}, "wd")))
        self.ok(self.svc.submit(cmd(
            "request_regen", "lead", "continuity_lead",
            {"ticket_ref": "t1", "estimated_cost": 100,
             "deadline": "2026-09-28T12:00:00+08:00"}, "g1")))
        req = self.svc.status()["pending_regen"][0]
        self.ok(self.svc.submit(cmd("approve_regen", "p", "producer",
                                    {"request_ref": req}, "ap1")))
        self.receive("a2", "c2", "clip-2", "b2", "h2", "s2", summary="补位候选")
        self.ok(self.svc.submit(cmd("confirm_constraint", "lead", "continuity_lead",
                                    {"shot_ref": "s2", "candidate_ref": "c2",
                                     "constraint": "服装与上一镜一致"}, "cc1")))
        self.ok(self.svc.submit(cmd("open_edit", "ed", "editor",
                                    {"edit_ref": "E1", "plan_ref": "p",
                                     "deadline": "2026-09-26T18:00:00+08:00"}, "oe")))
        self.ok(self.svc.submit(cmd("lock_shot", "ed", "editor",
                                    {"edit_ref": "E1", "shot_ref": "s2", "candidate_ref": "c2"}, "lk")))
        self.ok(self.svc.submit(cmd("deliver_release", "lead", "continuity_lead",
                                    {"edit_ref": "E1", "revision_note": "c1 淘汰后连锁返工，采用 c2"}, "dl")))

        story = explain_final_shot(self.svc, "s2")
        kinds = [s["kind"] for s in story["steps"]]
        self.assertEqual("c2", story["current_lock"]["candidate_ref"])
        self.assertEqual(1, len(story["delivered_revisions"]))
        for expected in [
            "CANDIDATE_RECEIVED", "OBSERVATION_RECORDED", "ISSUE_RECORDED",
            "CANDIDATE_WITHDRAWN", "TICKET_REOPENED", "REGEN_REQUESTED",
            "REGEN_APPROVED", "CONSTRAINT_CONFIRMED", "SHOT_LOCKED", "RELEASE_DELIVERED",
        ]:
            self.assertIn(expected, kinds, msg=f"采用链缺少 {expected}")
        # c1 与 c2 都出现在链路上：能解释“经过哪些候选”
        refs = {s["detail"].get("candidate_ref") for s in story["steps"]
                if s["kind"] in {"CANDIDATE_RECEIVED", "SHOT_LOCKED"}}
        self.assertEqual({"c1", "c2"}, refs)


if __name__ == "__main__":
    unittest.main()
