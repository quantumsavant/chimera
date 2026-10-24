"""
Tests for Phase 3: Retry Semantics, Operation-Level Idempotency Keys,
Deterministic Jitter, Cancellation During Backoff, and Transactional
Deduplication.
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
from typing import Any
import unittest

from chimera import (
    ClaimToken,
    SimulatedProcessCrash,
    TaskContext,
    TaskSpec,
    TaskState,
    VirtualClock,
    WorkflowStatus,
    cancel_workflow,
    create_workflow,
    get_task_result,
    recover_workflow,
    run_workflow,
)


class TestIdempotencyAndRetries(unittest.TestCase):
    """Verify idempotency key scopes, jitter bounds, and retry recovery accounting."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp.name)
        self.clock = VirtualClock(
            initial_time=1_700_000_000.0, auto_advance_on_wait=True
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_custom_operation_key_and_attempt_idempotency_key_lifecycle(
        self,
    ) -> None:
        observed_keys: list[tuple[str, str, str]] = []

        def flaky_op(_p: Any, ctx: TaskContext) -> str:
            observed_keys.append(
                (
                    ctx.idempotency_key,
                    ctx.operation_key,
                    ctx.attempt_idempotency_key,
                )
            )
            if ctx.attempt == 1:
                raise ConnectionError("retry me")
            return "ok"

        wf = create_workflow(
            [
                TaskSpec(
                    task_id="charge_task",
                    func=flaky_op,
                    operation_key="merchant:order_555:capture",
                    max_retries=1,
                    base_backoff_seconds=0.1,
                )
            ],
            workflow_id="wf_op_key",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(len(observed_keys), 2)
        # Logical operation key stays identical across retries, while attempt_idempotency_key advances
        self.assertEqual(observed_keys[0][0], "merchant:order_555:capture")
        self.assertEqual(observed_keys[1][0], "merchant:order_555:capture")
        self.assertEqual(observed_keys[0][1], "merchant:order_555:capture")
        self.assertNotEqual(observed_keys[0][2], observed_keys[1][2])
        self.assertIn("attempt:1:", observed_keys[0][2])
        self.assertIn("attempt:2:", observed_keys[1][2])

    def test_deterministic_seeded_jitter_and_max_backoff_cap(self) -> None:
        spec1 = TaskSpec(
            task_id="jitter_task",
            func=lambda: None,
            base_backoff_seconds=1.0,
            backoff_factor=2.0,
            max_backoff_seconds=3.5,
            jitter_seconds=0.5,
            jitter_seed=42,
        )
        spec2 = TaskSpec(
            task_id="jitter_task",
            func=lambda: None,
            base_backoff_seconds=1.0,
            backoff_factor=2.0,
            max_backoff_seconds=3.5,
            jitter_seconds=0.5,
            jitter_seed=42,
        )
        d1 = spec1.compute_backoff(1, workflow_id="wf_1")
        d2 = spec2.compute_backoff(1, workflow_id="wf_1")
        self.assertEqual(d1, d2)
        self.assertGreater(d1, 1.0)
        self.assertLessEqual(d1, 1.5)

        # Attempt 3 base is 4.0, which is capped by max_backoff_seconds=3.5
        d3 = spec1.compute_backoff(3, workflow_id="wf_1")
        self.assertEqual(d3, 3.5)

    def test_retry_accounting_preserved_across_crash_during_retry_wait(
        self,
    ) -> None:
        attempts_executed: list[int] = []
        crashed_in_retry_wait = False

        def always_fails(_p: Any, ctx: TaskContext) -> None:
            attempts_executed.append(ctx.attempt)
            raise ValueError(f"fail on attempt {ctx.attempt}")

        def crash_on_retry_wait(stage: str, rec: dict, _seq: int) -> None:
            nonlocal crashed_in_retry_wait
            if (
                not crashed_in_retry_wait
                and stage == "AFTER_FSYNC"
                and rec.get("event_type") == "TASK_STATE_CHANGED"
                and rec["data"].get("to_state") == TaskState.RETRY_WAIT.value
            ):
                crashed_in_retry_wait = True
                raise SimulatedProcessCrash("Crash while task is in RETRY_WAIT")

        tasks = [
            TaskSpec(
                task_id="retry_task",
                func=always_fails,
                max_retries=2,  # 1 initial + 2 retries = 3 total attempts max
                base_backoff_seconds=1.0,
            )
        ]
        wf = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="wf_retry_wait_crash",
            storage_dir=self.storage_dir,
            clock=self.clock,
            crash_hook=crash_on_retry_wait,
        )
        with self.assertRaises(SimulatedProcessCrash):
            run_workflow(wf)

        self.assertEqual(attempts_executed, [1])

        summary = recover_workflow(
            "wf_retry_wait_crash",
            tasks=tasks,
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        self.assertEqual(summary.status, WorkflowStatus.FAILED)
        # Must execute attempts 2 and 3 and stop at 3 total attempts
        self.assertEqual(attempts_executed, [1, 2, 3])
        res = get_task_result("wf_retry_wait_crash", "retry_task")
        self.assertEqual(res.attempt, 3)
        self.assertEqual(res.retry_count, 2)

    def test_cancellation_during_retry_wait_cancels_task_cleanly(self) -> None:
        import threading

        vclock = VirtualClock(
            initial_time=1_700_000_000.0, auto_advance_on_wait=False
        )
        entered_retry_wait = threading.Event()

        def on_log(stage: str, rec: dict, _seq: int) -> None:
            if (
                stage == "AFTER_FSYNC"
                and rec.get("event_type") == "TASK_STATE_CHANGED"
                and rec["data"].get("to_state") == TaskState.RETRY_WAIT.value
            ):
                entered_retry_wait.set()

        wf = create_workflow(
            [
                TaskSpec(
                    "backoff_task",
                    func=lambda: (_ for _ in ()).throw(RuntimeError("retry")),
                    max_retries=3,
                    base_backoff_seconds=30.0,
                )
            ],
            max_concurrency=1,
            workflow_id="wf_cancel_in_backoff",
            storage_dir=self.storage_dir,
            clock=vclock,
            crash_hook=on_log,
        )

        def canceller() -> None:
            self.assertTrue(entered_retry_wait.wait(timeout=5.0))
            cancel_workflow("wf_cancel_in_backoff", reason="CANCEL_BACKOFF")

        t = threading.Thread(target=canceller)
        t.start()
        summary = run_workflow(wf)
        t.join(timeout=5.0)
        self.assertTrue(entered_retry_wait.is_set())
        self.assertEqual(summary.status, WorkflowStatus.CANCELLED)
        self.assertEqual(
            summary.tasks["backoff_task"].state, TaskState.CANCELLED
        )

    def test_sqlite_transactional_deduplication_across_side_effect_crash_gap(
        self,
    ) -> None:
        db_path = self.storage_dir / "dedup.sqlite3"
        conn = sqlite3.connect(str(db_path))
        with conn:
            conn.execute(
                "CREATE TABLE transfers (idempotency_key TEXT PRIMARY KEY, amount INTEGER)"
            )
        conn.close()

        crashed_once = False
        raw_invocations = 0

        def transfer_funds(payload: dict[str, int], ctx: TaskContext) -> int:
            nonlocal raw_invocations
            raw_invocations += 1
            c = sqlite3.connect(str(db_path))
            try:
                c.isolation_level = None
                c.execute("BEGIN IMMEDIATE")
                row = c.execute(
                    "SELECT amount FROM transfers WHERE idempotency_key = ?",
                    (ctx.idempotency_key,),
                ).fetchone()
                if row is not None:
                    c.execute("COMMIT")
                    return int(row[0])
                c.execute(
                    "INSERT INTO transfers (idempotency_key, amount) VALUES (?, ?)",
                    (ctx.idempotency_key, payload["amount"]),
                )
                c.execute("COMMIT")
                return payload["amount"]
            finally:
                c.close()

        def crash_before_commit(token: ClaimToken, outcome: str) -> None:
            nonlocal crashed_once
            if outcome == "SUCCEEDED" and not crashed_once:
                crashed_once = True
                raise SimulatedProcessCrash("Crash after SQLite COMMIT")

        tasks = [
            TaskSpec(
                "wire_transfer",
                func=transfer_funds,
                input_payload={"amount": 5000},
                operation_key="transfer:tx_9001",
            )
        ]
        wf = create_workflow(
            tasks,
            workflow_id="wf_sqlite_dedup",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf.before_task_commit_hook = crash_before_commit
        with self.assertRaises(SimulatedProcessCrash):
            run_workflow(wf)

        summary = recover_workflow(
            "wf_sqlite_dedup",
            tasks=tasks,
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(raw_invocations, 2)

        c_check = sqlite3.connect(str(db_path))
        rows = c_check.execute("SELECT * FROM transfers").fetchall()
        c_check.close()
        self.assertEqual(rows, [("transfer:tx_9001", 5000)])


if __name__ == "__main__":
    unittest.main()
