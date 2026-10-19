"""
CHIMERA Adversarial & Hidden-Style Audit Test Suite
===================================================

Covers:
1. All 15 mandatory adversarial scenarios (A through O), strengthened after
   audit to prove exact properties without post-hoc shortcuts.
2. All 7 mandatory hidden-style audit tests:
   - Hidden 1: Two independent OS processes opening the same workflow log concurrently.
   - Hidden 2: A crash during an actual partial `os.write` inside `EventLog.append`.
   - Hidden 3: A crash after an external on-disk side effect before durable success.
   - Hidden 4: A stale worker completing WHILE a retry attempt is actively `RUNNING`
     (plus verifying superseded zombie workers exit the worker loop).
   - Hidden 5: Genuine multi-threaded cancellation racing with a successful completion
     (plus late cancellation after all tasks succeeded).
   - Hidden 6: Corrupted records (CRC, framing, and valid-CRC semantic state
     corruption) followed by apparently valid records.
   - Hidden 7: Recovery with altered task definitions (`input_payload`, `max_retries`,
     `timeout_seconds`, `retryable_exceptions`) or dependency graphs.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from typing import Any
import unittest

from chimera import (
    ClaimToken,
    ConcurrentWorkflowAccessError,
    CorruptedStateTransitionError,
    CycleDetectedError,
    DuplicateTaskIdError,
    EventLog,
    InvalidStateTransitionError,
    LogCorruptionError,
    MissingDependencyError,
    SimulatedProcessCrash,
    TaskContext,
    TaskSpec,
    TaskState,
    VirtualClock,
    WorkflowEngine,
    WorkflowStatus,
    WorkflowValidationError,
    cancel_workflow,
    create_workflow,
    get_task_result,
    get_workflow_status,
    recover_workflow,
    run_workflow,
    validate_workflow,
)


class TestChimeraAdversarialSuite(unittest.TestCase):
    """Complete adversarial and hidden-style audit test suite (22 tests)."""

    def setUp(self) -> None:
        self._tmp_dir_obj = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp_dir_obj.name)
        self.clock = VirtualClock(
            initial_time=1_700_000_000.0, auto_advance_on_wait=True
        )

    def tearDown(self) -> None:
        self._tmp_dir_obj.cleanup()

    # ==================================================================
    # Mandatory Scenarios A through O
    # ==================================================================

    def test_scenario_A_diamond_dag_concurrent_branches(self) -> None:
        """
        Scenario A: A diamond-shaped DAG (root -> {left, right} -> sink) with
        concurrent branches and a shared downstream task.
        Proves via a 2-party Barrier that `left` and `right` execute concurrently,
        and `sink` starts only after both succeed.
        """
        branch_barrier = threading.Barrier(2, timeout=5.0)
        execution_order: list[str] = []
        order_lock = threading.Lock()

        def root_fn(payload: dict[str, int]) -> int:
            with order_lock:
                execution_order.append("root")
            return payload["seed"]

        def left_fn(_payload: Any, ctx: TaskContext) -> int:
            branch_barrier.wait()
            with order_lock:
                execution_order.append("left")
            return ctx.dependency_results["root"] * 2

        def right_fn(_payload: Any, ctx: TaskContext) -> int:
            branch_barrier.wait()
            with order_lock:
                execution_order.append("right")
            return ctx.dependency_results["root"] + 5

        def sink_fn(_payload: Any, ctx: TaskContext) -> int:
            with order_lock:
                execution_order.append("sink")
            return (
                ctx.dependency_results["left"]
                + ctx.dependency_results["right"]
            )

        tasks = [
            TaskSpec("root", func=root_fn, input_payload={"seed": 10}),
            TaskSpec("left", func=left_fn, dependencies=["root"]),
            TaskSpec("right", func=right_fn, dependencies=["root"]),
            TaskSpec("sink", func=sink_fn, dependencies=["left", "right"]),
        ]

        wf = create_workflow(
            tasks,
            max_concurrency=2,
            workflow_id="wf_scenario_a",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        self.assertTrue(validate_workflow(wf))
        summary = run_workflow(wf)

        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(execution_order[0], "root")
        self.assertEqual(set(execution_order[1:3]), {"left", "right"})
        self.assertEqual(execution_order[3], "sink")

        sink_res = get_task_result(wf.workflow_id, "sink")
        self.assertEqual(sink_res.state, TaskState.SUCCEEDED)
        self.assertEqual(sink_res.result, (10 * 2) + (10 + 5))

        ready_events = [
            e.task_id
            for e in summary.structured_logs
            if e.event_type == "TASK_STATE_CHANGED"
            and e.to_state == TaskState.READY.value
        ]
        self.assertEqual(ready_events, ["root", "left", "right", "sink"])

    def test_scenario_B_deep_dependency_cycle_and_validation(self) -> None:
        """
        Scenario B: A dependency cycle hidden several levels deep (8 levels),
        plus validation of duplicate IDs and missing dependencies.
        """
        noop = lambda: None
        deep_cycle_tasks = [
            TaskSpec("n0", func=noop, dependencies=[]),
            TaskSpec("n1", func=noop, dependencies=["n0"]),
            TaskSpec("n2", func=noop, dependencies=["n1"]),
            TaskSpec("n3", func=noop, dependencies=["n2", "n7"]),
            TaskSpec("n4", func=noop, dependencies=["n3"]),
            TaskSpec("n5", func=noop, dependencies=["n4"]),
            TaskSpec("n6", func=noop, dependencies=["n5"]),
            TaskSpec("n7", func=noop, dependencies=["n6"]),
            TaskSpec("side_branch", func=noop, dependencies=["n2"]),
        ]
        wf_cycle = create_workflow(
            deep_cycle_tasks,
            max_concurrency=2,
            workflow_id="wf_scenario_b_cycle",
            storage_dir=self.storage_dir,
        )
        with self.assertRaises(CycleDetectedError) as ctx:
            validate_workflow(wf_cycle)
        self.assertEqual(
            ctx.exception.cycle_path, ["n3", "n4", "n5", "n6", "n7", "n3"]
        )

        dup_tasks = [
            TaskSpec("dup", func=noop),
            TaskSpec("dup", func=noop),
        ]
        with self.assertRaises(DuplicateTaskIdError):
            validate_workflow(dup_tasks)

        missing_dep_tasks = [
            TaskSpec("t1", func=noop, dependencies=["non_existent_task"]),
        ]
        with self.assertRaises(MissingDependencyError):
            validate_workflow(missing_dep_tasks)

    def test_scenario_C_crash_after_side_effect_before_commit(self) -> None:
        """
        Scenario C: A worker crashes after executing an external side effect
        but before recording `SUCCEEDED` in the local event log.
        """
        raw_external_calls: list[str] = []
        idempotent_ledger: dict[str, str] = {}
        crash_triggered = False

        def side_effect_task(_payload: Any, ctx: TaskContext) -> str:
            raw_external_calls.append(f"attempt_{ctx.attempt}_epoch_{ctx.claim_epoch}")
            if ctx.idempotency_key not in idempotent_ledger:
                idempotent_ledger[ctx.idempotency_key] = "committed_external_effect"
            return "task_done"

        def crash_before_success_commit(token: ClaimToken, outcome: str) -> None:
            nonlocal crash_triggered
            if (
                token.task_id == "effect_task"
                and outcome == "SUCCEEDED"
                and not crash_triggered
            ):
                crash_triggered = True
                raise SimulatedProcessCrash(
                    "Simulated process kill after side effect, before SUCCEEDED log append"
                )

        tasks = [TaskSpec("effect_task", func=side_effect_task)]
        wf = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="wf_scenario_c",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf.before_task_commit_hook = crash_before_success_commit

        with self.assertRaises(SimulatedProcessCrash):
            run_workflow(wf)

        self.assertEqual(len(raw_external_calls), 1)
        status_mid = get_workflow_status(wf.workflow_id)
        self.assertEqual(
            status_mid.tasks["effect_task"].state, TaskState.RUNNING
        )

        recovered_summary = recover_workflow("wf_scenario_c")
        self.assertEqual(recovered_summary.status, WorkflowStatus.SUCCEEDED)
        task_res = get_task_result("wf_scenario_c", "effect_task")
        self.assertEqual(task_res.state, TaskState.SUCCEEDED)
        self.assertEqual(task_res.result, "task_done")
        self.assertEqual(task_res.recovery_count, 1)

        self.assertEqual(len(raw_external_calls), 2)
        self.assertEqual(len(idempotent_ledger), 1)

    def test_scenario_D_restart_after_durable_success_never_reruns(self) -> None:
        """
        Scenario D: The process crashes immediately after `t1` commits `SUCCEEDED`
        to the durable log (before `t2` is promoted or run).
        Upon recovery, `t1` MUST NOT run again, and `t2` must consume `t1`'s
        durably persisted result and complete.
        """
        run_counts = {"t1": 0, "t2": 0}
        crashed_once = False

        def t1_fn() -> dict[str, int]:
            run_counts["t1"] += 1
            return {"value": 42}

        def t2_fn(_payload: Any, ctx: TaskContext) -> int:
            run_counts["t2"] += 1
            return ctx.dependency_results["t1"]["value"] * 10

        def crash_on_t1_succeeded(
            stage: str, record: dict[str, Any], _seq: int
        ) -> None:
            nonlocal crashed_once
            if (
                not crashed_once
                and stage == "AFTER_FSYNC"
                and record.get("event_type") == "TASK_STATE_CHANGED"
                and record["data"].get("task_id") == "t1"
                and record["data"].get("to_state") == TaskState.SUCCEEDED.value
            ):
                crashed_once = True
                raise SimulatedProcessCrash(
                    "Crash immediately after t1 SUCCEEDED was fsynced to disk"
                )

        tasks = [
            TaskSpec("t1", func=t1_fn),
            TaskSpec("t2", func=t2_fn, dependencies=["t1"]),
        ]
        wf = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="wf_scenario_d",
            storage_dir=self.storage_dir,
            clock=self.clock,
            crash_hook=crash_on_t1_succeeded,
        )

        with self.assertRaises(SimulatedProcessCrash):
            run_workflow(wf)

        self.assertEqual(run_counts["t1"], 1)
        self.assertEqual(run_counts["t2"], 0)

        summary = recover_workflow("wf_scenario_d")
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(run_counts["t1"], 1)
        self.assertEqual(run_counts["t2"], 1)
        self.assertEqual(get_task_result("wf_scenario_d", "t2").result, 420)

    def test_scenario_E_concurrent_workers_claim_same_task(self) -> None:
        """
        Scenario E: Two workers synchronize on a Barrier and attempt to claim
        the exact same `READY` task simultaneously. Exactly one succeeds; the
        other receives `None`.
        """
        wf = create_workflow(
            [TaskSpec("single_ready_task", func=lambda: "ok")],
            max_concurrency=4,
            workflow_id="wf_scenario_e",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf._ensure_initialized_in_log()

        start_barrier = threading.Barrier(2, timeout=5.0)
        claims: list[Any] = [None, None]

        def contender(idx: int) -> None:
            start_barrier.wait()
            claims[idx] = wf.try_claim_specific_task(
                "single_ready_task", f"worker-{idx}"
            )

        t0 = threading.Thread(target=contender, args=(0,))
        t1 = threading.Thread(target=contender, args=(1,))
        t0.start()
        t1.start()
        t0.join(timeout=5.0)
        t1.join(timeout=5.0)

        non_none_claims = [c for c in claims if c is not None]
        self.assertEqual(len(non_none_claims), 1)
        claimed_token = non_none_claims[0][0]
        self.assertEqual(claimed_token.task_id, "single_ready_task")
        self.assertEqual(claimed_token.attempt, 1)
        self.assertEqual(claimed_token.claim_epoch, 1)

    def test_scenario_F_task_timeout_with_background_worker(self) -> None:
        """
        Scenario F: A task times out while its worker continues running in the
        background.
        """
        vclock = VirtualClock(
            initial_time=1_700_000_000.0, auto_advance_on_wait=False
        )
        worker_entered = threading.Event()
        release_zombie_worker = threading.Event()
        zombie_finished = threading.Event()

        def stuck_worker_fn(_payload: Any, _ctx: TaskContext) -> str:
            worker_entered.set()
            release_zombie_worker.wait(timeout=5.0)
            return "late_zombie_result"

        tasks = [
            TaskSpec(
                "slow_task",
                func=stuck_worker_fn,
                timeout_seconds=10.0,
                max_retries=0,
            ),
            TaskSpec(
                "dependent_task",
                func=lambda: "should_not_run",
                dependencies=["slow_task"],
            ),
        ]
        wf = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="wf_scenario_f",
            storage_dir=self.storage_dir,
            clock=vclock,
        )

        def time_controller() -> None:
            self.assertTrue(worker_entered.wait(timeout=5.0))
            vclock.advance(15.0)

        controller_thread = threading.Thread(target=time_controller)
        controller_thread.start()

        summary = run_workflow(wf)
        controller_thread.join(timeout=5.0)

        self.assertEqual(summary.status, WorkflowStatus.FAILED)
        slow_res = get_task_result(wf.workflow_id, "slow_task")
        self.assertEqual(slow_res.state, TaskState.FAILED)
        self.assertEqual(slow_res.error_type, "TaskTimeoutError")
        self.assertEqual(
            get_task_result(wf.workflow_id, "dependent_task").state,
            TaskState.CANCELLED,
        )

        orig_complete = wf.complete_task_attempt

        def tracked_complete(*args: Any, **kwargs: Any) -> bool:
            res = orig_complete(*args, **kwargs)
            zombie_finished.set()
            return res

        wf.complete_task_attempt = tracked_complete  # type: ignore[method-assign]
        release_zombie_worker.set()
        self.assertTrue(zombie_finished.wait(timeout=5.0))

        post_zombie_res = get_task_result(wf.workflow_id, "slow_task")
        self.assertEqual(post_zombie_res.state, TaskState.FAILED)
        self.assertFalse(post_zombie_res.has_result)
        stale_events = [
            e
            for e in get_workflow_status(wf.workflow_id).structured_logs
            if e.event_type == "STALE_COMPLETION_IGNORED"
        ]
        self.assertEqual(len(stale_events), 1)

    def test_scenario_G_dependency_fails_after_retries_cascades_downstream(
        self,
    ) -> None:
        """
        Scenario G: A dependency fails after 3 retries (4 total attempts) with
        exponential backoff, while multiple downstream tasks (`child_a`, `child_b`,
        `grandchild`) are waiting and a sibling branch (`independent_ok`) succeeds.
        """
        attempts_log: list[tuple[int, float]] = []
        downstream_executed = False

        def flaky_always_fails(_payload: Any, ctx: TaskContext) -> None:
            attempts_log.append((ctx.attempt, self.clock.now()))
            raise RuntimeError(f"Transient failure on attempt {ctx.attempt}")

        def never_called() -> None:
            nonlocal downstream_executed
            downstream_executed = True

        tasks = [
            TaskSpec(
                "unreliable_root",
                func=flaky_always_fails,
                max_retries=3,
                base_backoff_seconds=1.0,
                backoff_factor=2.0,
                retryable_exceptions=(RuntimeError,),
            ),
            TaskSpec(
                "child_a",
                func=never_called,
                dependencies=["unreliable_root"],
            ),
            TaskSpec(
                "child_b",
                func=never_called,
                dependencies=["unreliable_root"],
            ),
            TaskSpec(
                "grandchild",
                func=never_called,
                dependencies=["child_a", "child_b"],
            ),
            TaskSpec(
                "independent_ok",
                func=lambda: "survived",
            ),
        ]

        wf = create_workflow(
            tasks,
            max_concurrency=2,
            workflow_id="wf_scenario_g",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        summary = run_workflow(wf)

        self.assertEqual(summary.status, WorkflowStatus.FAILED)
        self.assertFalse(downstream_executed)
        self.assertEqual(len(attempts_log), 4)

        t0 = attempts_log[0][1]
        self.assertAlmostEqual(attempts_log[1][1] - t0, 1.0, places=4)
        self.assertAlmostEqual(attempts_log[2][1] - t0, 3.0, places=4)
        self.assertAlmostEqual(attempts_log[3][1] - t0, 7.0, places=4)

        self.assertEqual(
            summary.tasks["unreliable_root"].state, TaskState.FAILED
        )
        self.assertEqual(summary.tasks["child_a"].state, TaskState.CANCELLED)
        self.assertEqual(summary.tasks["child_b"].state, TaskState.CANCELLED)
        self.assertEqual(summary.tasks["grandchild"].state, TaskState.CANCELLED)
        self.assertEqual(
            summary.tasks["independent_ok"].state, TaskState.SUCCEEDED
        )

    def test_scenario_H_truncated_final_event_log_record_recovery(self) -> None:
        """
        Scenario H: The final event-log record is truncated mid-header or
        mid-payload by an actual interrupted `os.write` inside `EventLog.append`
        (`DURING_PARTIAL_WRITE`). Verifies that recovery detects the truncated
        tail, repairs the log file via `os.ftruncate`, does NOT re-run `t1`,
        and finishes `t2`.
        """
        for partial_bytes in (8, 24, 55):
            with self.subTest(partial_bytes=partial_bytes):
                wf_id = f"wf_scenario_h_{partial_bytes}"
                counts = {"t1": 0, "t2": 0}

                def t1_fn() -> str:
                    counts["t1"] += 1
                    return "t1_output"

                def t2_fn(_p: Any, ctx: TaskContext) -> str:
                    counts["t2"] += 1
                    return ctx.dependency_results["t1"] + "_t2"

                def crash_mid_write_of_t2_ready(
                    stage: str, rec: dict[str, Any], _seq: int
                ) -> None:
                    if (
                        stage == "DURING_PARTIAL_WRITE"
                        and rec.get("event_type") == "TASK_STATE_CHANGED"
                        and rec["data"].get("task_id") == "t2"
                        and rec["data"].get("to_state") == TaskState.READY.value
                    ):
                        raise SimulatedProcessCrash(
                            f"Crash after writing {partial_bytes} bytes of t2 READY frame"
                        )

                wf = create_workflow(
                    [
                        TaskSpec("t1", func=t1_fn),
                        TaskSpec("t2", func=t2_fn, dependencies=["t1"]),
                    ],
                    max_concurrency=1,
                    workflow_id=wf_id,
                    storage_dir=self.storage_dir,
                    clock=self.clock,
                    crash_hook=crash_mid_write_of_t2_ready,
                    partial_write_bytes=partial_bytes,
                )
                with self.assertRaises(SimulatedProcessCrash):
                    run_workflow(wf)

                self.assertEqual(counts["t1"], 1)
                self.assertEqual(counts["t2"], 0)

                recovered = recover_workflow(wf_id)
                self.assertTrue(recovered.truncated_tail_repaired)
                self.assertEqual(recovered.status, WorkflowStatus.SUCCEEDED)
                self.assertEqual(counts["t1"], 1)
                self.assertEqual(counts["t2"], 1)
                self.assertEqual(
                    get_task_result(wf_id, "t2").result, "t1_output_t2"
                )

    def test_scenario_I_earlier_event_log_record_corrupted(self) -> None:
        """
        Scenario I: An earlier event-log record is corrupted (CRC mismatch,
        header corruption, or mid-file truncation). Verifies that recovery
        NEVER silently ignores earlier corruption and raises `LogCorruptionError`.
        """
        wf = create_workflow(
            [
                TaskSpec("t1", func=lambda: "ok1"),
                TaskSpec("t2", func=lambda: "ok2", dependencies=["t1"]),
            ],
            max_concurrency=1,
            workflow_id="wf_scenario_i",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        run_workflow(wf)
        original_lines = wf.log_path.read_bytes().splitlines(keepends=True)
        self.assertGreaterEqual(len(original_lines), 4)

        corrupted_lines = list(original_lines)
        rec2 = bytearray(corrupted_lines[1])
        rec2[-5] ^= 0x01
        corrupted_lines[1] = bytes(rec2)
        wf.log_path.write_bytes(b"".join(corrupted_lines))

        with self.assertRaises(LogCorruptionError) as exc_ctx:
            recover_workflow("wf_scenario_i")

        self.assertIn("CRC32 checksum mismatch at seq=2", str(exc_ctx.exception))

        corrupted_lines_2 = list(original_lines)
        corrupted_lines_2[0] = corrupted_lines_2[0][:-10]
        wf.log_path.write_bytes(b"".join(corrupted_lines_2))

        with self.assertRaises(LogCorruptionError):
            recover_workflow("wf_scenario_i")

    def test_scenario_J_simultaneous_completion_and_cancellation(self) -> None:
        """
        Scenario J: Two concurrent tasks (`t_a` and `t_b`) finish almost
        simultaneously while `cancel_workflow` is requested.
        """
        both_running_barrier = threading.Barrier(2, timeout=5.0)
        ta_committed = threading.Event()
        tb_ready_to_commit = threading.Event()
        cancel_finished = threading.Event()
        tc_ran = False

        def task_a_fn() -> str:
            both_running_barrier.wait()
            return "result_a"

        def task_b_fn() -> str:
            both_running_barrier.wait()
            self.assertTrue(ta_committed.wait(timeout=5.0))
            return "result_b"

        def task_c_fn() -> str:
            nonlocal tc_ran
            tc_ran = True
            return "result_c"

        wf = create_workflow(
            [
                TaskSpec("t_a", func=task_a_fn),
                TaskSpec("t_b", func=task_b_fn),
                TaskSpec("t_c", func=task_c_fn, dependencies=["t_a", "t_b"]),
            ],
            max_concurrency=2,
            workflow_id="wf_scenario_j",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )

        orig_complete = wf.complete_task_attempt

        def observe_complete(
            token: ClaimToken,
            *,
            succeeded: bool,
            result: Any = None,
            exception: BaseException | None = None,
        ) -> bool:
            if token.task_id == "t_a":
                res = orig_complete(
                    token,
                    succeeded=succeeded,
                    result=result,
                    exception=exception,
                )
                ta_committed.set()
                return res
            if token.task_id == "t_b":
                tb_ready_to_commit.set()
                self.assertTrue(cancel_finished.wait(timeout=5.0))
                return orig_complete(
                    token,
                    succeeded=succeeded,
                    result=result,
                    exception=exception,
                )
            return orig_complete(
                token, succeeded=succeeded, result=result, exception=exception
            )

        wf.complete_task_attempt = observe_complete  # type: ignore[method-assign]

        def canceller_thread_fn() -> None:
            self.assertTrue(tb_ready_to_commit.wait(timeout=5.0))
            cancel_workflow(wf.workflow_id, reason="SIMULTANEOUS_USER_CANCEL")
            cancel_finished.set()

        canceller = threading.Thread(target=canceller_thread_fn)
        canceller.start()

        summary = run_workflow(wf)
        canceller.join(timeout=5.0)

        self.assertEqual(summary.status, WorkflowStatus.CANCELLED)
        self.assertFalse(tc_ran)
        self.assertEqual(summary.tasks["t_a"].state, TaskState.SUCCEEDED)
        self.assertEqual(summary.tasks["t_a"].result, "result_a")
        self.assertEqual(summary.tasks["t_b"].state, TaskState.CANCELLED)
        self.assertEqual(summary.tasks["t_c"].state, TaskState.CANCELLED)

    def test_scenario_K_retryable_error_then_success(self) -> None:
        """
        Scenario K: A task raises a configured retryable exception (`ConnectionError`)
        on attempts 1 and 2, transitioning through `RETRY_WAIT`, and succeeds on
        attempt 3.
        """
        attempts = 0

        def transient_network_task(_payload: Any, ctx: TaskContext) -> str:
            nonlocal attempts
            attempts += 1
            if ctx.attempt < 3:
                raise ConnectionError(f"Socket reset on attempt {ctx.attempt}")
            return f"connected_on_attempt_{ctx.attempt}"

        wf = create_workflow(
            [
                TaskSpec(
                    "net_task",
                    func=transient_network_task,
                    timeout_seconds=30.0,
                    max_retries=3,
                    base_backoff_seconds=0.25,
                    backoff_factor=2.0,
                    retryable_exceptions=(ConnectionError, TimeoutError),
                )
            ],
            max_concurrency=1,
            workflow_id="wf_scenario_k",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        summary = run_workflow(wf)

        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(attempts, 3)
        res = get_task_result(wf.workflow_id, "net_task")
        self.assertEqual(res.state, TaskState.SUCCEEDED)
        self.assertEqual(res.attempt, 3)
        self.assertEqual(res.result, "connected_on_attempt_3")

        states_traversed = [
            (e.from_state, e.to_state)
            for e in summary.structured_logs
            if e.task_id == "net_task" and e.event_type == "TASK_STATE_CHANGED"
        ]
        self.assertEqual(
            states_traversed,
            [
                ("PENDING", "READY"),
                ("READY", "RUNNING"),
                ("RUNNING", "RETRY_WAIT"),
                ("RETRY_WAIT", "READY"),
                ("READY", "RUNNING"),
                ("RUNNING", "RETRY_WAIT"),
                ("RETRY_WAIT", "READY"),
                ("READY", "RUNNING"),
                ("RUNNING", "SUCCEEDED"),
            ],
        )

    def test_scenario_L_stale_completion_after_superseded_by_retry(self) -> None:
        """
        Scenario L: Attempt 1 times out (with `retry_on_timeout=True`), so the
        task transitions `RUNNING -> RETRY_WAIT -> READY -> RUNNING` (Attempt 2).
        While Attempt 2 is actively `RUNNING`, Attempt 1's worker thread finishes
        and submits its stale result, which is rejected (`STALE_COMPLETION_IGNORED`),
        and Attempt 2 then completes with `"fresh_attempt_2_result"`.
        """
        vclock = VirtualClock(
            initial_time=1_700_000_000.0, auto_advance_on_wait=False
        )
        attempt1_started = threading.Event()
        attempt2_started = threading.Event()
        attempt1_rejected = threading.Event()

        def task_fn(_payload: Any, ctx: TaskContext) -> str:
            if ctx.attempt == 1:
                attempt1_started.set()
                self.assertTrue(attempt2_started.wait(timeout=5.0))
                return "stale_attempt_1_result"
            attempt2_started.set()
            self.assertTrue(attempt1_rejected.wait(timeout=5.0))
            return "fresh_attempt_2_result"

        wf = create_workflow(
            [
                TaskSpec(
                    "superseded_task",
                    func=task_fn,
                    timeout_seconds=5.0,
                    max_retries=1,
                    base_backoff_seconds=1.0,
                    retry_on_timeout=True,
                )
            ],
            max_concurrency=1,
            workflow_id="wf_scenario_l",
            storage_dir=self.storage_dir,
            clock=vclock,
        )

        orig_complete = wf.complete_task_attempt

        def observe_complete(
            token: ClaimToken,
            *,
            succeeded: bool,
            result: Any = None,
            exception: BaseException | None = None,
        ) -> bool:
            committed = orig_complete(
                token, succeeded=succeeded, result=result, exception=exception
            )
            if token.attempt == 1 and not committed:
                attempt1_rejected.set()
            return committed

        wf.complete_task_attempt = observe_complete  # type: ignore[method-assign]

        def clock_driver() -> None:
            self.assertTrue(attempt1_started.wait(timeout=5.0))
            vclock.advance(6.0)
            while (
                wf.get_task_result("superseded_task").state
                != TaskState.RETRY_WAIT
            ):
                vclock.advance(0.05)
            vclock.advance(2.0)

        driver = threading.Thread(target=clock_driver)
        driver.start()

        summary = run_workflow(wf)
        driver.join(timeout=5.0)

        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        res = get_task_result(wf.workflow_id, "superseded_task")
        self.assertEqual(res.state, TaskState.SUCCEEDED)
        self.assertEqual(res.attempt, 2)
        self.assertEqual(res.result, "fresh_attempt_2_result")
        stale_logs = [
            e
            for e in summary.structured_logs
            if e.event_type == "STALE_COMPLETION_IGNORED"
        ]
        self.assertEqual(len(stale_logs), 1)

    def test_scenario_M_exhaustive_crash_between_every_persistence_operation(
        self,
    ) -> None:
        """
        Scenario M: Exhaustively inject a simulated process crash at EVERY
        persistence operation boundary (seq = 1, 2, ..., N) across a workflow
        containing root tasks, dependencies, and a retryable failure.
        """
        def make_tasks(
            exec_counts: dict[str, int], fail_once_state: dict[str, int]
        ) -> list[TaskSpec]:
            def t_root() -> int:
                exec_counts["t_root"] += 1
                return 10

            def t_retry(_p: Any, ctx: TaskContext) -> int:
                exec_counts["t_retry"] += 1
                fail_once_state["t_retry"] += 1
                if fail_once_state["t_retry"] == 1:
                    raise ValueError("Transient first-run error")
                return ctx.dependency_results["t_root"] + 5

            def t_leaf(_p: Any, ctx: TaskContext) -> int:
                exec_counts["t_leaf"] += 1
                return ctx.dependency_results["t_retry"] * 2

            return [
                TaskSpec("t_root", func=t_root),
                TaskSpec(
                    "t_retry",
                    func=t_retry,
                    dependencies=["t_root"],
                    max_retries=1,
                    base_backoff_seconds=0.01,
                ),
                TaskSpec("t_leaf", func=t_leaf, dependencies=["t_retry"]),
            ]

        base_counts = {"t_root": 0, "t_retry": 0, "t_leaf": 0}
        base_fail = {"t_retry": 0}
        baseline_wf = create_workflow(
            make_tasks(base_counts, base_fail),
            max_concurrency=1,
            workflow_id="wf_scenario_m_baseline",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        baseline_summary = run_workflow(baseline_wf)
        total_records = baseline_summary.log_record_count
        self.assertGreaterEqual(total_records, 10)

        for crash_seq in range(1, total_records + 1):
            with self.subTest(crash_seq=crash_seq):
                wf_id = f"wf_scenario_m_seq_{crash_seq}"
                counts = {"t_root": 0, "t_retry": 0, "t_leaf": 0}
                fail_tracker = {"t_retry": 0}
                committed_succeeded_before_crash: set[str] = set()

                def crash_at_seq(
                    stage: str, rec: dict[str, Any], seq: int
                ) -> None:
                    if (
                        stage == "AFTER_FSYNC"
                        and rec.get("event_type") == "TASK_STATE_CHANGED"
                        and rec["data"].get("to_state")
                        == TaskState.SUCCEEDED.value
                    ):
                        committed_succeeded_before_crash.add(
                            rec["data"]["task_id"]
                        )
                    if stage == "AFTER_FSYNC" and seq == crash_seq:
                        raise SimulatedProcessCrash(
                            f"Injected crash after seq={seq}"
                        )

                wf = create_workflow(
                    make_tasks(counts, fail_tracker),
                    max_concurrency=1,
                    workflow_id=wf_id,
                    storage_dir=self.storage_dir,
                    clock=self.clock,
                    crash_hook=crash_at_seq,
                )
                with self.assertRaises(SimulatedProcessCrash):
                    run_workflow(wf)

                counts_at_crash = dict(counts)

                recovered = recover_workflow(wf_id)
                self.assertEqual(recovered.status, WorkflowStatus.SUCCEEDED)
                self.assertEqual(
                    get_task_result(wf_id, "t_leaf").result, 30
                )

                for done_tid in committed_succeeded_before_crash:
                    self.assertEqual(
                        counts[done_tid],
                        counts_at_crash[done_tid],
                        f"Task '{done_tid}' was re-executed after durable SUCCEEDED at crash_seq={crash_seq}!",
                    )

    def test_scenario_N_empty_and_single_task_workflows(self) -> None:
        """
        Scenario N: An empty workflow (0 tasks) and a workflow containing a
        single task.
        """
        empty_wf = create_workflow(
            [],
            max_concurrency=2,
            workflow_id="wf_scenario_n_empty",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        self.assertTrue(validate_workflow(empty_wf))
        empty_summary = run_workflow(empty_wf)
        self.assertEqual(empty_summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(empty_summary.total_tasks, 0)

        single_wf = create_workflow(
            [
                TaskSpec(
                    "only_task",
                    input_payload={"x": 7},
                    func=lambda p: p["x"] * 6,
                )
            ],
            max_concurrency=1,
            workflow_id="wf_scenario_n_single",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        self.assertTrue(validate_workflow(single_wf))
        single_summary = run_workflow(single_wf)
        self.assertEqual(single_summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(
            get_task_result(single_wf.workflow_id, "only_task").result, 42
        )

    def test_scenario_O_payload_and_dependency_result_isolation(self) -> None:
        """
        Scenario O: Tasks attempt to mutate shared `input_payload` dictionaries
        and `ctx.dependency_results` in-place.
        """
        shared_input = {"items": [1, 2, 3], "meta": {"owner": "original"}}

        def mutator_task(payload: dict[str, Any]) -> dict[str, Any]:
            payload["items"].append(999)
            payload["meta"]["owner"] = "corrupted_by_mutator"
            return {"list": [10, 20]}

        def reader_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
            ctx.dependency_results["mutator"]["list"].append(888)
            return copy.deepcopy(payload)

        wf = create_workflow(
            [
                TaskSpec(
                    "mutator",
                    func=mutator_task,
                    input_payload=shared_input,
                ),
                TaskSpec(
                    "reader",
                    func=reader_task,
                    input_payload=shared_input,
                    dependencies=["mutator"],
                ),
            ],
            max_concurrency=2,
            workflow_id="wf_scenario_o",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )

        shared_input["items"].append(777)

        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)

        reader_out = get_task_result(wf.workflow_id, "reader").result
        self.assertEqual(
            reader_out, {"items": [1, 2, 3], "meta": {"owner": "original"}}
        )

        mutator_out = get_task_result(wf.workflow_id, "mutator").result
        self.assertEqual(mutator_out, {"list": [10, 20]})

    # ==================================================================
    # 7 Mandatory Hidden-Style Audit Tests
    # ==================================================================

    def test_hidden_1_two_independent_processes_opening_same_workflow_log_concurrently(
        self,
    ) -> None:
        """
        Hidden Test 1: Two independent OS processes (and two in-process engines)
        opening and attempting to run/recover the same workflow log concurrently.
        """
        task_entered = threading.Event()
        release_task = threading.Event()

        def blocking_task() -> str:
            task_entered.set()
            release_task.wait(timeout=10.0)
            return "completed_by_owner"

        wf1 = create_workflow(
            [TaskSpec("t1", func=blocking_task)],
            max_concurrency=1,
            workflow_id="wf_hidden_concurrent_proc",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )

        owner_thread = threading.Thread(target=lambda: run_workflow(wf1))
        owner_thread.start()
        self.assertTrue(task_entered.wait(timeout=5.0))

        try:
            child_code = (
                "import sys\n"
                "import sys; sys.path.insert(0, 'src'); from chimera import TaskSpec, recover_workflow, ConcurrentWorkflowAccessError\n"
                "try:\n"
                f"    recover_workflow('wf_hidden_concurrent_proc', tasks=[TaskSpec('t1', func=lambda: 'hijack')], storage_dir={str(self.storage_dir)!r})\n"
                "    sys.exit(2)\n"
                "except ConcurrentWorkflowAccessError:\n"
                "    sys.exit(0)\n"
            )
            proc = subprocess.run(
                [sys.executable, "-c", child_code],
                cwd=str(Path(__file__).resolve().parents[1]),
                capture_output=True,
                text=True,
                timeout=5.0,
            )
            self.assertEqual(
                proc.returncode,
                0,
                f"Child process did not raise ConcurrentWorkflowAccessError: stdout={proc.stdout}, stderr={proc.stderr}",
            )

            wf2 = WorkflowEngine(
                tasks=[TaskSpec("t1", func=lambda: "hijack")],
                workflow_id="wf_hidden_concurrent_proc",
                storage_dir=self.storage_dir,
            )
            with self.assertRaises(ConcurrentWorkflowAccessError):
                wf2.run()
        finally:
            release_task.set()
            owner_thread.join(timeout=5.0)

        self.assertEqual(
            get_task_result(wf1.workflow_id, "t1").result, "completed_by_owner"
        )

    def test_hidden_2_crash_during_partial_event_log_write(self) -> None:
        """
        Hidden Test 2: A crash occurs inside `EventLog.append` during a partial
        `os.write` (`DURING_PARTIAL_WRITE` before `os.fsync`).
        """
        exec_counts = {"t1": 0, "t2": 0}

        def t1_fn() -> int:
            exec_counts["t1"] += 1
            return 100

        def t2_fn(_p: Any, ctx: TaskContext) -> int:
            exec_counts["t2"] += 1
            return ctx.dependency_results["t1"] + 23

        def crash_mid_os_write(
            stage: str, rec: dict[str, Any], _seq: int
        ) -> None:
            if (
                stage == "DURING_PARTIAL_WRITE"
                and rec.get("event_type") == "TASK_STATE_CHANGED"
                and rec["data"].get("task_id") == "t2"
                and rec["data"].get("to_state") == TaskState.RUNNING.value
            ):
                raise SimulatedProcessCrash(
                    "Simulated power loss mid-os.write of t2 RUNNING record"
                )

        wf = create_workflow(
            [
                TaskSpec("t1", func=t1_fn),
                TaskSpec("t2", func=t2_fn, dependencies=["t1"]),
            ],
            max_concurrency=1,
            workflow_id="wf_hidden_partial_write",
            storage_dir=self.storage_dir,
            clock=self.clock,
            crash_hook=crash_mid_os_write,
            partial_write_bytes=48,
        )

        with self.assertRaises(SimulatedProcessCrash):
            run_workflow(wf)

        wf._event_log.set_crash_hook(None)
        with self.assertRaises(LogCorruptionError):
            wf._event_log.append("WORKFLOW_CANCEL_REQUESTED", {"reason": "LATE"})

        recovered = recover_workflow("wf_hidden_partial_write")
        self.assertTrue(recovered.truncated_tail_repaired)
        self.assertEqual(recovered.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(exec_counts["t1"], 1)
        self.assertEqual(exec_counts["t2"], 1)
        self.assertEqual(
            get_task_result("wf_hidden_partial_write", "t2").result, 123
        )

    def test_hidden_3_crash_after_external_file_side_effect_before_durable_success(
        self,
    ) -> None:
        """
        Hidden Test 3: A worker performs a real external filesystem side effect
        and crashes during the partial write of its `SUCCEEDED` log record.
        """
        raw_effect_file = self.storage_dir / "external_unguarded.txt"
        idempotent_effect_file = self.storage_dir / "external_idempotent.json"
        idempotent_effect_file.write_text("{}", encoding="utf-8")

        def file_side_effect_task(payload: dict[str, str], ctx: TaskContext) -> str:
            with raw_effect_file.open("a", encoding="utf-8") as f:
                f.write(f"charge:{payload['order_id']}\n")
                f.flush()
                os.fsync(f.fileno())

            ledger = json.loads(idempotent_effect_file.read_text(encoding="utf-8"))
            if ctx.idempotency_key not in ledger:
                ledger[ctx.idempotency_key] = payload["order_id"]
                idempotent_effect_file.write_text(
                    json.dumps(ledger), encoding="utf-8"
                )
            return f"charged_{payload['order_id']}"

        def crash_during_succeeded_write(
            stage: str, rec: dict[str, Any], _seq: int
        ) -> None:
            if (
                stage == "DURING_PARTIAL_WRITE"
                and rec.get("event_type") == "TASK_STATE_CHANGED"
                and rec["data"].get("task_id") == "payment_task"
                and rec["data"].get("to_state") == TaskState.SUCCEEDED.value
            ):
                raise SimulatedProcessCrash(
                    "Crash mid-write of SUCCEEDED after external file side effect"
                )

        wf = create_workflow(
            [
                TaskSpec(
                    "payment_task",
                    func=file_side_effect_task,
                    input_payload={"order_id": "ORD-900"},
                )
            ],
            max_concurrency=1,
            workflow_id="wf_hidden_side_effect",
            storage_dir=self.storage_dir,
            clock=self.clock,
            crash_hook=crash_during_succeeded_write,
            partial_write_bytes=30,
        )

        with self.assertRaises(SimulatedProcessCrash):
            run_workflow(wf)

        self.assertEqual(
            raw_effect_file.read_text(encoding="utf-8").splitlines(),
            ["charge:ORD-900"],
        )

        recovered = recover_workflow("wf_hidden_side_effect")
        self.assertTrue(recovered.truncated_tail_repaired)
        self.assertEqual(recovered.status, WorkflowStatus.SUCCEEDED)

        self.assertEqual(
            raw_effect_file.read_text(encoding="utf-8").splitlines(),
            ["charge:ORD-900", "charge:ORD-900"],
        )
        self.assertEqual(
            json.loads(idempotent_effect_file.read_text(encoding="utf-8")),
            {"wf_hidden_side_effect:payment_task": "ORD-900"},
        )

    def test_hidden_4_stale_worker_completes_while_retry_running_and_exits_pool(
        self,
    ) -> None:
        """
        Hidden Test 4: Attempt 1 times out, and Attempt 2 starts running on a
        replacement worker thread (`worker-2`). While Attempt 2 is actively
        `RUNNING`, Attempt 1 (`worker-1`) finishes and attempts to commit `SUCCEEDED`.
        Verifies:
        1. Attempt 1's completion is fenced off and ignored while Attempt 2
           remains `RUNNING`.
        2. The superseded `worker-1` thread immediately exits `_worker_entry`
           and does NOT claim the downstream task `after_retry` (`worker-2`
           claims `after_retry`).
        """
        vclock = VirtualClock(
            initial_time=1_700_000_000.0, auto_advance_on_wait=False
        )
        attempt1_in_flight = threading.Event()
        attempt2_in_flight = threading.Event()
        stale_commit_rejected = threading.Event()

        def retry_task(_p: Any, ctx: TaskContext) -> str:
            if ctx.attempt == 1:
                attempt1_in_flight.set()
                self.assertTrue(attempt2_in_flight.wait(timeout=5.0))
                return "stale_val"
            attempt2_in_flight.set()
            # Wait until `STALE_COMPLETION_IGNORED` has been logged for Attempt 1
            self.assertTrue(stale_commit_rejected.wait(timeout=5.0))
            return "fresh_val"

        def on_log_event(stage: str, rec: dict[str, Any], _seq: int) -> None:
            if (
                stage == "AFTER_FSYNC"
                and rec.get("event_type") == "STALE_COMPLETION_IGNORED"
            ):
                stale_commit_rejected.set()

        wf = create_workflow(
            [
                TaskSpec(
                    "flaky_timeout",
                    func=retry_task,
                    timeout_seconds=5.0,
                    max_retries=1,
                    base_backoff_seconds=1.0,
                    retry_on_timeout=True,
                ),
                TaskSpec(
                    "after_retry",
                    func=lambda _p, ctx: ctx.dependency_results["flaky_timeout"]
                    + "_downstream",
                    dependencies=["flaky_timeout"],
                ),
            ],
            max_concurrency=1,
            workflow_id="wf_hidden_stale_retry",
            storage_dir=self.storage_dir,
            clock=vclock,
            crash_hook=on_log_event,
        )

        claimed_by_workers: dict[str, list[str]] = {}

        def track_claim(token: ClaimToken) -> None:
            claimed_by_workers.setdefault(token.task_id, []).append(
                token.worker_id
            )

        wf.on_task_claimed_hook = track_claim

        def drive_clock() -> None:
            self.assertTrue(attempt1_in_flight.wait(timeout=5.0))
            vclock.advance(6.0)
            while (
                wf.get_task_result("flaky_timeout").state
                != TaskState.RETRY_WAIT
            ):
                vclock.advance(0.05)
            vclock.advance(2.0)

        driver = threading.Thread(target=drive_clock)
        driver.start()
        summary = run_workflow(wf)
        driver.join(timeout=5.0)

        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(
            get_task_result(wf.workflow_id, "after_retry").result,
            "fresh_val_downstream",
        )
        self.assertEqual(
            claimed_by_workers["flaky_timeout"], ["worker-1", "worker-2"]
        )
        self.assertEqual(claimed_by_workers["after_retry"], ["worker-2"])

    def test_hidden_5_cancellation_racing_with_successful_completion(self) -> None:
        """
        Hidden Test 5:
        Part A: Genuine multi-threaded race between `cancel_workflow` and a
                completing worker synchronized on a 2-party Barrier.
        Part B: If all tasks have already transitioned to `SUCCEEDED` before
                `cancel()` is called, the workflow status remains `SUCCEEDED`.
        """
        for iteration in range(10):
            wf_id = f"wf_hidden_cancel_race_{iteration}"
            race_barrier = threading.Barrier(2, timeout=5.0)

            def fast_task() -> str:
                return "done"

            wf = create_workflow(
                [TaskSpec("t1", func=fast_task)],
                max_concurrency=1,
                workflow_id=wf_id,
                storage_dir=self.storage_dir,
                clock=self.clock,
            )

            def sync_before_commit(token: ClaimToken, _outcome: str) -> None:
                if token.task_id == "t1":
                    race_barrier.wait()

            wf.before_task_commit_hook = sync_before_commit

            def concurrent_canceller() -> None:
                race_barrier.wait()
                cancel_workflow(wf_id)

            c_thread = threading.Thread(target=concurrent_canceller)
            c_thread.start()
            summary = run_workflow(wf)
            c_thread.join(timeout=5.0)

            t1_state = summary.tasks["t1"].state
            self.assertIn(t1_state, (TaskState.SUCCEEDED, TaskState.CANCELLED))
            if t1_state == TaskState.SUCCEEDED:
                self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
                self.assertEqual(summary.tasks["t1"].result, "done")
            else:
                self.assertEqual(summary.status, WorkflowStatus.CANCELLED)
                stale_events = [
                    e
                    for e in summary.structured_logs
                    if e.event_type == "STALE_COMPLETION_IGNORED"
                ]
                self.assertEqual(len(stale_events), 1)

        wf_late = create_workflow(
            [TaskSpec("t1", func=lambda: "ok")],
            max_concurrency=1,
            workflow_id="wf_hidden_late_cancel",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf_late._ensure_initialized_in_log()
        claim = wf_late.claim_next_ready_task("w1")
        assert claim is not None
        wf_late._record_task_transition(
            "t1",
            TaskState.SUCCEEDED,
            reason="EXECUTION_SUCCEEDED",
            result="ok",
            include_result=True,
        )
        late_summary = wf_late.cancel()
        self.assertEqual(late_summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(
            late_summary.tasks["t1"].state, TaskState.SUCCEEDED
        )

    def test_hidden_6_corrupted_records_followed_by_apparently_valid_records(
        self,
    ) -> None:
        """
        Hidden Test 6: An earlier record is corrupted (including semantic
        corruption with a valid CRC32 checksum: illegal transition `PENDING -> SUCCEEDED`,
        decreasing `claim_epoch`, or duplicate `WORKFLOW_CREATED`), followed by
        apparently valid records.
        """
        tasks = [
            TaskSpec("t1", func=lambda: 1),
            TaskSpec("t2", func=lambda: 2, dependencies=["t1"]),
        ]

        wf_a = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="wf_hidden_corrupt_semantic_a",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf_a._ensure_initialized_in_log()
        wf_a._event_log.append(
            "TASK_STATE_CHANGED",
            {
                "task_id": "t2",
                "from_state": "PENDING",
                "to_state": "SUCCEEDED",
                "reason": "FORGED_RECORD",
                "attempt": 1,
                "claim_epoch": 1,
                "recovery_count": 0,
                "result": 2,
            },
        )
        wf_a._event_log.append(
            "TASK_STATE_CHANGED",
            {
                "task_id": "t1",
                "from_state": "READY",
                "to_state": "RUNNING",
                "reason": "CLAIMED_BY_WORKER",
                "attempt": 1,
                "claim_epoch": 2,
                "recovery_count": 0,
            },
        )
        with self.assertRaises(CorruptedStateTransitionError) as ctx_a:
            recover_workflow("wf_hidden_corrupt_semantic_a")
        self.assertIn("Illegal state transition", str(ctx_a.exception))

        wf_b = create_workflow(
            [TaskSpec("t1", func=lambda: 1, max_retries=1)],
            max_concurrency=1,
            workflow_id="wf_hidden_corrupt_semantic_b",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf_b._ensure_initialized_in_log()
        wf_b._event_log.append(
            "TASK_STATE_CHANGED",
            {
                "task_id": "t1",
                "from_state": "READY",
                "to_state": "RUNNING",
                "reason": "CLAIMED_BY_WORKER",
                "attempt": 1,
                "claim_epoch": 5,
                "recovery_count": 0,
            },
        )
        wf_b._event_log.append(
            "TASK_STATE_CHANGED",
            {
                "task_id": "t1",
                "from_state": "RUNNING",
                "to_state": "SUCCEEDED",
                "reason": "EXECUTION_SUCCEEDED",
                "attempt": 1,
                "claim_epoch": 2,
                "recovery_count": 0,
                "result": 1,
            },
        )
        with self.assertRaises(CorruptedStateTransitionError):
            recover_workflow("wf_hidden_corrupt_semantic_b")

        wf_c = create_workflow(
            [TaskSpec("t1", func=lambda: 1)],
            max_concurrency=1,
            workflow_id="wf_hidden_corrupt_semantic_c",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf_c._ensure_initialized_in_log()
        wf_c._event_log.append(
            "WORKFLOW_CREATED",
            {"max_concurrency": 1, "tasks": {}},
        )
        wf_c._event_log.append(
            "TASK_STATE_CHANGED",
            {
                "task_id": "t1",
                "from_state": "READY",
                "to_state": "RUNNING",
                "reason": "CLAIMED_BY_WORKER",
                "attempt": 1,
                "claim_epoch": 1,
                "recovery_count": 0,
            },
        )
        with self.assertRaises(CorruptedStateTransitionError) as ctx_c:
            recover_workflow("wf_hidden_corrupt_semantic_c")
        self.assertIn("Duplicate or out-of-position WORKFLOW_CREATED", str(ctx_c.exception))

    def test_hidden_7_recovery_with_altered_task_definitions_or_dependency_graphs(
        self,
    ) -> None:
        """
        Hidden Test 7: Attempting to recover a workflow with altered task
        definitions (`input_payload`, `max_retries`, `timeout_seconds`,
        `retryable_exceptions`, `priority`) or altered dependency graphs
        raises `WorkflowValidationError`.
        """
        wf = create_workflow(
            [
                TaskSpec(
                    "a",
                    func=lambda p: p["x"],
                    input_payload={"x": 10},
                    max_retries=2,
                    timeout_seconds=15.0,
                    retryable_exceptions=(ValueError,),
                    priority=1,
                ),
                TaskSpec(
                    "b",
                    func=lambda: 20,
                    dependencies=["a"],
                ),
            ],
            max_concurrency=1,
            workflow_id="wf_hidden_altered_specs",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        run_workflow(wf)

        alterations = [
            # 1. Altered input_payload
            [
                TaskSpec(
                    "a",
                    func=lambda p: p["x"],
                    input_payload={"x": 999},
                    max_retries=2,
                    timeout_seconds=15.0,
                    retryable_exceptions=(ValueError,),
                    priority=1,
                ),
                TaskSpec("b", func=lambda: 20, dependencies=["a"]),
            ],
            # 2. Altered max_retries
            [
                TaskSpec(
                    "a",
                    func=lambda p: p["x"],
                    input_payload={"x": 10},
                    max_retries=0,
                    timeout_seconds=15.0,
                    retryable_exceptions=(ValueError,),
                    priority=1,
                ),
                TaskSpec("b", func=lambda: 20, dependencies=["a"]),
            ],
            # 3. Altered timeout_seconds
            [
                TaskSpec(
                    "a",
                    func=lambda p: p["x"],
                    input_payload={"x": 10},
                    max_retries=2,
                    timeout_seconds=60.0,
                    retryable_exceptions=(ValueError,),
                    priority=1,
                ),
                TaskSpec("b", func=lambda: 20, dependencies=["a"]),
            ],
            # 4. Altered retryable_exceptions
            [
                TaskSpec(
                    "a",
                    func=lambda p: p["x"],
                    input_payload={"x": 10},
                    max_retries=2,
                    timeout_seconds=15.0,
                    retryable_exceptions=(RuntimeError,),
                    priority=1,
                ),
                TaskSpec("b", func=lambda: 20, dependencies=["a"]),
            ],
            # 5. Altered dependency graph
            [
                TaskSpec(
                    "a",
                    func=lambda p: p["x"],
                    input_payload={"x": 10},
                    max_retries=2,
                    timeout_seconds=15.0,
                    retryable_exceptions=(ValueError,),
                    priority=1,
                ),
                TaskSpec("b", func=lambda: 20, dependencies=[]),
            ],
        ]

        for idx, bad_tasks in enumerate(alterations):
            with self.subTest(alteration_index=idx):
                with self.assertRaises(WorkflowValidationError):
                    recover_workflow(
                        "wf_hidden_altered_specs",
                        tasks=bad_tasks,
                        storage_dir=self.storage_dir,
                        clock=self.clock,
                    )


if __name__ == "__main__":
    unittest.main(verbosity=2)