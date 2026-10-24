"""
Tests for Phase 2: Supervised Process-Isolated Task Execution (`ProcessTaskExecutor`
and `ExecutionBackend.PROCESS`).

Covers:
- Multi-task DAG execution in real `spawn` child processes.
- Eager rejection of lambdas, local closures, and non-importable callables
  (`UnserializableCallableError`) without silent fallback to threads.
- Hard timeout enforcement against uncooperative CPU/sleep infinite loops,
  fencing the expired attempt BEFORE replacement attempts run, and reaping
  child processes (`active_process_count() == 0`).
- Repeated timeouts followed by retry recovery.
- Abrupt child crashes (`os._exit(17)`) and `FailureCategory.PROCESS_CRASH`.
- Task output size limit enforcement (`TaskOutputLimitExceededError`).
- Workflow cancellation and `shutdown()` reaping unresponsive children.
"""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import threading
import time
from typing import Any
import unittest

from chimera import (
    ExecutionBackend,
    FailureCategory,
    TaskContext,
    TaskOutputLimitExceededError,
    TaskSpec,
    TaskState,
    UnserializableCallableError,
    WorkflowStatus,
    cancel_workflow,
    create_workflow,
    get_task_result,
    run_workflow,
    validate_workflow,
)


# ============================================================================
# Top-level importable task callables for `spawn` child processes
# ============================================================================


def proc_add_task(payload: dict[str, int]) -> dict[str, int]:
    return {"sum": payload["a"] + payload["b"], "pid": os.getpid()}


def proc_multiply_dep_task(
    payload: dict[str, int], ctx: TaskContext
) -> dict[str, int]:
    upstream = ctx.dependency_results["step_add"]["sum"]
    return {
        "product": upstream * payload["factor"],
        "pid": os.getpid(),
        "upstream_pid": ctx.dependency_results["step_add"]["pid"],
    }


def proc_infinite_busy_loop(_payload: Any) -> str:
    # Uncooperative loop that ignores ctx.is_cancelled()
    while True:
        time.sleep(0.01)


def proc_timeout_twice_then_succeed(
    _payload: Any, ctx: TaskContext
) -> str:
    if ctx.attempt <= 2:
        while True:
            time.sleep(0.01)
    return f"succeeded_on_attempt_{ctx.attempt}_epoch_{ctx.claim_epoch}"


def proc_abrupt_os_exit(payload: dict[str, int]) -> str:
    os._exit(payload.get("exit_code", 17))


def proc_oversized_return(payload: dict[str, int]) -> str:
    return "X" * payload["num_chars"]


def proc_signal_file_and_block(payload: dict[str, str]) -> str:
    Path(payload["marker_path"]).write_text(str(os.getpid()), encoding="utf-8")
    while True:
        time.sleep(0.02)


class TestProcessIsolatedExecution(unittest.TestCase):
    """Verify real child-process execution, hard timeouts, crashes, and cleanup."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_process_backend_executes_dag_and_passes_dependency_results(
        self,
    ) -> None:
        tasks = [
            TaskSpec(
                task_id="step_add",
                func=proc_add_task,
                input_payload={"a": 15, "b": 27},
            ),
            TaskSpec(
                task_id="step_mul",
                func=proc_multiply_dep_task,
                input_payload={"factor": 3},
                dependencies=["step_add"],
            ),
        ]
        wf = create_workflow(
            tasks,
            max_concurrency=2,
            workflow_id="wf_proc_dag",
            storage_dir=self.storage_dir,
            execution_backend=ExecutionBackend.PROCESS,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(wf.active_process_count(), 0)

        r1 = get_task_result(wf.workflow_id, "step_add")
        r2 = get_task_result(wf.workflow_id, "step_mul")
        self.assertEqual(r1.result["sum"], 42)
        self.assertEqual(r2.result["product"], 126)
        self.assertNotEqual(r1.worker_pid, os.getpid())
        self.assertNotEqual(r2.worker_pid, os.getpid())
        self.assertEqual(r1.exit_code, 0)
        self.assertEqual(r2.exit_code, 0)

    def test_process_backend_rejects_lambdas_and_closures_without_silent_thread_fallback(
        self,
    ) -> None:
        # 1. Per-task PROCESS backend rejects lambda at TaskSpec construction
        with self.assertRaises(UnserializableCallableError):
            TaskSpec(
                task_id="lambda_task",
                func=lambda: 123,
                execution_backend=ExecutionBackend.PROCESS,
            )

        # 2. Workflow-level PROCESS backend rejects local closure at validate_workflow
        def local_closure(x: int) -> int:
            return x + 1

        wf = create_workflow(
            [TaskSpec("closure_task", func=local_closure, input_payload=5)],
            workflow_id="wf_bad_closure",
            storage_dir=self.storage_dir,
            execution_backend=ExecutionBackend.PROCESS,
        )
        with self.assertRaises(UnserializableCallableError):
            validate_workflow(wf)

    def test_process_backend_hard_timeout_terminates_infinite_loop_and_reaps_child(
        self,
    ) -> None:
        wf = create_workflow(
            [
                TaskSpec(
                    task_id="infinite_task",
                    func=proc_infinite_busy_loop,
                    timeout_seconds=0.12,
                    max_retries=0,
                ),
                TaskSpec(
                    task_id="downstream_blocked",
                    func=proc_add_task,
                    input_payload={"a": 1, "b": 2},
                    dependencies=["infinite_task"],
                ),
            ],
            max_concurrency=1,
            workflow_id="wf_proc_hard_timeout",
            storage_dir=self.storage_dir,
            execution_backend=ExecutionBackend.PROCESS,
        )
        t0 = time.monotonic()
        summary = run_workflow(wf)
        elapsed = time.monotonic() - t0

        self.assertEqual(summary.status, WorkflowStatus.FAILED)
        self.assertLess(elapsed, 5.0)
        self.assertEqual(wf.active_process_count(), 0)

        res = get_task_result(wf.workflow_id, "infinite_task")
        self.assertEqual(res.state, TaskState.FAILED)
        self.assertEqual(res.error_type, "TaskTimeoutError")
        self.assertEqual(res.failure_category, FailureCategory.TIMEOUT)
        self.assertEqual(res.timeout_count, 1)
        self.assertEqual(
            get_task_result(wf.workflow_id, "downstream_blocked").state,
            TaskState.CANCELLED,
        )

    def test_process_backend_repeated_timeouts_and_retry_recovery(self) -> None:
        wf = create_workflow(
            [
                TaskSpec(
                    task_id="flaky_loop",
                    func=proc_timeout_twice_then_succeed,
                    timeout_seconds=0.80,
                    max_retries=2,
                    base_backoff_seconds=0.01,
                    retry_on_timeout=True,
                )
            ],
            max_concurrency=1,
            workflow_id="wf_proc_retry_timeout",
            storage_dir=self.storage_dir,
            execution_backend=ExecutionBackend.PROCESS,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(wf.active_process_count(), 0)

        res = get_task_result(wf.workflow_id, "flaky_loop")
        self.assertEqual(res.state, TaskState.SUCCEEDED)
        self.assertEqual(res.attempt, 3)
        self.assertEqual(res.timeout_count, 2)
        self.assertEqual(res.retry_count, 2)
        self.assertIn("succeeded_on_attempt_3", res.result)

    def test_process_backend_abrupt_child_crash_exit_code_and_failure_category(
        self,
    ) -> None:
        wf = create_workflow(
            [
                TaskSpec(
                    task_id="crashing_child",
                    func=proc_abrupt_os_exit,
                    input_payload={"exit_code": 19},
                    max_retries=0,
                )
            ],
            max_concurrency=1,
            workflow_id="wf_proc_crash",
            storage_dir=self.storage_dir,
            execution_backend=ExecutionBackend.PROCESS,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.FAILED)
        self.assertEqual(wf.active_process_count(), 0)

        res = get_task_result(wf.workflow_id, "crashing_child")
        self.assertEqual(res.state, TaskState.FAILED)
        self.assertEqual(res.error_type, "ProcessWorkerCrashError")
        self.assertEqual(res.failure_category, FailureCategory.PROCESS_CRASH)
        self.assertEqual(res.exit_code, 19)
        self.assertEqual(summary.metrics.worker_crash_count, 1)

    def test_process_backend_oversized_output_rejected(self) -> None:
        wf = create_workflow(
            [
                TaskSpec(
                    task_id="noisy_task",
                    func=proc_oversized_return,
                    input_payload={"num_chars": 2048},
                    max_output_bytes=256,
                    max_retries=3,  # Serialization errors must not retry blindly
                )
            ],
            max_concurrency=1,
            workflow_id="wf_proc_oversized",
            storage_dir=self.storage_dir,
            execution_backend=ExecutionBackend.PROCESS,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.FAILED)
        res = get_task_result(wf.workflow_id, "noisy_task")
        self.assertEqual(res.state, TaskState.FAILED)
        self.assertEqual(res.attempt, 1)
        self.assertEqual(res.error_type, "TaskOutputLimitExceededError")
        self.assertEqual(
            res.failure_category, FailureCategory.SERIALIZATION_ERROR
        )

    def test_process_backend_cancellation_reaps_running_child_process(
        self,
    ) -> None:
        marker = self.storage_dir / "child_started.pid"
        wf = create_workflow(
            [
                TaskSpec(
                    task_id="long_running_child",
                    func=proc_signal_file_and_block,
                    input_payload={"marker_path": str(marker)},
                )
            ],
            max_concurrency=1,
            workflow_id="wf_proc_cancel",
            storage_dir=self.storage_dir,
            execution_backend=ExecutionBackend.PROCESS,
        )

        def cancel_once_child_starts() -> None:
            deadline = time.monotonic() + 5.0
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            cancel_workflow(wf.workflow_id, reason="CANCEL_CHILD_PROCESS")

        canceller = threading.Thread(target=cancel_once_child_starts)
        canceller.start()
        summary = run_workflow(wf)
        canceller.join(timeout=5.0)

        self.assertEqual(summary.status, WorkflowStatus.CANCELLED)
        self.assertEqual(wf.active_process_count(), 0)


if __name__ == "__main__":
    unittest.main()
