"""
Tests for workflow recovery after interruption, non-re-execution of committed
tasks, repeated recovery idempotency, and detection of altered TaskSpecs.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any
import unittest

from chimera import (
    SimulatedProcessCrash,
    TaskContext,
    TaskSpec,
    TaskState,
    VirtualClock,
    WorkflowStatus,
    WorkflowValidationError,
    create_workflow,
    get_task_result,
    recover_workflow,
    run_workflow,
)


class TestWorkflowRecovery(unittest.TestCase):
    """Verify state reconstruction, idempotency of recovery, and spec validation."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp.name)
        self.clock = VirtualClock(auto_advance_on_wait=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_committed_tasks_never_rerun_and_repeated_recovery_is_idempotent(
        self,
    ) -> None:
        counts = {"t1": 0, "t2": 0}
        crashed = False

        def t1_fn() -> int:
            counts["t1"] += 1
            return 21

        def t2_fn(_p: Any, ctx: TaskContext) -> int:
            counts["t2"] += 1
            return ctx.dependency_results["t1"] * 2

        def crash_after_t1(stage: str, rec: dict[str, Any], _seq: int) -> None:
            nonlocal crashed
            if (
                not crashed
                and stage == "AFTER_FSYNC"
                and rec.get("event_type") == "TASK_STATE_CHANGED"
                and rec["data"].get("task_id") == "t1"
                and rec["data"].get("to_state") == TaskState.SUCCEEDED.value
            ):
                crashed = True
                raise SimulatedProcessCrash("Crash after t1 SUCCEEDED")

        wf = create_workflow(
            [
                TaskSpec("t1", func=t1_fn),
                TaskSpec("t2", func=t2_fn, dependencies=["t1"]),
            ],
            max_concurrency=1,
            workflow_id="wf_recover_once",
            storage_dir=self.storage_dir,
            clock=self.clock,
            crash_hook=crash_after_t1,
        )
        with self.assertRaises(SimulatedProcessCrash):
            run_workflow(wf)

        self.assertEqual(counts, {"t1": 1, "t2": 0})

        # First recovery finishes t2 without re-running t1
        summary1 = recover_workflow("wf_recover_once")
        self.assertEqual(summary1.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(counts, {"t1": 1, "t2": 1})
        self.assertEqual(get_task_result("wf_recover_once", "t2").result, 42)

        # Second recovery on an already-completed workflow is a no-op (reruns nothing)
        summary2 = recover_workflow("wf_recover_once")
        self.assertEqual(summary2.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(counts, {"t1": 1, "t2": 1})

    def test_recovery_rejects_altered_task_specs_or_dependencies(self) -> None:
        wf = create_workflow(
            [
                TaskSpec(
                    "a",
                    func=lambda p: p["v"],
                    input_payload={"v": 5},
                    max_retries=2,
                    timeout_seconds=10.0,
                ),
                TaskSpec("b", func=lambda: 10, dependencies=["a"]),
            ],
            workflow_id="wf_spec_guard",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        run_workflow(wf)

        # Altered input_payload
        with self.assertRaises(WorkflowValidationError):
            recover_workflow(
                "wf_spec_guard",
                tasks=[
                    TaskSpec(
                        "a",
                        func=lambda p: p["v"],
                        input_payload={"v": 999},
                        max_retries=2,
                        timeout_seconds=10.0,
                    ),
                    TaskSpec("b", func=lambda: 10, dependencies=["a"]),
                ],
                storage_dir=self.storage_dir,
            )

        # Altered dependency graph
        with self.assertRaises(WorkflowValidationError):
            recover_workflow(
                "wf_spec_guard",
                tasks=[
                    TaskSpec(
                        "a",
                        func=lambda p: p["v"],
                        input_payload={"v": 5},
                        max_retries=2,
                        timeout_seconds=10.0,
                    ),
                    TaskSpec("b", func=lambda: 10, dependencies=[]),
                ],
                storage_dir=self.storage_dir,
            )


if __name__ == "__main__":
    unittest.main()
