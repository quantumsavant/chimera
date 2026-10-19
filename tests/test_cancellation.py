"""
Tests for workflow cancellation, racing cancellation against task completion,
late cancellation on completed workflows, and stale completions after cancel.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
import threading
import unittest

from chimera import (
    ClaimToken,
    TaskSpec,
    TaskState,
    VirtualClock,
    WorkflowStatus,
    cancel_workflow,
    create_workflow,
    run_workflow,
)


class TestWorkflowCancellation(unittest.TestCase):
    """Verify cancellation semantics and terminal state preservation."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp.name)
        self.clock = VirtualClock(auto_advance_on_wait=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_cancel_before_run_cancels_all_tasks(self) -> None:
        executed = False

        def should_not_run() -> None:
            nonlocal executed
            executed = True

        wf = create_workflow(
            [
                TaskSpec("t1", func=should_not_run),
                TaskSpec("t2", func=should_not_run, dependencies=["t1"]),
            ],
            workflow_id="wf_cancel_pre",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        summary = cancel_workflow(wf.workflow_id, reason="PRE_RUN_ABORT")
        self.assertEqual(summary.status, WorkflowStatus.CANCELLED)
        self.assertFalse(executed)
        self.assertEqual(summary.tasks["t1"].state, TaskState.CANCELLED)
        self.assertEqual(summary.tasks["t2"].state, TaskState.CANCELLED)

    def test_cancellation_racing_with_completion_and_late_cancellation(
        self,
    ) -> None:
        barrier = threading.Barrier(2, timeout=5.0)
        wf = create_workflow(
            [TaskSpec("t1", func=lambda: "ok")],
            max_concurrency=1,
            workflow_id="wf_cancel_race",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )

        def on_before_commit(token: ClaimToken, _outcome: str) -> None:
            if token.task_id == "t1":
                barrier.wait()

        wf.before_task_commit_hook = on_before_commit

        def canceller() -> None:
            barrier.wait()
            cancel_workflow(wf.workflow_id)

        t = threading.Thread(target=canceller)
        t.start()
        summary = run_workflow(wf)
        t.join(timeout=5.0)

        t1_state = summary.tasks["t1"].state
        self.assertIn(t1_state, (TaskState.SUCCEEDED, TaskState.CANCELLED))
        if t1_state == TaskState.SUCCEEDED:
            self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        else:
            self.assertEqual(summary.status, WorkflowStatus.CANCELLED)

        # Calling cancel on an already-SUCCEEDED workflow never overwrites SUCCEEDED
        wf_done = create_workflow(
            [TaskSpec("t_done", func=lambda: 123)],
            workflow_id="wf_already_done",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        run_workflow(wf_done)
        after_cancel = cancel_workflow(wf_done.workflow_id)
        self.assertEqual(after_cancel.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(
            after_cancel.tasks["t_done"].state, TaskState.SUCCEEDED
        )


if __name__ == "__main__":
    unittest.main()
