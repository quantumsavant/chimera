"""
Unit and integration tests for DAG validation, deterministic priority and
lexicographical scheduling, diamond dependencies, and downstream failure cascade.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
import threading
from typing import Any
import unittest

from chimera import (
    CycleDetectedError,
    DuplicateTaskIdError,
    MissingDependencyError,
    TaskContext,
    TaskSpec,
    TaskState,
    VirtualClock,
    WorkflowStatus,
    create_workflow,
    get_task_result,
    run_workflow,
    validate_workflow,
)


class TestSchedulerAndDAGValidation(unittest.TestCase):
    """Verify DAG validation, scheduling order, and dependency blocking."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp.name)
        self.clock = VirtualClock(auto_advance_on_wait=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_rejects_duplicate_ids_missing_deps_and_deep_cycles(self) -> None:
        noop = lambda: None
        with self.assertRaises(DuplicateTaskIdError):
            validate_workflow([TaskSpec("a", func=noop), TaskSpec("a", func=noop)])

        with self.assertRaises(MissingDependencyError):
            validate_workflow([TaskSpec("a", func=noop, dependencies=["ghost"])])

        # 8-level graph with a 5-node cycle buried at depth 3
        cycle_tasks = [
            TaskSpec("n0", func=noop),
            TaskSpec("n1", func=noop, dependencies=["n0"]),
            TaskSpec("n2", func=noop, dependencies=["n1"]),
            TaskSpec("n3", func=noop, dependencies=["n2", "n7"]),
            TaskSpec("n4", func=noop, dependencies=["n3"]),
            TaskSpec("n5", func=noop, dependencies=["n4"]),
            TaskSpec("n6", func=noop, dependencies=["n5"]),
            TaskSpec("n7", func=noop, dependencies=["n6"]),
        ]
        with self.assertRaises(CycleDetectedError) as ctx:
            validate_workflow(cycle_tasks)
        self.assertEqual(
            ctx.exception.cycle_path, ["n3", "n4", "n5", "n6", "n7", "n3"]
        )

    def test_deterministic_priority_and_lexicographical_tie_breaking(self) -> None:
        order: list[str] = []
        tasks = [
            TaskSpec(
                "task_c",
                priority=10,
                func=lambda: order.append("task_c"),
            ),
            TaskSpec(
                "task_b",
                priority=5,
                func=lambda: order.append("task_b"),
            ),
            TaskSpec(
                "task_a",
                priority=5,
                func=lambda: order.append("task_a"),
            ),
        ]
        wf = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="wf_prio_order",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        # Priority 5 runs before Priority 10; within Priority 5, task_a < task_b
        self.assertEqual(order, ["task_a", "task_b", "task_c"])

    def test_diamond_dag_concurrent_execution_and_join(self) -> None:
        barrier = threading.Barrier(2, timeout=5.0)

        def left_fn(_p: Any, ctx: TaskContext) -> int:
            barrier.wait()
            return ctx.dependency_results["root"] * 3

        def right_fn(_p: Any, ctx: TaskContext) -> int:
            barrier.wait()
            return ctx.dependency_results["root"] + 7

        tasks = [
            TaskSpec("root", func=lambda: 10),
            TaskSpec("left", func=left_fn, dependencies=["root"]),
            TaskSpec("right", func=right_fn, dependencies=["root"]),
            TaskSpec(
                "sink",
                func=lambda _p, ctx: (
                    ctx.dependency_results["left"]
                    + ctx.dependency_results["right"]
                ),
                dependencies=["left", "right"],
            ),
        ]
        wf = create_workflow(
            tasks,
            max_concurrency=2,
            workflow_id="wf_diamond",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(get_task_result(wf.workflow_id, "sink").result, 47)

    def test_failed_task_exhausting_retries_blocks_all_downstream_tasks(self) -> None:
        attempts: list[float] = []
        downstream_called = False

        def always_fails() -> None:
            attempts.append(self.clock.now())
            raise RuntimeError("boom")

        def downstream_fn() -> None:
            nonlocal downstream_called
            downstream_called = True

        tasks = [
            TaskSpec(
                "root_fail",
                func=always_fails,
                max_retries=2,
                base_backoff_seconds=1.0,
                backoff_factor=2.0,
            ),
            TaskSpec("mid", func=downstream_fn, dependencies=["root_fail"]),
            TaskSpec("leaf", func=downstream_fn, dependencies=["mid"]),
        ]
        wf = create_workflow(
            tasks,
            max_concurrency=2,
            workflow_id="wf_cascade_fail",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.FAILED)
        self.assertFalse(downstream_called)
        self.assertEqual(len(attempts), 3)
        self.assertEqual(summary.tasks["root_fail"].state, TaskState.FAILED)
        self.assertEqual(summary.tasks["mid"].state, TaskState.CANCELLED)
        self.assertEqual(summary.tasks["leaf"].state, TaskState.CANCELLED)


if __name__ == "__main__":
    unittest.main()
