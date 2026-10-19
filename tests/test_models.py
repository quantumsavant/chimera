"""
Unit tests for `TaskSpec`, `TaskContext`, `VirtualClock`, and configuration
validation.
"""

from __future__ import annotations

import tempfile
import threading
import unittest

from chimera import (
    TaskContext,
    TaskSpec,
    VirtualClock,
    WorkflowValidationError,
    create_workflow,
    get_task_result,
)


class TestModelsAndValidation(unittest.TestCase):
    """Verify domain model validation, backoff math, and clock invariants."""

    def test_invalid_task_spec_parameters_rejected(self) -> None:
        with self.assertRaises(WorkflowValidationError):
            TaskSpec(task_id="", func=lambda: 1)

        with self.assertRaises(WorkflowValidationError):
            TaskSpec(task_id="   ", func=lambda: 1)

        with self.assertRaises(WorkflowValidationError):
            TaskSpec(task_id="bad_func", func="not_callable")  # type: ignore[arg-type]

        with self.assertRaises(WorkflowValidationError):
            TaskSpec(task_id="neg_retries", func=lambda: 1, max_retries=-1)

        with self.assertRaises(WorkflowValidationError):
            TaskSpec(task_id="zero_timeout", func=lambda: 1, timeout_seconds=0.0)

        with self.assertRaises(WorkflowValidationError):
            TaskSpec(task_id="neg_backoff", func=lambda: 1, base_backoff_seconds=-0.1)

        with self.assertRaises(WorkflowValidationError):
            TaskSpec(task_id="bad_factor", func=lambda: 1, backoff_factor=0.5)

        with self.assertRaises(WorkflowValidationError):
            TaskSpec(task_id="neg_max_backoff", func=lambda: 1, max_backoff_seconds=-1.0)

    def test_invalid_workflow_max_concurrency_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(WorkflowValidationError):
                create_workflow([], max_concurrency=0, storage_dir=tmpdir)

    def test_exponential_backoff_computation_and_cap(self) -> None:
        spec = TaskSpec(
            task_id="backoff_check",
            func=lambda: None,
            base_backoff_seconds=0.5,
            backoff_factor=2.0,
            max_backoff_seconds=3.0,
        )
        self.assertAlmostEqual(spec.compute_backoff(1), 0.5)
        self.assertAlmostEqual(spec.compute_backoff(2), 1.0)
        self.assertAlmostEqual(spec.compute_backoff(3), 2.0)
        self.assertAlmostEqual(spec.compute_backoff(4), 3.0)  # capped at 3.0

    def test_virtual_clock_monotonicity_enforcement(self) -> None:
        clock = VirtualClock(initial_time=100.0)
        self.assertEqual(clock.now(), 100.0)
        clock.advance(5.5)
        self.assertEqual(clock.now(), 105.5)
        with self.assertRaises(ValueError):
            clock.advance(-1.0)
        with self.assertRaises(ValueError):
            clock.set_time(99.0)

    def test_task_context_cancellation_signaling(self) -> None:
        ev = threading.Event()
        ctx = TaskContext(
            workflow_id="wf_1",
            task_id="t_1",
            attempt=1,
            claim_epoch=1,
            idempotency_key="wf_1:t_1",
            dependency_results={},
            _cancel_event=ev,
        )
        self.assertFalse(ctx.is_cancelled())
        ctx.raise_if_cancelled()
        ev.set()
        self.assertTrue(ctx.is_cancelled())
        with self.assertRaises(InterruptedError):
            ctx.raise_if_cancelled()

    def test_unknown_task_result_lookup_raises_key_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            wf = create_workflow(
                [TaskSpec("t1", func=lambda: 1)],
                workflow_id="wf_lookup",
                storage_dir=tmpdir,
            )
            wf.validate()
            with self.assertRaises(KeyError):
                get_task_result(wf.workflow_id, "missing_task")


if __name__ == "__main__":
    unittest.main()
