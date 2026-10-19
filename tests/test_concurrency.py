"""
Tests for worker concurrency bounds, simultaneous task claim exclusion,
cross-process and in-process file lock contention, and payload isolation.
"""

from __future__ import annotations

import copy
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from typing import Any
import unittest

from chimera import (
    ConcurrentWorkflowAccessError,
    TaskContext,
    TaskSpec,
    VirtualClock,
    WorkflowEngine,
    WorkflowStatus,
    create_workflow,
    get_task_result,
    run_workflow,
)


class TestConcurrencyAndIsolation(unittest.TestCase):
    """Verify single-claim exclusion, process locks, and payload isolation."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp.name)
        self.clock = VirtualClock(auto_advance_on_wait=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_simultaneous_worker_claim_exclusion(self) -> None:
        wf = create_workflow(
            [TaskSpec("shared_task", func=lambda: "ok")],
            max_concurrency=4,
            workflow_id="wf_claim_race",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf._ensure_initialized_in_log()

        barrier = threading.Barrier(2, timeout=5.0)
        results: list[Any] = [None, None]

        def claim_worker(idx: int) -> None:
            barrier.wait()
            results[idx] = wf.try_claim_specific_task(
                "shared_task", f"worker-{idx}"
            )

        t0 = threading.Thread(target=claim_worker, args=(0,))
        t1 = threading.Thread(target=claim_worker, args=(1,))
        t0.start()
        t1.start()
        t0.join(timeout=5.0)
        t1.join(timeout=5.0)

        succeeded_claims = [r for r in results if r is not None]
        self.assertEqual(len(succeeded_claims), 1)

    def test_cross_process_and_in_process_lock_contention(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def hold_lock_task() -> str:
            entered.set()
            release.wait(timeout=10.0)
            return "owner_done"

        wf1 = create_workflow(
            [TaskSpec("t1", func=hold_lock_task)],
            workflow_id="wf_locked",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        runner = threading.Thread(target=lambda: run_workflow(wf1))
        runner.start()
        self.assertTrue(entered.wait(timeout=5.0))

        try:
            src_dir = str(Path(__file__).resolve().parents[1] / "src")
            env = dict(os.environ)
            env["PYTHONPATH"] = (
                src_dir
                if not env.get("PYTHONPATH")
                else f"{src_dir}{os.pathsep}{env['PYTHONPATH']}"
            )
            child_script = (
                "import sys\n"
                "from chimera import TaskSpec, recover_workflow, ConcurrentWorkflowAccessError\n"
                "try:\n"
                f"    recover_workflow('wf_locked', tasks=[TaskSpec('t1', func=lambda: 'x')], storage_dir={str(self.storage_dir)!r})\n"
                "    sys.exit(2)\n"
                "except ConcurrentWorkflowAccessError:\n"
                "    sys.exit(0)\n"
            )
            proc = subprocess.run(
                [sys.executable, "-c", child_script],
                env=env,
                capture_output=True,
                text=True,
                timeout=5.0,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)

            wf2 = WorkflowEngine(
                tasks=[TaskSpec("t1", func=lambda: "x")],
                workflow_id="wf_locked",
                storage_dir=self.storage_dir,
            )
            with self.assertRaises(ConcurrentWorkflowAccessError):
                wf2.run()
        finally:
            release.set()
            runner.join(timeout=5.0)

    def test_payload_and_dependency_result_deep_copy_isolation(self) -> None:
        shared = {"items": [1, 2], "nested": {"k": "orig"}}

        def mutate_payload(payload: dict[str, Any]) -> dict[str, Any]:
            payload["items"].append(999)
            payload["nested"]["k"] = "mutated"
            return {"out": [10, 20]}

        def read_payload(
            payload: dict[str, Any], ctx: TaskContext
        ) -> dict[str, Any]:
            ctx.dependency_results["t1"]["out"].append(888)
            return copy.deepcopy(payload)

        wf = create_workflow(
            [
                TaskSpec("t1", func=mutate_payload, input_payload=shared),
                TaskSpec(
                    "t2",
                    func=read_payload,
                    input_payload=shared,
                    dependencies=["t1"],
                ),
            ],
            workflow_id="wf_isolation",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        shared["items"].append(777)
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(
            get_task_result(wf.workflow_id, "t2").result,
            {"items": [1, 2], "nested": {"k": "orig"}},
        )
        self.assertEqual(
            get_task_result(wf.workflow_id, "t1").result,
            {"out": [10, 20]},
        )


if __name__ == "__main__":
    unittest.main()
