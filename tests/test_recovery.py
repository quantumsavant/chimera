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



    def test_recovery_across_real_os_process_restarts(self) -> None:
        """
        Phase 9: Verify recovery across real OS process boundaries (Process 1
        crashes after committing t1 SUCCEEDED; Process 2 recovers from disk
        without any shared in-memory state and completes t2 without re-running t1).
        """
        import os
        import subprocess
        import sys

        src_dir = str(Path(__file__).resolve().parents[1] / "src")
        env = dict(os.environ)
        env["PYTHONPATH"] = (
            src_dir
            if not env.get("PYTHONPATH")
            else f"{src_dir}{os.pathsep}{env['PYTHONPATH']}"
        )
        counter_file = self.storage_dir / "t1_runs.txt"

        proc1_code = (
            "import sys\n"
            "from pathlib import Path\n"
            "from chimera import TaskSpec, TaskState, SimulatedProcessCrash, create_workflow, run_workflow\n"
            f"storage = Path({str(self.storage_dir)!r})\n"
            f"counter = Path({str(counter_file)!r})\n"
            "def t1_fn():\n"
            "    prev = int(counter.read_text()) if counter.exists() else 0\n"
            "    counter.write_text(str(prev + 1))\n"
            "    return {'val': 50}\n"
            "def t2_fn(_p, ctx):\n"
            "    return ctx.dependency_results['t1']['val'] + 7\n"
            "def crash_after_t1(stage, rec, _seq):\n"
            "    if stage == 'AFTER_FSYNC' and rec.get('event_type') == 'TASK_STATE_CHANGED' and rec['data'].get('task_id') == 't1' and rec['data'].get('to_state') == 'SUCCEEDED':\n"
            "        os._exit(99)\n"
            "import os\n"
            "wf = create_workflow([TaskSpec('t1', func=t1_fn), TaskSpec('t2', func=t2_fn, dependencies=['t1'])], max_concurrency=1, workflow_id='wf_real_proc_restart', storage_dir=storage, crash_hook=crash_after_t1)\n"
            "run_workflow(wf)\n"
        )
        p1 = subprocess.run([sys.executable, "-c", proc1_code], env=env, capture_output=True, text=True, timeout=10.0)
        self.assertEqual(p1.returncode, 99, p1.stderr)
        self.assertEqual(counter_file.read_text(), "1")

        proc2_code = (
            "import json, sys\n"
            "from pathlib import Path\n"
            "from chimera import TaskSpec, recover_workflow, get_task_result\n"
            f"storage = Path({str(self.storage_dir)!r})\n"
            f"counter = Path({str(counter_file)!r})\n"
            "def t1_fn():\n"
            "    prev = int(counter.read_text()) if counter.exists() else 0\n"
            "    counter.write_text(str(prev + 1))\n"
            "    return {'val': 50}\n"
            "def t2_fn(_p, ctx):\n"
            "    return ctx.dependency_results['t1']['val'] + 7\n"
            "summary = recover_workflow('wf_real_proc_restart', tasks=[TaskSpec('t1', func=t1_fn), TaskSpec('t2', func=t2_fn, dependencies=['t1'])], storage_dir=storage)\n"
            "res = get_task_result('wf_real_proc_restart', 't2')\n"
            "print(json.dumps({'status': summary.status.value, 'result': res.result}))\n"
        )
        p2 = subprocess.run([sys.executable, "-c", proc2_code], env=env, capture_output=True, text=True, timeout=10.0)
        self.assertEqual(p2.returncode, 0, p2.stderr)
        self.assertEqual(counter_file.read_text(), "1")
        self.assertIn('"result": 57', p2.stdout)


if __name__ == "__main__":
    unittest.main()
