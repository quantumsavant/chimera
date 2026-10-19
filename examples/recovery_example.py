"""
Runnable example demonstrating crash recovery from a truncated final event-log
record without re-executing tasks whose completion was already durably committed.
"""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from chimera import (
    SimulatedProcessCrash,
    TaskContext,
    TaskSpec,
    TaskState,
    create_workflow,
    get_task_result,
    recover_workflow,
    run_workflow,
)


def main() -> None:
    execution_counts = {"step_1": 0, "step_2": 0}

    def step_1_fn() -> str:
        execution_counts["step_1"] += 1
        return "checkpoint_alpha"

    def step_2_fn(_payload: Any, ctx: TaskContext) -> str:
        execution_counts["step_2"] += 1
        return f"{ctx.dependency_results['step_1']} -> completed_step_2"

    def crash_during_step_2_ready_write(
        stage: str, rec: dict[str, Any], _seq: int
    ) -> None:
        if (
            stage == "DURING_PARTIAL_WRITE"
            and rec.get("event_type") == "TASK_STATE_CHANGED"
            and rec["data"].get("task_id") == "step_2"
            and rec["data"].get("to_state") == TaskState.READY.value
        ):
            raise SimulatedProcessCrash(
                "Simulated power failure mid-write after step_1 SUCCEEDED"
            )

    with tempfile.TemporaryDirectory() as tmpdir:
        wf = create_workflow(
            [
                TaskSpec("step_1", func=step_1_fn),
                TaskSpec("step_2", func=step_2_fn, dependencies=["step_1"]),
            ],
            max_concurrency=1,
            workflow_id="wf_recovery_demo",
            storage_dir=tmpdir,
            crash_hook=crash_during_step_2_ready_write,
            partial_write_bytes=24,
        )

        try:
            run_workflow(wf)
        except SimulatedProcessCrash as exc:
            print(f"[recovery_example] Caught expected crash: {exc}")

        print(f"[recovery_example] Counts before recovery: {execution_counts}")
        recovered_summary = recover_workflow("wf_recovery_demo")
        final_output = get_task_result("wf_recovery_demo", "step_2")
        print(
            f"[recovery_example] Recovered status={recovered_summary.status.value} "
            f"tail_repaired={recovered_summary.truncated_tail_repaired} "
            f"counts_after={execution_counts} output={final_output.result!r}"
        )


if __name__ == "__main__":
    main()
