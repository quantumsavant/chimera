"""
Runnable example demonstrating the supervised `PROCESS` execution backend
(`multiprocessing.get_context("spawn")`), including hard timeout termination
of an uncooperative CPU infinite loop and recovery on a subsequent task.
"""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import time
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from chimera import (
    ExecutionBackend,
    FailureCategory,
    TaskContext,
    TaskSpec,
    create_workflow,
    get_task_result,
    run_workflow,
)


def compute_squares_in_process(payload: dict[str, Any]) -> list[int]:
    """Top-level importable task function executed in a child process."""
    return [int(x) * int(x) for x in payload["numbers"]]


def infinite_loop_until_second_attempt(
    _payload: Any, ctx: TaskContext
) -> str:
    """
    Attempt 1 enters an uncooperative CPU busy-loop (`while True: pass`) that
    ignores cooperative cancellation and can only be stopped by OS process
    termination (`SIGTERM`/`SIGKILL`). Attempt 2 succeeds immediately.
    """
    if ctx.attempt == 1:
        while True:
            time.sleep(0.01)
    return f"recovered_on_attempt_{ctx.attempt}"


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tasks = [
            TaskSpec(
                task_id="square_numbers",
                func=compute_squares_in_process,
                input_payload={"numbers": [2, 4, 6, 8]},
            ),
            TaskSpec(
                task_id="hard_timeout_recovery",
                func=infinite_loop_until_second_attempt,
                dependencies=["square_numbers"],
                timeout_seconds=0.80,
                max_retries=1,
                base_backoff_seconds=0.02,
                retry_on_timeout=True,
            ),
        ]

        wf = create_workflow(
            tasks,
            max_concurrency=2,
            workflow_id="wf_process_demo",
            storage_dir=tmpdir,
            execution_backend=ExecutionBackend.PROCESS,
        )
        summary = run_workflow(wf)
        res_sq = get_task_result(wf.workflow_id, "square_numbers")
        res_to = get_task_result(wf.workflow_id, "hard_timeout_recovery")
        print(
            f"[process_isolation_example] status={summary.status.value} "
            f"squares={res_sq.result} timeout_task_result={res_to.result!r} "
            f"attempts={res_to.attempt} timeouts={res_to.timeout_count} "
            f"active_children={wf.active_process_count()}"
        )
        assert FailureCategory.NONE == res_to.failure_category


if __name__ == "__main__":
    main()
