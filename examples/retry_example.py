"""
Runnable example demonstrating task retries, exponential backoff, and
configurable retryable exception filtering using `VirtualClock`.
"""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from chimera import (
    TaskContext,
    TaskSpec,
    VirtualClock,
    create_workflow,
    get_task_result,
    run_workflow,
)


def main() -> None:
    clock = VirtualClock(initial_time=1_700_000_000.0, auto_advance_on_wait=True)
    call_timestamps: list[tuple[int, float]] = []

    def flaky_api_call(_payload: Any, ctx: TaskContext) -> dict[str, Any]:
        call_timestamps.append((ctx.attempt, clock.now()))
        if ctx.attempt < 3:
            raise ConnectionError(f"Transient failure on attempt {ctx.attempt}")
        return {"status": 200, "attempt": ctx.attempt}

    with tempfile.TemporaryDirectory() as tmpdir:
        wf = create_workflow(
            [
                TaskSpec(
                    task_id="fetch_remote_config",
                    func=flaky_api_call,
                    max_retries=3,
                    base_backoff_seconds=0.5,
                    backoff_factor=2.0,
                    retryable_exceptions=(ConnectionError, TimeoutError),
                )
            ],
            max_concurrency=1,
            workflow_id="wf_retry_demo",
            storage_dir=tmpdir,
            clock=clock,
        )
        summary = run_workflow(wf)
        res = get_task_result(wf.workflow_id, "fetch_remote_config")
        deltas = [round(ts - call_timestamps[0][1], 2) for _, ts in call_timestamps]
        print(
            f"[retry_example] status={summary.status.value} "
            f"attempts={res.attempt} backoff_offsets_sec={deltas} result={res.result}"
        )


if __name__ == "__main__":
    main()
