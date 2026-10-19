"""
Minimal runnable example demonstrating a diamond DAG workflow with concurrent
branches, dependency result passing, and payload isolation.
"""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile

# Allow running directly from a clean checkout without prior pip install
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from chimera import (
    TaskContext,
    TaskSpec,
    create_workflow,
    get_task_result,
    run_workflow,
    validate_workflow,
)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tasks = [
            TaskSpec(
                task_id="extract_orders",
                input_payload={"records": [100, 250, 400]},
                func=lambda payload: [
                    x for x in payload["records"] if x >= 150
                ],
            ),
            TaskSpec(
                task_id="compute_subtotal",
                dependencies=["extract_orders"],
                func=lambda _p, ctx: sum(
                    ctx.dependency_results["extract_orders"]
                ),
            ),
            TaskSpec(
                task_id="compute_tax",
                dependencies=["extract_orders"],
                input_payload={"tax_rate": 0.08},
                func=lambda payload, ctx: round(
                    sum(ctx.dependency_results["extract_orders"])
                    * payload["tax_rate"],
                    2,
                ),
            ),
            TaskSpec(
                task_id="generate_invoice",
                dependencies=["compute_subtotal", "compute_tax"],
                func=lambda _p, ctx: {
                    "subtotal": ctx.dependency_results["compute_subtotal"],
                    "tax": ctx.dependency_results["compute_tax"],
                    "total": ctx.dependency_results["compute_subtotal"]
                    + ctx.dependency_results["compute_tax"],
                },
            ),
        ]

        wf = create_workflow(
            tasks,
            max_concurrency=2,
            workflow_id="wf_basic_demo",
            storage_dir=tmpdir,
        )
        validate_workflow(wf)
        summary = run_workflow(wf)
        invoice = get_task_result(wf.workflow_id, "generate_invoice")
        print(
            f"[basic_workflow] status={summary.status.value} "
            f"invoice={invoice.result} log_records={summary.log_record_count}"
        )


if __name__ == "__main__":
    main()
