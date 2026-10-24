"""
Runnable example demonstrating external API side-effect deduplication using
`TaskContext.idempotency_key` and explicit `TaskSpec(operation_key=...)`
across a crash in the external-side-effect-to-durable-commit window.

Delivery Semantics Demonstrated:
- CHIMERA guarantees at-least-once task execution prior to durable `SUCCEEDED`
  commit, and zero re-execution after durable `SUCCEEDED` commit.
- Because a local event log cannot atomically commit an external HTTP/API call,
  a crash immediately after the external call returns—before `SUCCEEDED` is
  `fsync`ed locally—causes recovery to re-execute the task.
- Passing `ctx.idempotency_key` (or a stable business `operation_key`) to the
  external API ensures the external side effect is applied only once.
"""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from chimera import (
    ClaimToken,
    SimulatedProcessCrash,
    TaskContext,
    TaskSpec,
    create_workflow,
    get_task_result,
    recover_workflow,
    run_workflow,
)


class SimulatedExternalPaymentGateway:
    """Simulated external API that deduplicates charges by `idempotency_key`."""

    def __init__(self) -> None:
        self.raw_http_requests: int = 0
        self.committed_charges: dict[str, dict[str, Any]] = {}

    def charge_card(
        self,
        *,
        idempotency_key: str,
        customer_id: str,
        amount_cents: int,
    ) -> dict[str, Any]:
        self.raw_http_requests += 1
        if idempotency_key in self.committed_charges:
            cached = dict(self.committed_charges[idempotency_key])
            cached["deduplicated_replay"] = True
            return cached

        receipt = {
            "charge_id": f"ch_{len(self.committed_charges) + 1:04d}",
            "customer_id": customer_id,
            "amount_cents": amount_cents,
            "idempotency_key": idempotency_key,
            "deduplicated_replay": False,
        }
        self.committed_charges[idempotency_key] = receipt
        return dict(receipt)


def main() -> None:
    gateway = SimulatedExternalPaymentGateway()
    crashed_once = False

    def process_payment(
        payload: dict[str, Any], ctx: TaskContext
    ) -> dict[str, Any]:
        return gateway.charge_card(
            idempotency_key=ctx.idempotency_key,
            customer_id=payload["customer_id"],
            amount_cents=payload["amount_cents"],
        )

    def crash_before_local_commit(token: ClaimToken, outcome: str) -> None:
        nonlocal crashed_once
        if (
            token.task_id == "charge_invoice_1042"
            and outcome == "SUCCEEDED"
            and not crashed_once
        ):
            crashed_once = True
            raise SimulatedProcessCrash(
                "Crash after external API charge succeeded, before local SUCCEEDED log commit"
            )

    with tempfile.TemporaryDirectory() as tmpdir:
        tasks = [
            TaskSpec(
                task_id="charge_invoice_1042",
                func=process_payment,
                input_payload={"customer_id": "cust_88", "amount_cents": 19900},
                operation_key="billing:invoice_1042:charge_v1",
            )
        ]
        wf = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="wf_idempotent_api_demo",
            storage_dir=tmpdir,
        )
        wf.before_task_commit_hook = crash_before_local_commit

        try:
            run_workflow(wf)
        except SimulatedProcessCrash as exc:
            print(f"[idempotent_api_example] Caught simulated crash: {exc}")

        summary = recover_workflow("wf_idempotent_api_demo", tasks=tasks, storage_dir=tmpdir)
        res = get_task_result("wf_idempotent_api_demo", "charge_invoice_1042")
        print(
            f"[idempotent_api_example] status={summary.status.value} "
            f"raw_api_calls={gateway.raw_http_requests} "
            f"unique_charges={len(gateway.committed_charges)} "
            f"result={res.result}"
        )


if __name__ == "__main__":
    main()
