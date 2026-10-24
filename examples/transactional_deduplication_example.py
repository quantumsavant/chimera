"""
Runnable example demonstrating application-level transactional deduplication
using Python's standard library `sqlite3` database.

Atomicity Boundary:
- Within `record_inventory_reservation()`, the business table mutation
  (`INSERT INTO inventory_reservations`) and the deduplication marker
  (`INSERT INTO applied_operations`) execute inside a single SQLite `BEGIN IMMEDIATE`
  ACID transaction.
- If the process crashes AFTER the SQLite transaction commits but BEFORE
  CHIMERA appends `SUCCEEDED` to `<workflow_id>.chimera.log`, CHIMERA replays
  the task on recovery (at-least-once task execution), and the SQLite
  deduplication table returns the existing reservation row without duplicating
  the inventory deduction.
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
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


def init_db(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        with conn:
            conn.execute(
                """
                CREATE TABLE inventory (
                    sku TEXT PRIMARY KEY,
                    available_qty INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE applied_operations (
                    idempotency_key TEXT PRIMARY KEY,
                    sku TEXT NOT NULL,
                    reserved_qty INTEGER NOT NULL,
                    remaining_qty INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO inventory (sku, available_qty) VALUES (?, ?)",
                ("GPU-H100", 10),
            )
    finally:
        conn.close()


def reserve_stock_tx(
    db_path: Path,
    idempotency_key: str,
    sku: str,
    qty: int,
) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT sku, reserved_qty, remaining_qty FROM applied_operations WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                return {
                    "sku": row[0],
                    "reserved_qty": row[1],
                    "remaining_qty": row[2],
                    "deduplicated": True,
                }

            cur_row = conn.execute(
                "SELECT available_qty FROM inventory WHERE sku = ?",
                (sku,),
            ).fetchone()
            if cur_row is None or cur_row[0] < qty:
                raise RuntimeError(f"Insufficient stock for {sku}")

            new_qty = int(cur_row[0]) - int(qty)
            conn.execute(
                "UPDATE inventory SET available_qty = ? WHERE sku = ?",
                (new_qty, sku),
            )
            conn.execute(
                "INSERT INTO applied_operations (idempotency_key, sku, reserved_qty, remaining_qty) VALUES (?, ?, ?, ?)",
                (idempotency_key, sku, qty, new_qty),
            )
            conn.execute("COMMIT")
            return {
                "sku": sku,
                "reserved_qty": qty,
                "remaining_qty": new_qty,
                "deduplicated": False,
            }
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "warehouse.sqlite3"
        init_db(db_path)

        crashed_once = False

        def reserve_task(
            payload: dict[str, Any], ctx: TaskContext
        ) -> dict[str, Any]:
            return reserve_stock_tx(
                db_path=Path(payload["db_path"]),
                idempotency_key=ctx.idempotency_key,
                sku=payload["sku"],
                qty=payload["qty"],
            )

        def crash_after_sqlite_commit(token: ClaimToken, outcome: str) -> None:
            nonlocal crashed_once
            if (
                token.task_id == "reserve_gpu"
                and outcome == "SUCCEEDED"
                and not crashed_once
            ):
                crashed_once = True
                raise SimulatedProcessCrash(
                    "Crash after SQLite COMMIT but before CHIMERA EventLog append"
                )

        tasks = [
            TaskSpec(
                task_id="reserve_gpu",
                func=reserve_task,
                input_payload={
                    "db_path": str(db_path),
                    "sku": "GPU-H100",
                    "qty": 3,
                },
                operation_key="order:ORD-700:reserve_gpu",
            )
        ]

        wf = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="wf_sqlite_tx_demo",
            storage_dir=tmp_path,
        )
        wf.before_task_commit_hook = crash_after_sqlite_commit

        try:
            run_workflow(wf)
        except SimulatedProcessCrash as exc:
            print(f"[transactional_dedup_example] Caught expected crash: {exc}")

        summary = recover_workflow(
            "wf_sqlite_tx_demo", tasks=tasks, storage_dir=tmp_path
        )
        res = get_task_result("wf_sqlite_tx_demo", "reserve_gpu")
        print(
            f"[transactional_dedup_example] status={summary.status.value} "
            f"result={res.result}"
        )


if __name__ == "__main__":
    main()
