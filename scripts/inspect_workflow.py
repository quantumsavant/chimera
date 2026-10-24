"""
Read-only CLI diagnostic tool for inspecting CHIMERA workflow event logs.

Usage:
    python3 scripts/inspect_workflow.py <path/to/workflow.chimera.log> [--workflow-id WF_ID] [--hmac-key-hex HEX] [--json]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from chimera.diagnostics import inspect_workflow_log


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only diagnostic inspector for CHIMERA .chimera.log files."
    )
    parser.add_argument(
        "log_path",
        type=str,
        help="Path to the <workflow_id>.chimera.log file to inspect.",
    )
    parser.add_argument(
        "--workflow-id",
        type=str,
        default=None,
        help="Optional explicit workflow_id (inferred from filename if omitted).",
    )
    parser.add_argument(
        "--hmac-key-hex",
        type=str,
        default=None,
        help="Optional hex-encoded HMAC-SHA256 key for @CHIMERA2 authenticated logs.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON output.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    hmac_key = (
        bytes.fromhex(args.hmac_key_hex)
        if args.hmac_key_hex is not None
        else None
    )
    report = inspect_workflow_log(
        args.log_path,
        workflow_id=args.workflow_id,
        hmac_key=hmac_key,
    )

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(f"=== CHIMERA Workflow Log Diagnostic Report ===")
        print(f"Log Path           : {report.log_path}")
        print(f"Workflow ID        : {report.workflow_id}")
        print(f"Exists             : {report.exists}")
        print(f"Healthy            : {report.is_healthy}")
        print(f"Frame Format       : @CHIMERA{report.format_version}")
        print(f"Schema Version     : v{report.schema_version}")
        print(f"Definition Version : {report.definition_version}")
        print(f"Definition Hash    : {report.definition_hash or '<none (v1)>'}")
        print(f"Record Count       : {report.record_count}")
        print(f"Workflow Status    : {report.workflow_status}")
        print(f"Cancel Requested   : {report.cancel_requested}")
        print(f"Truncated Tail     : {report.truncated_tail_detected}")
        if report.corruption_error:
            print(f"Corruption Error   : {report.corruption_error}")
        if report.warnings:
            print(f"Warnings           : {report.warnings}")
        print(f"Counts by State    : {report.counts_by_state}")
        print(f"Aggregate Metrics  : {report.metrics}")
        if report.tasks:
            print("--- Tasks ---")
            for tid, t_info in sorted(report.tasks.items()):
                print(
                    f"  {tid}: state={t_info['state']} attempt={t_info['attempt']} "
                    f"epoch={t_info['claim_epoch']} retries={t_info['retry_count']} "
                    f"timeouts={t_info['timeout_count']} category={t_info['failure_category']}"
                )

    return 0 if report.is_healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
