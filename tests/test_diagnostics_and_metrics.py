"""
Tests for Phases 5, 6, 8, and 10:
- `workflow_id` path traversal and Windows reserved name validation (`InvalidWorkflowIdError`).
- Restrictive file permissions (`0o600`) on POSIX.
- Strict canonical JSON validation (`PayloadSerializationError`, `OversizedPayloadError`, `OversizedFrameError`).
- `@CHIMERA2|` HMAC-SHA256 authenticated framing and `migrate_v1_to_v2()`.
- `InMemoryMetricsSink`, `FailureCategory`, queue/execution duration tracking, and sensitive payload redaction.
- Read-only CLI/API inspection (`inspect_workflow_log` and `scripts/inspect_workflow.py`).
- Public snapshot immutability.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest

from chimera import (
    EventLog,
    FailureCategory,
    InMemoryMetricsSink,
    InvalidWorkflowIdError,
    LogAuthenticationError,
    OversizedFrameError,
    OversizedPayloadError,
    PayloadSerializationError,
    TaskSpec,
    VirtualClock,
    WorkflowStatus,
    create_workflow,
    get_task_result,
    inspect_workflow_log,
    recover_workflow,
    run_workflow,
)


class TestDiagnosticsMetricsAndHardening(unittest.TestCase):
    """Verify security boundaries, lossless serialization, HMAC v2 logs, metrics, and CLI diagnostics."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp.name)
        self.clock = VirtualClock(
            initial_time=1_700_000_000.0, auto_advance_on_wait=True
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_workflow_id_validation_rejects_path_traversal_and_reserved_names(
        self,
    ) -> None:
        invalid_ids = [
            "",
            "   ",
            "../escape",
            "../../etc/passwd",
            "sub/wf",
            "sub\\wf",
            ".hidden",
            "wf with spaces",
            "CON",
            "nul",
            "AUX.log",
            "COM1",
            "LPT9",
            "a" * 129,
        ]
        for bad_id in invalid_ids:
            with self.subTest(bad_id=bad_id):
                with self.assertRaises(InvalidWorkflowIdError):
                    create_workflow(
                        [], workflow_id=bad_id, storage_dir=self.storage_dir
                    )

    @unittest.skipUnless(os.name == "posix", "POSIX file mode check")
    def test_restrictive_file_permissions_0o600_on_posix(self) -> None:
        wf = create_workflow(
            [TaskSpec("t1", func=lambda: "secret")],
            workflow_id="wf_perms_check",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        run_workflow(wf)
        log_mode = stat.S_IMODE(wf.log_path.stat().st_mode)
        lock_mode = stat.S_IMODE(wf.lock_path.stat().st_mode)
        self.assertEqual(log_mode, 0o600)
        self.assertEqual(lock_mode, 0o600)

    def test_payload_serialization_rejects_non_json_types_nan_infinity_and_oversized_payloads(
        self,
    ) -> None:
        # 1. Non-JSON types in input_payload rejected at construction
        for bad_payload in (
            {"set_val": {1, 2, 3}},
            {"nan_val": float("nan")},
            {"inf_val": float("inf")},
            {"neg_inf": float("-inf")},
            {123: "non_string_key"},
            {"obj": object()},
        ):
            with self.assertRaises(PayloadSerializationError):
                TaskSpec("bad_input", func=lambda p: p, input_payload=bad_payload)

        # 2. Task returning non-JSON object fails attempt with PayloadSerializationError (no repr fallback!)
        wf_bad_ret = create_workflow(
            [TaskSpec("ret_set", func=lambda: {"unserializable_set"})],
            workflow_id="wf_bad_ret",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        summary = run_workflow(wf_bad_ret)
        self.assertEqual(summary.status, WorkflowStatus.FAILED)
        res = get_task_result(wf_bad_ret.workflow_id, "ret_set")
        self.assertEqual(res.error_type, "PayloadSerializationError")
        self.assertEqual(
            res.failure_category, FailureCategory.SERIALIZATION_ERROR
        )

        # 3. Oversized payload and oversized frame header rejected
        wf_small_cap = create_workflow(
            [TaskSpec("big_in", func=lambda: "X" * 500)],
            workflow_id="wf_small_cap",
            storage_dir=self.storage_dir,
            clock=self.clock,
            max_output_bytes=100,
        )
        sum_cap = run_workflow(wf_small_cap)
        self.assertEqual(sum_cap.status, WorkflowStatus.FAILED)
        self.assertEqual(
            get_task_result("wf_small_cap", "big_in").failure_category,
            FailureCategory.SERIALIZATION_ERROR,
        )

        # 4. Oversized frame header during replay raises OversizedFrameError
        log = EventLog(
            log_path=self.storage_dir / "wf_oversized_frame.chimera.log",
            workflow_id="wf_oversized_frame",
            max_frame_bytes=1024,
        )
        # Craft a header claiming 50,000 bytes
        fake_header = b"@CHIMERA1|0000000001|00050000|12345678|" + (b"a" * 60)
        log.log_path.write_bytes(fake_header)
        with self.assertRaises(OversizedFrameError):
            log.replay()

    def test_hmac_sha256_v2_framing_and_v1_to_v2_migration(self) -> None:
        secret_key = b"super-secret-production-hmac-key"
        tasks = [
            TaskSpec("t1", func=lambda: {"val": 42}),
            TaskSpec(
                "t2",
                func=lambda _p, ctx: ctx.dependency_results["t1"]["val"] * 2,
                dependencies=["t1"],
            ),
        ]
        wf = create_workflow(
            tasks,
            workflow_id="wf_hmac_v2",
            storage_dir=self.storage_dir,
            clock=self.clock,
            hmac_key=secret_key,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        raw_bytes = wf.log_path.read_bytes()
        self.assertTrue(raw_bytes.startswith(b"@CHIMERA2|"))

        # Recovery with correct key succeeds
        rec_ok = recover_workflow(
            "wf_hmac_v2",
            tasks=tasks,
            storage_dir=self.storage_dir,
            hmac_key=secret_key,
        )
        self.assertEqual(rec_ok.status, WorkflowStatus.SUCCEEDED)

        # Recovery with wrong key raises LogAuthenticationError
        with self.assertRaises(LogAuthenticationError):
            recover_workflow(
                "wf_hmac_v2",
                tasks=tasks,
                storage_dir=self.storage_dir,
                hmac_key=b"wrong-key",
            )

        # Migrate a v1 log to v2
        wf_v1 = create_workflow(
            tasks,
            workflow_id="wf_migrate_v1_v2",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        run_workflow(wf_v1)
        self.assertTrue(wf_v1.log_path.read_bytes().startswith(b"@CHIMERA1|"))
        migrated_count = wf_v1._event_log.migrate_v1_to_v2(hmac_key=secret_key)
        self.assertGreaterEqual(migrated_count, 6)
        self.assertTrue(wf_v1.log_path.read_bytes().startswith(b"@CHIMERA2|"))
        rec_migrated = recover_workflow(
            "wf_migrate_v1_v2",
            tasks=tasks,
            storage_dir=self.storage_dir,
            hmac_key=secret_key,
        )
        self.assertEqual(rec_migrated.status, WorkflowStatus.SUCCEEDED)

    def test_metrics_sink_and_sensitive_payload_redaction_in_structured_logs(
        self,
    ) -> None:
        sink = InMemoryMetricsSink()
        wf = create_workflow(
            [
                TaskSpec(
                    "secret_task",
                    func=lambda: {"api_token": "tok_SUPER_SECRET_999"},
                )
            ],
            workflow_id="wf_redaction",
            storage_dir=self.storage_dir,
            clock=self.clock,
            metrics_sink=sink,
            log_sensitive_payloads=False,
        )
        summary = run_workflow(wf)
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)

        # Authoritative result is available via get_task_result
        self.assertEqual(
            get_task_result("wf_redaction", "secret_task").result,
            {"api_token": "tok_SUPER_SECRET_999"},
        )
        # Operational structured_logs must NOT contain the raw secret string
        serialized_op_logs = json.dumps(
            [entry.to_dict() for entry in summary.structured_logs]
        )
        self.assertNotIn("tok_SUPER_SECRET_999", serialized_op_logs)

        # MetricsSink recorded counters and durations
        self.assertEqual(sink.counters.get("chimera.task.succeeded"), 1)
        self.assertIn("chimera.task.queue_delay_seconds", sink.durations)
        self.assertIn("chimera.task.execution_duration_seconds", sink.durations)

    def test_read_only_inspect_workflow_cli_and_api(self) -> None:
        wf = create_workflow(
            [TaskSpec("t1", func=lambda: "ok")],
            workflow_id="wf_inspect_cli",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        run_workflow(wf)

        report = inspect_workflow_log(wf.log_path)
        self.assertTrue(report.is_healthy)
        self.assertEqual(report.workflow_status, "SUCCEEDED")
        self.assertEqual(report.counts_by_state["SUCCEEDED"], 1)

        # Run CLI script with --json
        cli_script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "inspect_workflow.py"
        )
        proc = subprocess.run(
            [sys.executable, str(cli_script), str(wf.log_path), "--json"],
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        parsed = json.loads(proc.stdout)
        self.assertTrue(parsed["is_healthy"])
        self.assertEqual(parsed["workflow_id"], "wf_inspect_cli")

        # Append a truncated tail and verify read-only inspection detects it WITHOUT truncating the file
        orig_bytes = wf.log_path.read_bytes()
        dirty_bytes = orig_bytes + b"@CHIMERA1|0000000006|000"
        wf.log_path.write_bytes(dirty_bytes)
        report_trunc = inspect_workflow_log(wf.log_path)
        self.assertTrue(report_trunc.is_healthy)
        self.assertTrue(report_trunc.truncated_tail_detected)
        # File on disk must remain untouched by read-only inspection
        self.assertEqual(wf.log_path.read_bytes(), dirty_bytes)

    def test_snapshot_immutability_prevents_internal_state_mutation(
        self,
    ) -> None:
        wf = create_workflow(
            [TaskSpec("t1", func=lambda: {"nested": [1, 2, 3]})],
            workflow_id="wf_immutability",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        summary1 = run_workflow(wf)
        summary1.tasks["t1"].result["nested"].append(999)
        summary1.structured_logs[0].details["injected"] = "mutated"

        summary2 = wf.get_status()
        self.assertEqual(summary2.tasks["t1"].result, {"nested": [1, 2, 3]})
        self.assertNotIn("injected", summary2.structured_logs[0].details)


if __name__ == "__main__":
    unittest.main()
