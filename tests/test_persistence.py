"""
Tests for `EventLog` persistence, CRC32 framing, Windows `O_BINARY` flag
enforcement, interrupted partial writes, dirty-tail protection, truncated
final records, and corrupted interior records.
"""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
from typing import Any
from unittest.mock import patch
import unittest

from chimera import (
    CorruptedStateTransitionError,
    LogCorruptionError,
    SimulatedProcessCrash,
    TaskSpec,
    TaskState,
    VirtualClock,
    WorkflowStatus,
    create_workflow,
    get_task_result,
    recover_workflow,
    run_workflow,
)
import chimera.locking as locking_mod
import chimera.persistence as persistence_mod


class TestPersistenceAndLogIntegrity(unittest.TestCase):
    """Verify framed CRC32 log persistence, tail repair, and corruption detection."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp.name)
        self.clock = VirtualClock(auto_advance_on_wait=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_windows_o_binary_flag_enforced_on_all_os_open_calls(self) -> None:
        """
        Regression test for Windows Python 3.12 text-mode CRLF bug:
        Verifies that every `os.open()` call in `EventLog.append`,
        `EventLog.replay` (tail repair), and `WorkflowFileLock` includes
        `O_BINARY_FLAG` so Windows never translates `\\n` into `\\r\\n`.
        """
        fake_o_binary = 0x8000
        real_os_open = os.open
        recorded_flags: list[int] = []

        def checked_os_open(
            path: str, flags: int, mode: int = 0o777, **kwargs: Any
        ) -> int:
            # Ignore POSIX directory O_RDONLY open for _fsync_parent_dir
            if flags != os.O_RDONLY:
                recorded_flags.append(flags)
                self.assertTrue(
                    bool(flags & fake_o_binary),
                    f"os.open({path!r}) missing O_BINARY_FLAG in flags={flags:#x}",
                )
            clean_flags = flags & ~fake_o_binary if not hasattr(os, "O_BINARY") else flags
            return real_os_open(path, clean_flags, mode, **kwargs)

        with (
            patch.object(persistence_mod, "O_BINARY_FLAG", fake_o_binary),
            patch.object(locking_mod, "O_BINARY_FLAG", fake_o_binary),
            patch("os.open", side_effect=checked_os_open),
        ):
            wf = create_workflow(
                [TaskSpec("t1", func=lambda: "ok")],
                max_concurrency=1,
                workflow_id="wf_o_binary_check",
                storage_dir=self.storage_dir,
                clock=self.clock,
            )
            run_workflow(wf)
            # Truncate 10 bytes to also exercise os.open inside replay() tail repair
            raw = wf.log_path.read_bytes()
            self.assertNotIn(b"\r\n", raw)
            wf.log_path.write_bytes(raw[:-10])
            recovered = recover_workflow("wf_o_binary_check")
            self.assertEqual(recovered.status, WorkflowStatus.SUCCEEDED)
            self.assertTrue(recovered.truncated_tail_repaired)
            self.assertGreaterEqual(len(recorded_flags), 4)

    def test_partial_write_crash_on_seq_1_and_subsequent_records(self) -> None:
        """
        Verify recovery from a partial write on `seq=1` (`WORKFLOW_CREATED`)
        as well as mid-header and mid-payload partial writes on later records.
        """
        # 1. Partial write on seq=1 (WORKFLOW_CREATED at offset 0)
        crashed_seq1 = False

        def crash_seq1(stage: str, _rec: dict[str, Any], seq: int) -> None:
            nonlocal crashed_seq1
            if not crashed_seq1 and stage == "DURING_PARTIAL_WRITE" and seq == 1:
                crashed_seq1 = True
                raise SimulatedProcessCrash("Crash mid-write of seq=1")

        wf1 = create_workflow(
            [TaskSpec("only", func=lambda: 99)],
            max_concurrency=1,
            workflow_id="wf_partial_seq1",
            storage_dir=self.storage_dir,
            clock=self.clock,
            crash_hook=crash_seq1,
            partial_write_bytes=18,
        )
        with self.assertRaises(SimulatedProcessCrash):
            run_workflow(wf1)

        rec1 = recover_workflow("wf_partial_seq1")
        self.assertTrue(rec1.truncated_tail_repaired)
        self.assertEqual(rec1.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(get_task_result("wf_partial_seq1", "only").result, 99)

        # 2. Partial write on subsequent records + dirty-tail guard
        for partial_bytes in (6, 22, 50):
            with self.subTest(partial_bytes=partial_bytes):
                wf_id = f"wf_partial_{partial_bytes}"
                counts = {"t1": 0, "t2": 0}

                def crash_mid_write(
                    stage: str, rec: dict[str, Any], _seq: int
                ) -> None:
                    if (
                        stage == "DURING_PARTIAL_WRITE"
                        and rec.get("event_type") == "TASK_STATE_CHANGED"
                        and rec["data"].get("task_id") == "t2"
                        and rec["data"].get("to_state") == TaskState.READY.value
                    ):
                        raise SimulatedProcessCrash("Crash during partial write")

                wf = create_workflow(
                    [
                        TaskSpec(
                            "t1",
                            func=lambda: (
                                counts.__setitem__("t1", counts["t1"] + 1)
                                or "v1"
                            ),
                        ),
                        TaskSpec(
                            "t2",
                            func=lambda _p, ctx: (
                                counts.__setitem__("t2", counts["t2"] + 1)
                                or ctx.dependency_results["t1"] + "_v2"
                            ),
                            dependencies=["t1"],
                        ),
                    ],
                    max_concurrency=1,
                    workflow_id=wf_id,
                    storage_dir=self.storage_dir,
                    clock=self.clock,
                    crash_hook=crash_mid_write,
                    partial_write_bytes=partial_bytes,
                )

                with self.assertRaises(SimulatedProcessCrash):
                    run_workflow(wf)

                wf._event_log.set_crash_hook(None)
                with self.assertRaises(LogCorruptionError):
                    wf._event_log.append(
                        "WORKFLOW_CANCEL_REQUESTED", {"reason": "UNSAFE"}
                    )

                recovered = recover_workflow(wf_id)
                self.assertTrue(recovered.truncated_tail_repaired)
                self.assertEqual(recovered.status, WorkflowStatus.SUCCEEDED)
                self.assertEqual(counts["t1"], 1)
                self.assertEqual(counts["t2"], 1)
                self.assertEqual(get_task_result(wf_id, "t2").result, "v1_v2")

    def test_corrupted_interior_records_raise_log_corruption_error(self) -> None:
        wf = create_workflow(
            [
                TaskSpec("t1", func=lambda: "ok1"),
                TaskSpec("t2", func=lambda: "ok2", dependencies=["t1"]),
            ],
            max_concurrency=1,
            workflow_id="wf_corrupt_interior",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        run_workflow(wf)
        lines = wf.log_path.read_bytes().splitlines(keepends=True)

        # 1. Bit flip in record #2 payload must specifically report CRC32 mismatch at seq=2
        bad_crc = list(lines)
        b_arr = bytearray(bad_crc[1])
        b_arr[-4] ^= 0x01
        bad_crc[1] = bytes(b_arr)
        wf.log_path.write_bytes(b"".join(bad_crc))
        with self.assertRaises(LogCorruptionError) as ctx_crc:
            recover_workflow("wf_corrupt_interior")
        self.assertIn("CRC32 checksum mismatch at seq=2", str(ctx_crc.exception))

        # 2. Corrupted header field in record #2 (non-hex character in CRC field)
        bad_hdr = list(lines)
        h_arr = bytearray(bad_hdr[1])
        h_arr[32] = ord("Z")
        bad_hdr[1] = bytes(h_arr)
        wf.log_path.write_bytes(b"".join(bad_hdr))
        with self.assertRaises(LogCorruptionError) as ctx_hdr:
            recover_workflow("wf_corrupt_interior")
        self.assertIn("Corrupted numeric/hex fields in frame header", str(ctx_hdr.exception))

    def test_semantic_corruption_with_valid_crc_raises_corrupted_transition_error(
        self,
    ) -> None:
        wf = create_workflow(
            [
                TaskSpec("t1", func=lambda: 1),
                TaskSpec("t2", func=lambda: 2, dependencies=["t1"]),
            ],
            max_concurrency=1,
            workflow_id="wf_semantic_corrupt",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf._ensure_initialized_in_log()
        wf._event_log.append(
            "TASK_STATE_CHANGED",
            {
                "task_id": "t2",
                "from_state": "PENDING",
                "to_state": "SUCCEEDED",
                "reason": "FORGED",
                "attempt": 1,
                "claim_epoch": 1,
                "recovery_count": 0,
                "result": 2,
            },
        )
        with self.assertRaises(CorruptedStateTransitionError) as ctx:
            recover_workflow("wf_semantic_corrupt")
        self.assertIn("Illegal state transition for task 't2'", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
