"""
Durable framed append-only event log (`EventLog`) with CRC32 checksums,
monotonic sequence verification, tail-truncation repair, and deterministic
fault injection.

On-disk frame format (one binary LF-terminated ASCII record per frame):
    `@CHIMERA1|<seq:010d>|<payload_len:08d>|<crc32_hex:08x>|<compact_json>\n`

Cross-platform binary I/O note:
    All low-level `os.open()` calls explicitly include `O_BINARY_FLAG`
    (`getattr(os, "O_BINARY", 0)`). On Windows (`nt`), calling `os.open()`
    without `os.O_BINARY` opens the file descriptor in C runtime text mode
    (`_O_TEXT`), which translates every `0x0A` (`\n`) written by `os.write()`
    into `0x0D 0x0A` (`\r\n`), shifting frame byte offsets and placing `\r` at
    `payload_end`. Including `O_BINARY_FLAG` guarantees identical byte-for-byte
    framing across Linux, macOS, and Windows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import threading
from typing import Any, Callable
import zlib

from chimera.clock import Clock, SystemClock
from chimera.exceptions import ConcurrentWorkflowAccessError, LogCorruptionError
from chimera.locking import WorkflowFileLock

O_BINARY_FLAG: int = getattr(os, "O_BINARY", 0)

FRAME_MAGIC = b"@CHIMERA1|"
FRAME_HEADER_LEN = 39  # "@CHIMERA1|" (10) + seq (10) + "|" (1) + len (8) + "|" (1) + crc (8) + "|" (1)
JSON_PAYLOAD_PREFIX = b'{"data":'
_ASCII_DIGITS = frozenset(b"0123456789")
_ASCII_HEX = frozenset(b"0123456789abcdef")


@dataclass
class ReplayResult:
    """Result returned by `EventLog.replay()`."""

    records: list[dict[str, Any]]
    truncated_tail_repaired: bool = False
    truncated_offset: int | None = None
    warnings: list[str] = field(default_factory=list)


class EventLog:
    """
    Append-only framed event log with CRC32 verification and tail repair.
    """

    def __init__(
        self,
        log_path: str | Path,
        workflow_id: str,
        clock: Clock | None = None,
        crash_hook: Callable[[str, dict[str, Any], int], None] | None = None,
        file_lock: WorkflowFileLock | None = None,
        partial_write_bytes: int | None = None,
    ) -> None:
        self.log_path = Path(log_path)
        self.workflow_id = workflow_id
        self._clock = clock or SystemClock()
        self._lock = threading.RLock()
        self._next_seq = 1
        self._crash_hook = crash_hook
        self.partial_write_bytes: int | None = partial_write_bytes
        self._unrepaired_partial_write: bool = False
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._file_lock = file_lock or WorkflowFileLock(
            self.log_path.with_suffix(".lock"), workflow_id
        )

    def set_crash_hook(
        self,
        hook: Callable[[str, dict[str, Any], int], None] | None,
        *,
        partial_write_bytes: int | None = None,
    ) -> None:
        with self._lock:
            self._crash_hook = hook
            self.partial_write_bytes = partial_write_bytes

    def _fsync_parent_dir(self) -> None:
        """Flush parent directory entry on POSIX filesystems when creating or truncating log file."""
        if os.name != "posix":
            return
        try:
            dir_fd = os.open(str(self.log_path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

    @staticmethod
    def _encode_frame(seq: int, record: dict[str, Any]) -> bytes:
        json_bytes = json.dumps(
            record, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
        crc = zlib.crc32(json_bytes) & 0xFFFFFFFF
        header = f"@CHIMERA1|{seq:010d}|{len(json_bytes):08d}|{crc:08x}|".encode(
            "ascii"
        )
        if len(header) != FRAME_HEADER_LEN:
            raise LogCorruptionError(
                f"Internal framing error: header length {len(header)} != {FRAME_HEADER_LEN}"
            )
        return header + json_bytes + b"\n"

    @staticmethod
    def _is_valid_partial_header(tail_slice: bytes, expected_seq: int) -> bool:
        """
        Validate that `tail_slice` (with `0 < len(tail_slice) < 39`) is a
        genuine prefix of a valid 39-byte frame header for `expected_seq`.
        """
        if b"\n" in tail_slice or b"\r" in tail_slice:
            return False
        expected_prefix = f"@CHIMERA1|{expected_seq:010d}|".encode("ascii")
        n = len(tail_slice)
        prefix_len = min(len(expected_prefix), n)
        if tail_slice[:prefix_len] != expected_prefix[:prefix_len]:
            return False
        # Positions 21..28 (inclusive) must be ASCII digits (payload_len)
        for idx in range(21, min(29, n)):
            if tail_slice[idx] not in _ASCII_DIGITS:
                return False
        # Position 29 must be '|'
        if n > 29 and tail_slice[29:30] != b"|":
            return False
        # Positions 30..37 (inclusive) must be lowercase hex digits (crc32)
        for idx in range(30, min(38, n)):
            if tail_slice[idx] not in _ASCII_HEX:
                return False
        # Position 38 must be '|'
        if n > 38 and tail_slice[38:39] != b"|":
            return False
        return True

    def append(self, event_type: str, data: dict[str, Any]) -> dict[str, Any]:
        """
        Atomically append and fsync a framed event record while holding the
        exclusive workflow file lock.
        """
        with self._file_lock.acquire(), self._lock:
            if self._unrepaired_partial_write:
                raise LogCorruptionError(
                    f"Cannot append to '{self.log_path}' after an interrupted partial write "
                    "without running recovery/replay first."
                )

            seq = self._next_seq
            if (
                seq == 1
                and self.log_path.exists()
                and self.log_path.stat().st_size > 0
            ):
                raise ConcurrentWorkflowAccessError(
                    f"Cannot initialize seq=1 on existing non-empty log '{self.log_path}'; "
                    "use recover_workflow() or a unique workflow_id."
                )

            record: dict[str, Any] = {
                "seq": seq,
                "timestamp": self._clock.now(),
                "workflow_id": self.workflow_id,
                "event_type": event_type,
                "data": data,
            }
            frame = self._encode_frame(seq, record)

            if self._crash_hook is not None:
                self._crash_hook("BEFORE_WRITE", record, seq)

            is_new_file = not self.log_path.exists()
            fd = os.open(
                str(self.log_path),
                os.O_CREAT | os.O_WRONLY | os.O_APPEND | O_BINARY_FLAG,
                0o644,
            )
            try:
                cut_point = self.partial_write_bytes
                if cut_point is None and self._crash_hook is not None:
                    cut_point = max(1, len(frame) // 2)

                if (
                    self._crash_hook is not None
                    and cut_point is not None
                    and 0 < cut_point < len(frame)
                ):
                    first_part = frame[:cut_point]
                    second_part = frame[cut_point:]
                    self._write_all(fd, first_part)
                    self._unrepaired_partial_write = True
                    self._crash_hook("DURING_PARTIAL_WRITE", record, seq)
                    self._write_all(fd, second_part)
                    self._unrepaired_partial_write = False
                else:
                    self._write_all(fd, frame)

                if self._crash_hook is not None:
                    self._crash_hook("AFTER_WRITE_BEFORE_FSYNC", record, seq)

                os.fsync(fd)
            finally:
                os.close(fd)

            if is_new_file or seq == 1:
                self._fsync_parent_dir()

            self._next_seq += 1

            if self._crash_hook is not None:
                self._crash_hook("AFTER_FSYNC", record, seq)

            return record

    @staticmethod
    def _write_all(fd: int, buf: bytes) -> None:
        total_written = 0
        while total_written < len(buf):
            written = os.write(fd, buf[total_written:])
            if written <= 0:
                raise OSError("os.write returned 0 bytes during EventLog.append")
            total_written += written

    def replay(self, repair_truncated_tail: bool = True) -> ReplayResult:
        """
        Replay all valid records from the event log while holding the exclusive
        workflow file lock.
        """
        with self._file_lock.acquire(), self._lock:
            if not self.log_path.exists():
                self._next_seq = 1
                self._unrepaired_partial_write = False
                return ReplayResult(records=[])

            raw = self.log_path.read_bytes()
            total_len = len(raw)
            pos = 0
            expected_seq = 1
            records: list[dict[str, Any]] = []
            truncated_tail_repaired = False
            truncated_offset: int | None = None
            warnings: list[str] = []

            while pos < total_len:
                remaining = total_len - pos

                # Case 1: Fewer than 39 bytes remaining at EOF
                if remaining < FRAME_HEADER_LEN:
                    tail_slice = raw[pos:total_len]
                    if not self._is_valid_partial_header(
                        tail_slice, expected_seq
                    ):
                        raise LogCorruptionError(
                            f"Corrupted record header at byte offset {pos} (remaining={remaining} bytes)."
                        )
                    truncated_offset = pos
                    warnings.append(
                        f"FINAL_RECORD_TRUNCATED: Incomplete {remaining}-byte header at EOF offset {pos}."
                    )
                    break

                # Case 2: At least 39 bytes available -> parse fixed header
                header = raw[pos : pos + FRAME_HEADER_LEN]
                if (
                    header[:10] != FRAME_MAGIC
                    or header[20:21] != b"|"
                    or header[29:30] != b"|"
                    or header[38:39] != b"|"
                ):
                    raise LogCorruptionError(
                        f"Malformed frame header delimiters or magic at byte offset {pos}."
                    )

                try:
                    seq_str = header[10:20].decode("ascii")
                    len_str = header[21:29].decode("ascii")
                    crc_str = header[30:38].decode("ascii")
                    if not (seq_str.isdigit() and len_str.isdigit()):
                        raise ValueError("Non-digit seq or length field")
                    seq = int(seq_str, 10)
                    payload_len = int(len_str, 10)
                    expected_crc = int(crc_str, 16)
                except (ValueError, UnicodeDecodeError) as exc:
                    raise LogCorruptionError(
                        f"Corrupted numeric/hex fields in frame header at byte offset {pos}: {exc}"
                    ) from exc

                if seq != expected_seq:
                    raise LogCorruptionError(
                        f"Log sequence gap or out-of-order record at offset {pos}: "
                        f"expected seq {expected_seq}, found {seq}."
                    )

                frame_len = FRAME_HEADER_LEN + payload_len + 1
                if remaining < frame_len:
                    partial_payload = raw[pos + FRAME_HEADER_LEN : total_len]
                    prefix_len = min(
                        len(JSON_PAYLOAD_PREFIX), len(partial_payload)
                    )
                    if (
                        FRAME_MAGIC in partial_payload
                        or b"\n" in partial_payload
                        or b"\r" in partial_payload
                        or partial_payload[:prefix_len]
                        != JSON_PAYLOAD_PREFIX[:prefix_len]
                        or any(b < 0x20 or b > 0x7E for b in partial_payload)
                    ):
                        raise LogCorruptionError(
                            f"Mid-file corruption or invalid partial payload bytes at seq={seq}, offset={pos}."
                        )
                    # If all payload_len bytes are present and only the 1-byte '\n' is
                    # missing, verify the CRC32 matches before treating as tail truncation.
                    if len(partial_payload) == payload_len:
                        partial_crc = zlib.crc32(partial_payload) & 0xFFFFFFFF
                        if partial_crc != expected_crc:
                            raise LogCorruptionError(
                                f"CRC32 checksum mismatch on un-terminated final frame at seq={seq}, offset={pos}: "
                                f"expected {expected_crc:08x}, computed {partial_crc:08x}."
                            )
                    truncated_offset = pos
                    warnings.append(
                        f"FINAL_RECORD_TRUNCATED: Record seq={seq} at offset {pos} truncated "
                        f"({remaining} of {frame_len} bytes present)."
                    )
                    break

                # Case 3: Full frame bytes are present -> validate trailer, CRC32, and JSON
                payload_start = pos + FRAME_HEADER_LEN
                payload_end = payload_start + payload_len
                payload_bytes = raw[payload_start:payload_end]
                trailer = raw[payload_end : payload_end + 1]

                if trailer != b"\n":
                    raise LogCorruptionError(
                        f"Missing frame newline terminator at seq={seq}, offset={pos}."
                    )

                actual_crc = zlib.crc32(payload_bytes) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    raise LogCorruptionError(
                        f"CRC32 checksum mismatch at seq={seq}, offset={pos}: "
                        f"expected {expected_crc:08x}, computed {actual_crc:08x}."
                    )

                try:
                    record = json.loads(payload_bytes.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise LogCorruptionError(
                        f"Invalid JSON payload at seq={seq}, offset={pos}: {exc}"
                    ) from exc

                if (
                    not isinstance(record, dict)
                    or record.get("seq") != seq
                    or record.get("workflow_id") != self.workflow_id
                    or not isinstance(record.get("data"), dict)
                ):
                    raise LogCorruptionError(
                        f"Record metadata mismatch at seq={seq}, offset={pos}."
                    )

                records.append(record)
                pos += frame_len
                expected_seq += 1

            if truncated_offset is not None and repair_truncated_tail:
                fd = os.open(str(self.log_path), os.O_RDWR | O_BINARY_FLAG)
                try:
                    os.ftruncate(fd, truncated_offset)
                    os.fsync(fd)
                finally:
                    os.close(fd)
                self._fsync_parent_dir()
                truncated_tail_repaired = True

            self._unrepaired_partial_write = False
            self._next_seq = expected_seq
            return ReplayResult(
                records=records,
                truncated_tail_repaired=truncated_tail_repaired,
                truncated_offset=truncated_offset,
                warnings=warnings,
            )
