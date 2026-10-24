"""
Durable framed append-only event log (`EventLog`) with CRC32 checksums,
optional HMAC-SHA256 authentication (`@CHIMERA2|`), monotonic sequence
verification, defensive frame size limits, strict schema validation,
tail-truncation repair, and deterministic fault injection.

Supported On-Disk Frame Formats (one binary LF-terminated ASCII record per frame):
1. Format v1 (`@CHIMERA1|`, 39-byte header, default when `hmac_key=None`):
   `@CHIMERA1|<seq:010d>|<payload_len:08d>|<crc32_hex:08x>|<compact_json>\n`
2. Format v2 (`@CHIMERA2|`, 104-byte header, used when `hmac_key` is provided
   or `format_version=2`):
   `@CHIMERA2|<seq:010d>|<payload_len:08d>|<crc32_hex:08x>|<hmac_sha256_hex:64x>|<compact_json>\n`

Note on CRC32 vs. HMAC-SHA256:
- CRC32 detects accidental storage corruption (bit rot, partial sector writes).
- Optional HMAC-SHA256 (`hmac_key`) detects unauthenticated tampering with
  individual frames, and chains the previous frame's MAC (`prev_mac`) so
  interior record deletion or splicing is detected. Neither CRC32 nor a local
  HMAC prevents an attacker with full disk write access from truncating or
  rolling back the entire file to an earlier valid prefix unless combined with
  external monotonic state.
"""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, field
import hashlib
import hmac
import math
import os
from pathlib import Path
import threading
from typing import Any, Callable
import zlib

from chimera.clock import Clock, SystemClock
from chimera.exceptions import (
    ConcurrentWorkflowAccessError,
    LogAuthenticationError,
    LogCorruptionError,
    OversizedFrameError,
    OversizedPayloadError,
)
from chimera.locking import (
    DEFAULT_FILE_MODE,
    WorkflowFileLock,
    ensure_private_directory,
)
from chimera.serialization import (
    DEFAULT_MAX_FRAME_BYTES,
    DEFAULT_MAX_PAYLOAD_BYTES,
    canonical_json_bytes,
    canonical_json_loads,
)
from chimera.validation import validate_workflow_id

O_BINARY_FLAG: int = getattr(os, "O_BINARY", 0)

FRAME_MAGIC = b"@CHIMERA1|"
FRAME_V2_MAGIC = b"@CHIMERA2|"
FRAME_HEADER_LEN = 39  # "@CHIMERA1|" (10) + seq (10) + "|" (1) + len (8) + "|" (1) + crc (8) + "|" (1)
FRAME_V2_HEADER_LEN = 104  # "@CHIMERA2|" (10) + seq (10) + "|" (1) + len (8) + "|" (1) + crc (8) + "|" (1) + hmac (64) + "|" (1)
JSON_PAYLOAD_PREFIX = b'{"data":'
_ASCII_DIGITS = frozenset(b"0123456789")
_ASCII_HEX = frozenset(b"0123456789abcdef")

KNOWN_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "WORKFLOW_CREATED",
        "TASK_STATE_CHANGED",
        "STALE_COMPLETION_IGNORED",
        "WORKFLOW_CANCEL_REQUESTED",
        "WORKFLOW_COMPLETED",
        "DEFINITION_MIGRATED",
    }
)


@dataclass
class ReplayResult:
    """Result returned by `EventLog.replay()`."""

    records: list[dict[str, Any]]
    truncated_tail_repaired: bool = False
    truncated_offset: int | None = None
    warnings: list[str] = field(default_factory=list)
    format_version: int = 1


class EventLog:
    """
    Append-only framed event log with CRC32 verification, optional HMAC-SHA256
    authentication, frame size bounds, strict schema checks, and tail repair.
    """

    def __init__(
        self,
        log_path: str | Path,
        workflow_id: str,
        clock: Clock | None = None,
        crash_hook: Callable[[str, dict[str, Any], int], None] | None = None,
        file_lock: WorkflowFileLock | None = None,
        partial_write_bytes: int | None = None,
        *,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        file_mode: int = DEFAULT_FILE_MODE,
        hmac_key: bytes | None = None,
        format_version: int | None = None,
    ) -> None:
        validate_workflow_id(workflow_id)
        if max_payload_bytes <= 0 or max_frame_bytes <= 0:
            raise ValueError("max_payload_bytes and max_frame_bytes must be > 0.")
        self.log_path = Path(log_path)
        self.workflow_id = workflow_id
        self._clock = clock or SystemClock()
        self._lock = threading.RLock()
        self._next_seq = 1
        self._crash_hook = crash_hook
        self.partial_write_bytes: int | None = partial_write_bytes
        self.max_payload_bytes = max_payload_bytes
        self.max_frame_bytes = max_frame_bytes
        self.file_mode = file_mode
        self._hmac_key: bytes | None = bytes(hmac_key) if hmac_key is not None else None
        self.format_version: int = (
            format_version
            if format_version is not None
            else (2 if self._hmac_key is not None else 1)
        )
        if self.format_version not in (1, 2):
            raise ValueError(f"Unsupported EventLog format_version: {self.format_version}")
        self._last_mac_hex: str = "0" * 64
        self._unrepaired_partial_write: bool = False
        ensure_private_directory(self.log_path.parent)
        self._file_lock = file_lock or WorkflowFileLock(
            self.log_path.with_suffix(".lock"),
            workflow_id,
            file_mode=self.file_mode,
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
    def _compute_hmac_hex(
        key: bytes | None,
        seq: int,
        crc: int,
        prev_mac_hex: str,
        json_bytes: bytes,
    ) -> str:
        effective_key = key if key is not None else b"CHIMERA_UNKEYED_V2"
        mac = hmac.new(effective_key, digestmod=hashlib.sha256)
        mac.update(f"{seq:010d}|{crc:08x}|{prev_mac_hex}|".encode("ascii"))
        mac.update(json_bytes)
        return mac.hexdigest()

    @classmethod
    def _encode_frame(
        cls,
        seq: int,
        record: dict[str, Any],
        *,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        format_version: int = 1,
        hmac_key: bytes | None = None,
        prev_mac_hex: str = "0" * 64,
    ) -> tuple[bytes, str]:
        json_bytes = canonical_json_bytes(record, max_bytes=max_payload_bytes)
        if len(json_bytes) > max_frame_bytes:
            raise OversizedFrameError(
                f"Encoded record payload ({len(json_bytes)} bytes) exceeds max_frame_bytes ({max_frame_bytes})."
            )
        crc = zlib.crc32(json_bytes) & 0xFFFFFFFF
        if format_version == 1:
            header = f"@CHIMERA1|{seq:010d}|{len(json_bytes):08d}|{crc:08x}|".encode(
                "ascii"
            )
            if len(header) != FRAME_HEADER_LEN:
                raise LogCorruptionError(
                    f"Internal framing error: header length {len(header)} != {FRAME_HEADER_LEN}"
                )
            return header + json_bytes + b"\n", prev_mac_hex

        mac_hex = cls._compute_hmac_hex(
            hmac_key, seq, crc, prev_mac_hex, json_bytes
        )
        header = (
            f"@CHIMERA2|{seq:010d}|{len(json_bytes):08d}|{crc:08x}|{mac_hex}|".encode(
                "ascii"
            )
        )
        if len(header) != FRAME_V2_HEADER_LEN:
            raise LogCorruptionError(
                f"Internal framing error: v2 header length {len(header)} != {FRAME_V2_HEADER_LEN}"
            )
        return header + json_bytes + b"\n", mac_hex

    @staticmethod
    def _is_valid_partial_header(
        tail_slice: bytes, expected_seq: int, format_version: int = 1
    ) -> bool:
        """
        Validate that `tail_slice` (with `0 < len(tail_slice) < header_len`) is a
        genuine prefix of a valid frame header for `expected_seq`.
        """
        if b"\n" in tail_slice or b"\r" in tail_slice:
            return False
        magic_str = "@CHIMERA2" if format_version == 2 else "@CHIMERA1"
        expected_prefix = f"{magic_str}|{expected_seq:010d}|".encode("ascii")
        n = len(tail_slice)
        prefix_len = min(len(expected_prefix), n)
        if tail_slice[:prefix_len] != expected_prefix[:prefix_len]:
            return False
        for idx in range(21, min(29, n)):
            if tail_slice[idx] not in _ASCII_DIGITS:
                return False
        if n > 29 and tail_slice[29:30] != b"|":
            return False
        for idx in range(30, min(38, n)):
            if tail_slice[idx] not in _ASCII_HEX:
                return False
        if n > 38 and tail_slice[38:39] != b"|":
            return False
        if format_version == 2:
            for idx in range(39, min(103, n)):
                if tail_slice[idx] not in _ASCII_HEX:
                    return False
            if n > 103 and tail_slice[103:104] != b"|":
                return False
        return True

    @staticmethod
    def _validate_record_schema(
        record: Any, expected_seq: int, expected_workflow_id: str, pos: int
    ) -> None:
        if not isinstance(record, dict):
            raise LogCorruptionError(
                f"Record at seq={expected_seq}, offset={pos} is not a JSON object."
            )
        if record.get("seq") != expected_seq or isinstance(record.get("seq"), bool):
            raise LogCorruptionError(
                f"Record seq mismatch at offset={pos}: expected {expected_seq}, got {record.get('seq')!r}."
            )
        ts = record.get("timestamp")
        if (
            not isinstance(ts, (int, float))
            or isinstance(ts, bool)
            or math.isnan(float(ts))
            or math.isinf(float(ts))
        ):
            raise LogCorruptionError(
                f"Invalid timestamp {ts!r} at seq={expected_seq}, offset={pos}."
            )
        if record.get("workflow_id") != expected_workflow_id:
            raise LogCorruptionError(
                f"Record workflow_id mismatch at seq={expected_seq}, offset={pos}: "
                f"expected {expected_workflow_id!r}, got {record.get('workflow_id')!r}."
            )
        etype = record.get("event_type")
        if not isinstance(etype, str) or etype not in KNOWN_EVENT_TYPES:
            raise LogCorruptionError(
                f"Unknown or invalid event_type {etype!r} at seq={expected_seq}, offset={pos}."
            )
        data = record.get("data")
        if not isinstance(data, dict):
            raise LogCorruptionError(
                f"Record 'data' field must be a dict at seq={expected_seq}, offset={pos}."
            )

    def append(self, event_type: str, data: dict[str, Any]) -> dict[str, Any]:
        """
        Atomically append and fsync a framed event record while holding the
        exclusive workflow file lock.
        """
        if event_type not in KNOWN_EVENT_TYPES:
            raise ValueError(f"Unsupported event_type: {event_type!r}")

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
            frame, new_mac = self._encode_frame(
                seq,
                record,
                max_payload_bytes=self.max_payload_bytes,
                max_frame_bytes=self.max_frame_bytes,
                format_version=self.format_version,
                hmac_key=self._hmac_key,
                prev_mac_hex=self._last_mac_hex,
            )

            if self._crash_hook is not None:
                self._crash_hook("BEFORE_WRITE", record, seq)

            is_new_file = not self.log_path.exists()
            fd = os.open(
                str(self.log_path),
                os.O_CREAT | os.O_WRONLY | os.O_APPEND | O_BINARY_FLAG,
                self.file_mode,
            )
            try:
                if is_new_file and os.name == "posix":
                    try:
                        os.fchmod(fd, self.file_mode)
                    except OSError:
                        pass

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
            self._last_mac_hex = new_mac

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

    def replay(
        self,
        repair_truncated_tail: bool = True,
        *,
        read_only: bool = False,
    ) -> ReplayResult:
        """
        Replay all valid records from the event log.
        When `read_only=True`, does not acquire the exclusive lock or truncate
        the file on disk (used by read-only diagnostic tooling).
        """
        lock_ctx = nullcontext() if read_only else self._file_lock.acquire()
        with lock_ctx, self._lock:
            if not self.log_path.exists():
                if not read_only:
                    self._next_seq = 1
                    self._last_mac_hex = "0" * 64
                    self._unrepaired_partial_write = False
                return ReplayResult(
                    records=[], format_version=self.format_version
                )

            raw = self.log_path.read_bytes()
            total_len = len(raw)
            pos = 0
            expected_seq = 1
            running_mac_hex = "0" * 64
            detected_version = self.format_version
            records: list[dict[str, Any]] = []
            truncated_tail_repaired = False
            truncated_offset: int | None = None
            warnings: list[str] = []

            while pos < total_len:
                remaining = total_len - pos

                # Determine format version of this frame from magic prefix if available
                frame_ver = detected_version
                if remaining >= 10:
                    magic_Candidate = raw[pos : pos + 10]
                    if magic_Candidate == FRAME_MAGIC:
                        frame_ver = 1
                    elif magic_Candidate == FRAME_V2_MAGIC:
                        frame_ver = 2
                    else:
                        raise LogCorruptionError(
                            f"Malformed frame magic {magic_Candidate!r} at byte offset {pos}."
                        )
                    if expected_seq == 1:
                        detected_version = frame_ver
                    elif frame_ver != detected_version:
                        raise LogCorruptionError(
                            f"Mixed frame format versions in log at offset {pos} (expected v{detected_version}, found v{frame_ver})."
                        )
                else:
                    # Fewer than 10 bytes left at EOF
                    tail_slice = raw[pos:total_len]
                    if not (
                        self._is_valid_partial_header(tail_slice, expected_seq, 1)
                        or self._is_valid_partial_header(tail_slice, expected_seq, 2)
                    ):
                        raise LogCorruptionError(
                            f"Corrupted record header at byte offset {pos} (remaining={remaining} bytes)."
                        )
                    truncated_offset = pos
                    warnings.append(
                        f"FINAL_RECORD_TRUNCATED: Incomplete {remaining}-byte header at EOF offset {pos}."
                    )
                    break

                hdr_len = FRAME_V2_HEADER_LEN if frame_ver == 2 else FRAME_HEADER_LEN

                # Case 1: Fewer than hdr_len bytes remaining at EOF
                if remaining < hdr_len:
                    tail_slice = raw[pos:total_len]
                    if not self._is_valid_partial_header(
                        tail_slice, expected_seq, frame_ver
                    ):
                        raise LogCorruptionError(
                            f"Corrupted record header at byte offset {pos} (remaining={remaining} bytes)."
                        )
                    truncated_offset = pos
                    warnings.append(
                        f"FINAL_RECORD_TRUNCATED: Incomplete {remaining}-byte header at EOF offset {pos}."
                    )
                    break

                # Case 2: At least hdr_len bytes available -> parse fixed header
                header = raw[pos : pos + hdr_len]
                expected_magic = FRAME_V2_MAGIC if frame_ver == 2 else FRAME_MAGIC
                if (
                    header[:10] != expected_magic
                    or header[20:21] != b"|"
                    or header[29:30] != b"|"
                    or header[38:39] != b"|"
                    or (frame_ver == 2 and header[103:104] != b"|")
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
                    expected_mac_hex = ""
                    if frame_ver == 2:
                        expected_mac_hex = header[39:103].decode("ascii")
                        if any(ord(c) not in _ASCII_HEX for c in expected_mac_hex):
                            raise ValueError("Non-hex character in HMAC field")
                except (ValueError, UnicodeDecodeError) as exc:
                    raise LogCorruptionError(
                        f"Corrupted numeric/hex fields in frame header at byte offset {pos}: {exc}"
                    ) from exc

                if seq != expected_seq:
                    raise LogCorruptionError(
                        f"Log sequence gap or out-of-order record at offset {pos}: "
                        f"expected seq {expected_seq}, found {seq}."
                    )

                if payload_len > self.max_frame_bytes:
                    raise OversizedFrameError(
                        f"Frame payload_len ({payload_len} bytes) at seq={seq}, offset={pos} "
                        f"exceeds max_frame_bytes ({self.max_frame_bytes})."
                    )

                frame_len = hdr_len + payload_len + 1
                if remaining < frame_len:
                    partial_payload = raw[pos + hdr_len : total_len]
                    prefix_len = min(
                        len(JSON_PAYLOAD_PREFIX), len(partial_payload)
                    )
                    if (
                        FRAME_MAGIC in partial_payload
                        or FRAME_V2_MAGIC in partial_payload
                        or b"\n" in partial_payload
                        or b"\r" in partial_payload
                        or partial_payload[:prefix_len]
                        != JSON_PAYLOAD_PREFIX[:prefix_len]
                        or any(b < 0x20 or b > 0x7E for b in partial_payload)
                    ):
                        raise LogCorruptionError(
                            f"Mid-file corruption or invalid partial payload bytes at seq={seq}, offset={pos}."
                        )
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

                # Case 3: Full frame bytes are present -> validate trailer, CRC32, HMAC, and JSON
                payload_start = pos + hdr_len
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

                if frame_ver == 2:
                    computed_mac = self._compute_hmac_hex(
                        self._hmac_key,
                        seq,
                        actual_crc,
                        running_mac_hex,
                        payload_bytes,
                    )
                    if not hmac.compare_digest(computed_mac, expected_mac_hex):
                        raise LogAuthenticationError(
                            f"HMAC-SHA256 authentication failed at seq={seq}, offset={pos}."
                        )
                    running_mac_hex = computed_mac
                elif self._hmac_key is not None:
                    raise LogAuthenticationError(
                        f"Log '{self.log_path}' uses unauthenticated @CHIMERA1 framing at seq={seq}, "
                        "but an hmac_key was required."
                    )

                record = canonical_json_loads(
                    payload_bytes, max_bytes=self.max_frame_bytes
                )
                self._validate_record_schema(
                    record, expected_seq, self.workflow_id, pos
                )

                records.append(record)
                pos += frame_len
                expected_seq += 1

            if truncated_offset is not None and repair_truncated_tail and not read_only:
                fd = os.open(str(self.log_path), os.O_RDWR | O_BINARY_FLAG)
                try:
                    os.ftruncate(fd, truncated_offset)
                    os.fsync(fd)
                finally:
                    os.close(fd)
                self._fsync_parent_dir()
                truncated_tail_repaired = True

            if not read_only:
                self._unrepaired_partial_write = False
                self._next_seq = expected_seq
                self._last_mac_hex = running_mac_hex
                if records:
                    self.format_version = detected_version

            return ReplayResult(
                records=records,
                truncated_tail_repaired=truncated_tail_repaired,
                truncated_offset=truncated_offset,
                warnings=warnings,
                format_version=detected_version,
            )

    def migrate_v1_to_v2(self, *, hmac_key: bytes | None = None) -> int:
        """
        Atomically migrate an existing `@CHIMERA1|` event log file to `@CHIMERA2|`
        (with chained HMAC-SHA256 authentication) via write-temp + fsync + os.replace.
        Returns the number of migrated records.
        """
        with self._file_lock.acquire(), self._lock:
            replay_res = self.replay(repair_truncated_tail=True)
            effective_key = hmac_key if hmac_key is not None else self._hmac_key
            tmp_path = self.log_path.with_suffix(".v2.tmp")
            prev_mac = "0" * 64
            frames: list[bytes] = []
            for rec in replay_res.records:
                frame, prev_mac = self._encode_frame(
                    int(rec["seq"]),
                    rec,
                    max_payload_bytes=self.max_payload_bytes,
                    max_frame_bytes=self.max_frame_bytes,
                    format_version=2,
                    hmac_key=effective_key,
                    prev_mac_hex=prev_mac,
                )
                frames.append(frame)

            fd = os.open(
                str(tmp_path),
                os.O_CREAT | os.O_WRONLY | os.O_TRUNC | O_BINARY_FLAG,
                self.file_mode,
            )
            try:
                if os.name == "posix":
                    try:
                        os.fchmod(fd, self.file_mode)
                    except OSError:
                        pass
                self._write_all(fd, b"".join(frames))
                os.fsync(fd)
            finally:
                os.close(fd)

            os.replace(str(tmp_path), str(self.log_path))
            self._fsync_parent_dir()
            self._hmac_key = bytes(effective_key) if effective_key is not None else None
            self.format_version = 2
            self._last_mac_hex = prev_mac
            self._next_seq = len(replay_res.records) + 1
            return len(replay_res.records)
