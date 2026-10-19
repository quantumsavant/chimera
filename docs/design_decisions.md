# Key Design Decisions

## 1. Framed CRC32 ASCII Event Log Over SQLite or Pickle
- **Why not `pickle`?** `pickle` allows arbitrary code execution on untrusted or corrupted files and makes logs opaque to human inspection.
- **Why a custom framed format over raw JSON Lines?** In raw newline-delimited JSON, a bit-flip or premature newline inside an interior record can look similar to a truncated final line. CHIMERA prefixes every line with a fixed 39-byte ASCII header:
  `@CHIMERA1|<seq:010d>|<payload_len:08d>|<crc32_hex:08x>|<compact_json>\n`
  This allows `EventLog.replay()` to distinguish:
  - **Truncated final record at EOF**: Cleanly rolled back via `os.ftruncate` + `os.fsync`.
  - **Interior or byte corruption**: Caught by header delimiters, monotonic `seq`, byte length, raw newline prohibition inside `ensure_ascii=True` JSON, CRC32 verification, and semantic state-transition validation (`CorruptedStateTransitionError`).

## 2. Monotonic Fencing Tokens (`ClaimToken`) for Timeouts and Cancellation
Python's `threading.Thread` does not provide a safe OS mechanism to forcibly kill a running thread mid-execution. Instead of pretending a timed-out thread has stopped:
1. CHIMERA signals `ctx._cancel_event` so cooperative tasks can exit early via `ctx.is_cancelled()` or `ctx.raise_if_cancelled()`.
2. The engine bumps `_global_claim_epoch`, transitions the task to `RETRY_WAIT` or `FAILED` (`TaskTimeoutError`), and spawns a replacement worker thread so wedged threads cannot starve `max_concurrency`.
3. When the stale background thread eventually returns, its older `ClaimToken.claim_epoch` is rejected by `_complete_task_attempt_locked`, logged as `STALE_COMPLETION_IGNORED`, and the thread exits immediately.

## 3. Dual-Layer File Locking (`WorkflowFileLock`)
POSIX `fcntl.flock` locks are associated with open file descriptions (working across independent processes), whereas in-process sharing of an engine or accidental dual instantiation within one process should also fail deterministically. `WorkflowFileLock` combines an in-process canonical-path lock table (`_ACTIVE_IN_PROCESS_LOCKS`) with an OS-level non-blocking lock (`fcntl.flock` on POSIX, `msvcrt.locking` on Windows).
