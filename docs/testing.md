# Testing & Adversarial Verification Guide

CHIMERA uses Python's built-in `unittest` framework with zero external test dependencies and **zero `time.sleep()` calls**.

## Deterministic Synchronization Primitives Used
- **`VirtualClock`**: Advances logical time deterministically for timeout expiration and exponential backoff (`RETRY_WAIT -> READY`) without sleeping.
- **`threading.Barrier` & `threading.Event`**: Force true concurrent execution of diamond DAG branches, simultaneous worker claim races, and simultaneous completion vs. cancellation races.
- **`EventLog` Fault Injection (`crash_hook` & `partial_write_bytes`)**: Injects `SimulatedProcessCrash` (`BaseException`) at `BEFORE_WRITE`, `DURING_PARTIAL_WRITE` (after writing $k$ bytes of a frame via `os.write` before `os.fsync`), `AFTER_WRITE_BEFORE_FSYNC`, and `AFTER_FSYNC`.

## Test Suite Organization
- `tests/test_models.py`: `TaskSpec` parameter validation, backoff math, `VirtualClock` monotonicity, and `TaskContext` cancellation helpers.
- `tests/test_scheduler.py`: Duplicate IDs, missing dependencies, deep cycles, `(priority, task_id)` ordering, diamond DAGs, and failure cascades.
- `tests/test_persistence.py`: Framed CRC32 encoding, partial-write truncation recovery, dirty-tail append protection, bit-flip CRC errors, and semantic state corruption (`CorruptedStateTransitionError`).
- `tests/test_recovery.py`: Non-re-execution of committed `SUCCEEDED` tasks, idempotent repeated recovery, and rejection of altered `TaskSpec` definitions on recovery.
- `tests/test_cancellation.py`: Pre-run cancellation, barrier-synchronized completion vs. cancellation races, and late cancellation after workflow completion.
- `tests/test_concurrency.py`: Simultaneous claim exclusion, cross-process `subprocess` lock contention (`ConcurrentWorkflowAccessError`), and deep-copy payload isolation.
- `tests/test_adversarial.py`: Complete 22-test benchmark suite covering Mandatory Scenarios A–O and Hidden Tests 1–7.
