# CHIMERA Phase 1 — Architecture, Correctness & Risk Assessment

## 1. Baseline Verification Summary

Before modifying any code, the existing repository (`v0.1.0`) was extracted and executed in a clean environment:

- **Command Executed**: `python3 scripts/run_tests.py`
- **Python Runtime**: `Python 3.11.2` (`CPython`)
- **Platform**: `Linux 4.19.0-gvisor x86_64` (`posix`)
- **Bytecode Compilation**: 22 Python files compiled cleanly (`py_compile`).
- **Baseline Test Count**: 43 tests across 7 test modules (`test_adversarial.py`, `test_cancellation.py`, `test_concurrency.py`, `test_models.py`, `test_persistence.py`, `test_recovery.py`, `test_scheduler.py`).
- **Baseline Outcome**: `43 passed, 0 failures, 0 errors` in `0.941s`.

---

## 2. Current Module Responsibilities & Dependency Relationships

### 2.1 Module Inventory (`v0.1.0`)

| Module | Responsibilities | Internal Dependencies |
| :--- | :--- | :--- |
| `src/chimera/exceptions.py` | Exception hierarchy rooted at `ChimeraError`, plus `SimulatedProcessCrash(BaseException)`. | None (leaf module) |
| `src/chimera/clock.py` | `Clock` protocol, monotonic wall-anchored `SystemClock`, controllable `VirtualClock`. | None (leaf module) |
| `src/chimera/models.py` | Enums (`TaskState`, `WorkflowStatus`), transition tables, `ClaimToken`, `TaskContext`, `TaskSpec`, `TaskRuntimeState`, `TaskResultView`, `StructuredLogEntry`, `WorkflowStatusSummary`. | `exceptions` |
| `src/chimera/locking.py` | `WorkflowFileLock` combining an in-process canonical-path table (`_ACTIVE_IN_PROCESS_LOCKS`) with OS advisory locks (`fcntl.flock` on POSIX, `msvcrt.locking` on Windows). | `exceptions` |
| `src/chimera/persistence.py` | Framed CRC32 append-only `EventLog` (`@CHIMERA1\|`), partial-header/payload tail truncation detection and `os.ftruncate` repair, parent-directory `fsync`. | `clock`, `exceptions`, `locking` |
| `src/chimera/validation.py` | `validate_dag()` performing duplicate ID checks, missing dependency checks, and iterative 3-color DFS cycle detection. | `exceptions`, `models` |
| `src/chimera/scheduler.py` | `DeterministicReadyScheduler` min-heap ordered by `(priority, task_id)`, eligible task promotion, BFS failure cascade, and `invoke_task_callable()`. | `models` |
| `src/chimera/recovery.py` | `RecoveryManager.verify_recovered_task_specs()` comparing persisted `WORKFLOW_CREATED` task metadata against caller-supplied `TaskSpec` objects. | `exceptions`, `models` |
| `src/chimera/engine.py` | `WorkflowEngine` state machine, worker thread orchestration, timeout/retry watchdog, cancellation, recovery, and module-level registry API. | `clock`, `exceptions`, `locking`, `models`, `persistence`, `recovery`, `scheduler`, `validation` |

### 2.2 Architectural Assessment of Module Boundaries
- **Strengths**: Acyclic internal module graph; deterministic clock injection (`Clock`); separation of framing (`persistence.py`) from DAG validation (`validation.py`) and ready-queue ordering (`scheduler.py`).
- **Gaps**:
  1. `WorkflowEngine.run()` directly spawns `threading.Thread` workers inline rather than delegating to an abstract `TaskExecutor` backend.
  2. Canonical payload/result serialization and schema validation are scattered across `models.py`, `engine.py`, and `persistence.py` with lossy `repr()` fallbacks.
  3. Observability and diagnostic inspection are tightly coupled to in-memory `WorkflowEngine` state rather than supporting standalone read-only inspection of on-disk logs.

---

## 3. State-Machine Invariants & Legal Transitions

### 3.1 Task Lifecycle State Machine
- **States**: `PENDING`, `READY`, `RUNNING`, `RETRY_WAIT`, `SUCCEEDED`, `FAILED`, `CANCELLED`.
- **Terminal States**: `SUCCEEDED`, `FAILED`, `CANCELLED` (zero outgoing transitions in both live and recovery modes).
- **Live Transitions (`LIVE_ALLOWED_TRANSITIONS`)**:
  - `PENDING -> {READY, CANCELLED}`
  - `READY -> {RUNNING, CANCELLED}`
  - `RUNNING -> {SUCCEEDED, FAILED, RETRY_WAIT, CANCELLED}`
  - `RETRY_WAIT -> {READY, CANCELLED}`
- **Recovery Reconciliation Transitions (`RECOVERY_ALLOWED_TRANSITIONS`)**:
  - Adds `RUNNING -> READY` exclusivamente when `is_recovery=True` and `reason == "RECOVERED_FROM_CRASH"`.

### 3.2 Core Invariants Verified
1. **Topological Readiness ($I_1$)**: A task enters `READY` or `RUNNING` only when all upstream dependencies are in `SUCCEEDED`. Enforced both during live promotion and during `_apply_event` replay.
2. **Monotonic Fencing ($I_2$)**: Every claim (`READY -> RUNNING`), timeout, cancellation, or crash recovery increments `_global_claim_epoch`.
3. **Stale Worker Rejection ($I_3$)**: `_complete_task_attempt_locked` verifies `not self._cancel_requested and rt.state == TaskState.RUNNING and rt.claim_epoch == token.claim_epoch and rt.attempt == token.attempt`. Mismatches append `STALE_COMPLETION_IGNORED` and return `False`.
4. **Post-Commit Immutability ($I_4$)**: Once `TASK_STATE_CHANGED(to_state="SUCCEEDED")` is `fsync`ed, `SUCCEEDED` has no outgoing edges and cannot be re-executed.

---

## 4. Detailed Audit Findings (Verified via Code Inspection & Reproduction)

### Finding F-01: Thread Backend Cannot Terminate Blocked Tasks & Can Accumulate Unbounded Zombie Threads
- **Severity**: **High**
- **Affected Files**: `src/chimera/engine.py` (lines 483–618)
- **Reproducible Scenario**: Configure a task with `timeout_seconds=0.05, max_retries=5, retry_on_timeout=True` whose callable blocks in `while True: pass` or a blocking OS/socket call. When `timeout_seconds` expires, `_enforce_timeouts_and_timers_locked()` increments `claim_epoch` and transitions the task from `RUNNING` to `RETRY_WAIT`, reducing `_active_running_count()` to `0`. On each retry, a new `threading.Thread` is spawned while all timed-out threads continue running indefinitely in the background.
- **Expected Behavior**: Tasks requiring hard execution deadlines must execute in a supervised child process (`ProcessTaskExecutor`) that enforces monotonic deadlines via graceful stop $\rightarrow$ `SIGTERM`/`terminate()` $\rightarrow$ `SIGKILL`/`kill()` and reaps the OS process before or alongside fencing the attempt.
- **Actual Behavior**: Only daemon `threading.Thread` workers exist; timed-out threads continue consuming CPU/memory until process exit.
- **Proposed Fix**: Introduce `src/chimera/executor.py` with `ExecutionBackend.THREAD` (`ThreadTaskExecutor`) and `ExecutionBackend.PROCESS` (`ProcessTaskExecutor` using `multiprocessing.get_context("spawn")`), bounded worker pools, and 3-stage process termination escalation.

### Finding F-02: Silent Lossy `repr()` Fallback in Durable Payloads and Task Results
- **Severity**: **High**
- **Affected Files**: `src/chimera/models.py` (lines 176–182), `src/chimera/engine.py` (lines 420–426)
- **Reproducible Scenario**: Pass a non-JSON type such as `input_payload={"tags": {"a", "b"}}` (a `set`) or return a custom object or `float("nan")` from a task `t1`. During live execution, `t2` receives the in-memory `set` via `copy.deepcopy`. If the process crashes after `t1` commits `SUCCEEDED` and recovers from disk, `t2` receives the string `"{'a', 'b'}"` (the `repr()` output) instead of a `set`. Furthermore, `json.dumps` without `allow_nan=False` writes non-standard `NaN`/`Infinity` literals into the log.
- **Expected Behavior**: Durable workflows must reject non-losslessly-serializable `input_payload` values before execution starts and reject unserializable or oversized task `result` values before committing `SUCCEEDED`, raising a typed `PayloadSerializationError` or `OversizedPayloadError`.
- **Actual Behavior**: `except (TypeError, OverflowError): return repr(val)` silently degrades arbitrary objects into strings in the durable log.
- **Proposed Fix**: Create `src/chimera/serialization.py` implementing strict, lossless canonical JSON validation (`allow_nan=False`, recursive type checking, configurable `max_payload_bytes`), and remove all `repr()` fallbacks from persisted task payloads and results.

### Finding F-03: Incomplete Workflow Definition Identity & Unverified Empty-Spec Recovery Bypass
- **Severity**: **High**
- **Affected Files**: `src/chimera/recovery.py` (lines 28–30), `src/chimera/models.py` (lines 175–196)
- **Reproducible Scenario**:
  1. In `RecoveryManager.verify_recovered_task_specs`, line 29 checks `if not task_specs: return`. If a caller instantiates `WorkflowEngine(tasks=[], workflow_id="existing_wf", storage_dir=...)` and calls `recover()`, `self._task_specs` is `{}` (falsy), so verification returns early without raising `WorkflowValidationError`, even though the persisted workflow had tasks!
  2. If a user changes a task function's internal business logic (or swaps `func` for a different function) without changing `input_payload` or retry parameters, `to_metadata_dict()` produces the exact same dictionary and recovery silently accepts the altered implementation.
- **Expected Behavior**:
  1. `verify_recovered_task_specs` must always compare `set(task_specs.keys()) == set(logged_tasks_meta.keys())` even when `task_specs` is empty (unless running in explicit read-only diagnostic mode).
  2. Workflows must persist an explicit `definition_version`, a canonical SHA-256 `definition_hash`, per-task `implementation_version` and callable reference metadata, and enforce an explicit `CompatibilityPolicy` (`STRICT`, `ALLOW_IMPLEMENTATION_UPGRADE`, `MIGRATE_LEGACY_V1`).
- **Actual Behavior**: `if not task_specs: return` skips verification when `tasks=[]`, and no cryptographic `definition_hash` or `implementation_version` is tracked.
- **Proposed Fix**: Remove the `if not task_specs: return` bypass, add canonical SHA-256 definition hashing, add `implementation_version` to `TaskSpec`, and enforce `CompatibilityPolicy` in `RecoveryManager`.

### Finding F-04: Unvalidated `workflow_id` Allows Path Traversal Outside `storage_dir`
- **Severity**: **High**
- **Affected Files**: `src/chimera/engine.py` (lines 59–69)
- **Reproducible Scenario**: Call `create_workflow(tasks, workflow_id="../escaped_wf", storage_dir="/tmp/safe_dir")`. `self.storage_dir / f"{self.workflow_id}.chimera.log"` resolves to `/tmp/escaped_wf.chimera.log`, writing `.log` and `.lock` files outside `storage_dir`.
- **Expected Behavior**: `workflow_id` must be validated at construction time against a strict pattern (`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`), rejecting path separators, `..`, NUL bytes, whitespace, and Windows reserved device names (`CON`, `NUL`, `AUX`, `PRN`, `COM1`-`COM9`, `LPT1`-`LPT9`) with `InvalidWorkflowIdError(WorkflowValidationError)`.
- **Actual Behavior**: Any string is accepted as `workflow_id`.
- **Proposed Fix**: Implement `validate_workflow_id()` in `src/chimera/validation.py` and invoke it in `WorkflowEngine.__init__`, `EventLog.__init__`, and `WorkflowFileLock.__init__`.

### Finding F-05: Permissive Default File Mode (`0o644`) and Unredacted Payloads in Operational Logs
- **Severity**: **Medium**
- **Affected Files**: `src/chimera/persistence.py` (line 164), `src/chimera/locking.py` (line 92), `src/chimera/engine.py` (lines 345–362)
- **Reproducible Scenario**: Run a workflow with sensitive data in `input_payload` or task `result`. The log file is created with mode `0o644` (readable by other local users if the parent directory is world-executable), and `summary.structured_logs` copies the raw `result` into `StructuredLogEntry.details` by default.
- **Expected Behavior**: Log and lock files should default to mode `0o600` (configurable via `file_mode`), storage directories should default to `0o700`, and operational `StructuredLogEntry.details` should omit raw `result` and `input_payload` values by default (`log_sensitive_payloads: bool = False`), recording only safe operational metadata (`has_result`, `result_type`, `failure_category`, `queue_delay_seconds`, `execution_duration_seconds`).
- **Actual Behavior**: Files use `0o644` and `StructuredLogEntry.details` includes raw `result` payloads.
- **Proposed Fix**: Default `file_mode` to `0o600` (with explicit `os.chmod` on POSIX creation), and sanitize `StructuredLogEntry.details` unless `log_sensitive_payloads=True`.

### Finding F-06: Unbounded Frame/Payload Sizes and Incomplete Event Schema Validation on Replay
- **Severity**: **Medium**
- **Affected Files**: `src/chimera/persistence.py` (lines 83–95, 224–314)
- **Reproducible Scenario**: A bug in a task produces a 50 MB return value, or a corrupted log header contains `payload_len = 99999999`. `EventLog` has no upper size guard before writing or reading frames, and `_apply_event` does not validate all field types in `record["data"]` (e.g., non-string `from_state` or non-numeric `timestamp` before float conversion).
- **Expected Behavior**: Enforce configurable `max_payload_bytes` (default 4 MiB) and `max_frame_bytes` (default 8 MiB) on write and replay, validate event schemas strictly before mutating state, and optionally support HMAC-SHA256 authenticated framing (`@CHIMERA2|`) when an `hmac_key` is configured.
- **Actual Behavior**: No frame size cap exists and only `@CHIMERA1|` unauthenticated CRC32 framing is supported.
- **Proposed Fix**: Add `max_payload_bytes` and `max_frame_bytes` checks, strict event schema validation in `persistence.py` / `engine.py`, and `@CHIMERA2|` optional HMAC-SHA256 support with a migration utility.

### Finding F-07: Windows 1-Byte Lock File Region & Fork PID Ownership Guard in `WorkflowFileLock`
- **Severity**: **Medium**
- **Affected Files**: `src/chimera/locking.py` (lines 38–126)
- **Reproducible Scenario**:
  1. If a user process calls `os.fork()` on POSIX while holding `WorkflowFileLock` (`_depth > 0`), the child process inherits `self._depth == 1` and `self._fd`, believing it already owns the lock without re-verifying PID ownership.
  2. On Windows, `msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)` locks byte `0..1`; ensuring the lock file is non-empty when acquired avoids edge cases with 0-byte files under third-party Windows file system filter drivers.
- **Expected Behavior**: `WorkflowFileLock` must track `_owner_pid = os.getpid()` and reset inherited lock state if accessed from a different PID, ensure every exception path closes `fd`, and maintain a 1-byte lock region cleanly.
- **Actual Behavior**: `_owner_pid` is not checked in `_acquire_internal`.
- **Proposed Fix**: Track `_owner_pid` in `WorkflowFileLock`, write a 1-byte marker on initial lock acquisition if empty, and harden exception cleanup.

### Finding F-08: Mutable Containers in Public Status Snapshots
- **Severity**: **Low**
- **Affected Files**: `src/chimera/models.py` (lines 230–265), `src/chimera/engine.py` (lines 688–723)
- **Reproducible Scenario**: Call `status = wf.get_status()`. `status.structured_logs` is a shallow copy of `self._structured_logs` (`list(self._structured_logs)`), where each `StructuredLogEntry.details` dictionary is the internal `details` dict. Mutating `status.structured_logs[0].details["x"] = 1` mutates the engine's internal log entry in place!
- **Expected Behavior**: Public snapshots (`WorkflowStatusSummary`, `TaskResultView`, `StructuredLogEntry`) must deep-copy `details`, `tasks`, and `counts_by_state` so caller mutations cannot corrupt internal engine state.
- **Actual Behavior**: `StructuredLogEntry.details` inside `status.structured_logs` shares a reference with `self._structured_logs`.
- **Proposed Fix**: Deep-copy `StructuredLogEntry` details when constructing snapshots and freeze/copy all returned collections.

### Finding F-09: `scripts/run_tests.py` Does Not Execute Runnable Examples Despite Docstring Claim
- **Severity**: **Low**
- **Affected Files**: `scripts/run_tests.py` (lines 1–50)
- **Reproducible Scenario**: Run `python3 scripts/run_tests.py`. The module docstring states `"Runs compilation checks, unit/integration/adversarial test discovery, and all runnable examples"`, but `main()` only compiles `.py` files and runs `unittest` discovery.
- **Expected Behavior**: `scripts/run_tests.py` must execute every example script in `examples/` and verify exit code `0` in addition to bytecode compilation and `unittest` execution.
- **Actual Behavior**: Examples are compiled but not executed by `scripts/run_tests.py`.
- **Proposed Fix**: Add `run_all_examples()` to `scripts/run_tests.py` and verify each example's exit code and output.

---

## 5. Prioritized Implementation Plan & Acceptance Criteria

| Priority | Phase | Scope & Key Deliverables | Acceptance Criteria |
| :---: | :--- | :--- | :--- |
| **P0** | **Phase 5 & 6** | Canonical lossless JSON serialization (`serialization.py`), removal of `repr()` fallbacks, `workflow_id` validation, restrictive `0o600`/`0o700` permissions, PID-aware `WorkflowFileLock`, `@CHIMERA2\|` optional HMAC-SHA256 framing, frame size limits. | Non-JSON payloads/results fail fast with `PayloadSerializationError`; invalid `workflow_id` raises `InvalidWorkflowIdError`; all 43 existing tests + new persistence/locking tests pass. |
| **P1** | **Phase 2** | `TaskExecutor` interface, `ThreadTaskExecutor`, and supervised `ProcessTaskExecutor` (`multiprocessing.get_context("spawn")`) with monotonic deadlines, `terminate() -> kill()` escalation, output size limits, and lambda/closure rejection. | Infinite loops, blocking sleeps, and `os._exit()` child crashes in `PROCESS` mode are terminated/detected within bounded time, fenced before retry, and leave 0 zombie processes. |
| **P2** | **Phase 3 & 4** | Explicit idempotency (`operation_key`, `idempotency_key`, `attempt_key`), seeded jitter, retry accounting across `RETRY_WAIT` crashes, canonical SHA-256 `definition_hash`, `implementation_version`, and `CompatibilityPolicy`. | Altered task definitions or `implementation_version` fail under `STRICT`, succeed under `ALLOW_IMPLEMENTATION_UPGRADE`, and legacy v1 logs migrate under `MIGRATE_LEGACY_V1`. |
| **P3** | **Phase 7 & 8** | Graceful/forced engine `shutdown()`, context manager support, `FailureCategory`, per-task queue/execution latency, `WorkflowMetrics`, `MetricsSink`, payload redaction, and `scripts/inspect_workflow.py` read-only CLI. | `inspect_workflow.py` inspects healthy, truncated, and corrupted logs without modifying files; metrics and failure categories are verified in unit tests. |
| **P4** | **Phase 9, 10 & 11** | Multi-OS CI workflow (`.github/workflows/ci.yml`), reproducible benchmark suite (`scripts/run_benchmarks.py` + `docs/BENCHMARKS.md`), SQLite & API idempotency examples, `docs/MIGRATION.md`, and `docs/DISTRIBUTED_ARCHITECTURE_PROPOSAL.md`. | All unit, integration, process-isolation, and adversarial tests pass (`0` failures/errors); all examples and benchmarks run reproducibly. |
