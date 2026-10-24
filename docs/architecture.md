# CHIMERA Architecture & Invariants (`v0.2.0`)

## Layered Module Architecture

CHIMERA is organized into eleven focused modules under `src/chimera/`:

| Module | Primary Responsibility |
| :--- | :--- |
| `exceptions.py` | Typed exception hierarchy rooted at `ChimeraError` (`WorkflowValidationError`, `InvalidWorkflowIdError`, `CycleDetectedError`, `PayloadSerializationError`, `OversizedPayloadError`, `WorkflowDefinitionMismatchError`, `LogCorruptionError`, `OversizedFrameError`, `LogAuthenticationError`, `CorruptedStateTransitionError`, `ConcurrentWorkflowAccessError`, `UnserializableCallableError`, `ProcessWorkerCrashError`, `TaskOutputLimitExceededError`, `TaskTimeoutError`). |
| `clock.py` | `Clock` protocol, production `SystemClock`, and deterministic `VirtualClock`. |
| `serialization.py` | Strict RFC 8259 canonical JSON serialization (`allow_nan=False`, no `repr()` fallback), byte-size enforcement (`max_payload_bytes`), canonical SHA-256 hashing, and `spawn` child-process callable verification. |
| `models.py` | `TaskState`, `WorkflowStatus`, `ExecutionBackend`, `CompatibilityPolicy`, `FailureCategory`, `ClaimToken`, `TaskContext`, `TaskSpec`, `TaskRuntimeState`, `TaskResultView`, `StructuredLogEntry`, `WorkflowMetrics`, and `WorkflowStatusSummary`. |
| `validation.py` | `validate_workflow_id()` (path-traversal & Windows reserved-name guard) and `validate_dag()` (iterative 3-color DFS cycle detection). |
| `locking.py` | Fork-safe, cross-process (`fcntl.flock` / `msvcrt.locking`) and in-process (`_ACTIVE_IN_PROCESS_LOCKS`) exclusive file locking (`WorkflowFileLock`) with `0o600`/`0o700` permissions. |
| `persistence.py` | Framed append-only `EventLog` supporting `@CHIMERA1` (CRC32) and `@CHIMERA2` (CRC32 + chained HMAC-SHA256), frame size bounds, strict schema validation, tail-truncation repair, and atomic `migrate_v1_to_v2()`. |
| `scheduler.py` | `DeterministicReadyScheduler` (`(priority, task_id)` min-heap), eligible task promotion, and BFS failure cascade. |
| `executor.py` | Supervised `ProcessTaskExecutor` (`multiprocessing.get_context("spawn")`) with dedicated per-child pipes, monotonic deadlines, 3-stage `cancel -> terminate() -> kill()` escalation, and output size limits. |
| `recovery.py` | `RecoveryManager` verifying `definition_version`, canonical SHA-256 `definition_hash`, `implementation_version`, and `CompatibilityPolicy` (`STRICT`, `ALLOW_IMPLEMENTATION_UPGRADE`, `MIGRATE_LEGACY_V1`). |
| `diagnostics.py` | `MetricsSink` protocol (`NullMetricsSink`, `InMemoryMetricsSink`) and read-only `inspect_workflow_log()` diagnostic analyzer. |
| `engine.py` | `WorkflowEngine` state machine, thread/process worker dispatch, timeout/backoff supervision, cancellation, shutdown, recovery, and public API functions. |

## Core System Invariants

1. **Topological Readiness ($I_1$)**: A task $T$ transitions to `READY` or `RUNNING` if and only if every dependency $D \in \text{dependencies}(T)$ is in state `SUCCEEDED`.
2. **Single-Active-Claim Exclusion ($I_2$)**: Every `READY -> RUNNING` claim increments a strictly monotonic `claim_epoch` counter and issues a `ClaimToken(workflow_id, task_id, attempt, claim_epoch, worker_id)` under `self._cond`.
3. **Fenced Completion Monotonicity ($I_3$)**: `complete_task_attempt` accepts a worker's result if and only if `not self._cancel_requested`, `rt.state == TaskState.RUNNING`, `rt.claim_epoch == token.claim_epoch`, and `rt.attempt == token.attempt`. On timeout, the expired attempt is durably fenced (`claim_epoch` incremented in `EventLog`) **before** the child process is killed or any replacement attempt can start or commit.
4. **At-Most-Once Post-Commit Execution ($I_4$)**: `TaskState.SUCCEEDED` has an empty set of outgoing transitions (`frozenset()`). Once a task's `SUCCEEDED` record is `fsync`ed to disk, neither live execution nor crash recovery can ever transition or re-execute that task.
5. **Lossless Canonical Persistence ($I_5$)**: Every persisted `input_payload` and task `result` is validated as strict canonical JSON (`allow_nan=False`) before persistence; unserializable or oversized values fail fast with `PayloadSerializationError` / `TaskOutputLimitExceededError` rather than degrading silently via `repr()`.
