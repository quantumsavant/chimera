# CHIMERA Architecture & Invariants

## Layered Module Architecture

CHIMERA is organized into nine focused modules under `src/chimera/`:

| Module | Primary Responsibility |
| :--- | :--- |
| `exceptions.py` | Hierarchy of typed exceptions (`ChimeraError`, `WorkflowValidationError`, `CycleDetectedError`, `LogCorruptionError`, `CorruptedStateTransitionError`, `ConcurrentWorkflowAccessError`, `TaskTimeoutError`). |
| `clock.py` | `Clock` protocol, production `SystemClock`, and deterministic `VirtualClock`. |
| `models.py` | `TaskState`, `WorkflowStatus`, `ClaimToken`, `TaskContext`, `TaskSpec`, `TaskRuntimeState`, `TaskResultView`, `StructuredLogEntry`, and `WorkflowStatusSummary`. |
| `locking.py` | Cross-process (`fcntl.flock` / `msvcrt.locking`) and in-process (`_ACTIVE_IN_PROCESS_LOCKS`) exclusive file locking (`WorkflowFileLock`). |
| `persistence.py` | Framed CRC32 append-only `EventLog`, atomic tail truncation repair, parent-directory `fsync`, and partial-write fault injection. |
| `validation.py` | DAG validation (`validate_dag`) with iterative 3-color DFS cycle detection. |
| `scheduler.py` | `DeterministicReadyScheduler` (`(priority, task_id)` min-heap), eligible task promotion, and transitive failure cascade. |
| `recovery.py` | `RecoveryManager` verifying persisted `WORKFLOW_CREATED` metadata against provided `TaskSpec` definitions. |
| `engine.py` | `WorkflowEngine` state machine, fenced worker pool orchestration, timeout/backoff enforcement, cancellation, recovery, and public API functions. |

## Core System Invariants

1. **Topological Readiness ($I_1$)**: A task $T$ transitions to `READY` or `RUNNING` if and only if every dependency $D \in \text{dependencies}(T)$ is in state `SUCCEEDED`.
2. **Single-Active-Claim Exclusion ($I_2$)**: Every `READY -> RUNNING` claim increments a strictly monotonic `claim_epoch` counter and issues a `ClaimToken(workflow_id, task_id, attempt, claim_epoch, worker_id)` under `self._cond`.
3. **Fenced Completion Monotonicity ($I_3$)**: `complete_task_attempt` accepts a worker's result if and only if `not self._cancel_requested`, `rt.state == TaskState.RUNNING`, `rt.claim_epoch == token.claim_epoch`, and `rt.attempt == token.attempt`. Superseded completions are logged as `STALE_COMPLETION_IGNORED` and rejected, and superseded worker threads exit the worker loop immediately.
4. **At-Most-Once Post-Commit Execution ($I_4$)**: `TaskState.SUCCEEDED` has an empty set of outgoing transitions (`frozenset()`). Once a task's `SUCCEEDED` record is `fsync`ed to disk, neither live execution nor crash recovery can ever transition or re-execute that task.
5. **Event-Sourced State Equivalence ($I_5$)**: Every state change is appended to `EventLog` first and then applied to in-memory state via `_apply_event(record)`—the exact same reducer used during `recover_workflow()`.
