"""
Custom exception hierarchy for the CHIMERA workflow orchestration engine.
"""

from __future__ import annotations


class ChimeraError(Exception):
    """Base exception for all CHIMERA workflow engine errors."""


class WorkflowValidationError(ChimeraError):
    """Raised when a workflow DAG, configuration, or recovered TaskSpec fails validation."""


class InvalidWorkflowIdError(WorkflowValidationError):
    """Raised when a workflow_id is empty, overly long, or contains unsafe path characters."""


class DuplicateTaskIdError(WorkflowValidationError):
    """Raised when two or more tasks in a workflow share the same task_id."""


class MissingDependencyError(WorkflowValidationError):
    """Raised when a task references a dependency task_id that does not exist."""


class CycleDetectedError(WorkflowValidationError):
    """Raised when the workflow dependency graph contains a directed cycle."""

    def __init__(self, cycle_path: list[str]) -> None:
        self.cycle_path = cycle_path
        cycle_str = " -> ".join(cycle_path)
        super().__init__(f"Dependency cycle detected: {cycle_str}")


class PayloadSerializationError(WorkflowValidationError):
    """
    Raised when a task input_payload or task result cannot be losslessly
    serialized to canonical JSON (e.g., sets, arbitrary objects, NaN/Infinity).
    """


class OversizedPayloadError(PayloadSerializationError):
    """Raised when a serialized payload or task output exceeds the configured byte limit."""


class WorkflowDefinitionMismatchError(WorkflowValidationError):
    """
    Raised during recovery when the provided workflow definition version,
    canonical hash, task metadata, or implementation version is incompatible
    with the persisted workflow log under the active CompatibilityPolicy.
    """


class MissingWorkflowDefinitionError(WorkflowValidationError):
    """
    Raised when recover_workflow() is invoked without task specifications and
    no verified definition is available in memory, unless read-only inspection
    mode is explicitly requested.
    """


class LegacyLogMigrationError(WorkflowValidationError):
    """Raised when a legacy log cannot be migrated under the requested policy."""


class InvalidStateTransitionError(ChimeraError):
    """Raised when an illegal task state transition is attempted."""


class LogCorruptionError(ChimeraError):
    """
    Raised when an event log contains corrupted headers, sequence gaps,
    CRC32 checksum mismatches, mid-file truncations, or invalid transitions.
    """


class OversizedFrameError(LogCorruptionError):
    """Raised when an event log frame exceeds the maximum configured frame size."""


class LogAuthenticationError(LogCorruptionError):
    """Raised when an @CHIMERA2 frame fails HMAC-SHA256 authentication."""


class CorruptedStateTransitionError(LogCorruptionError, InvalidStateTransitionError):
    """
    Raised when an event log record has valid framing and CRC32 checksum but
    encodes an illegal or inconsistent state transition, unknown task_id,
    duplicate WORKFLOW_CREATED event, or non-monotonic fencing epoch.
    Inherits from both `LogCorruptionError` and `InvalidStateTransitionError`.
    """


class ConcurrentWorkflowAccessError(ChimeraError):
    """
    Raised when two independent processes or two engine/log instances attempt to
    open, mutate, or recover the same workflow log concurrently.
    """


class ExecutorError(ChimeraError):
    """Base exception for task execution backend failures."""


class UnserializableCallableError(WorkflowValidationError, ExecutorError):
    """
    Raised when a task callable cannot be resolved across a child process
    boundary (e.g., lambdas, closures, or non-importable callables) on the
    PROCESS execution backend.
    """


class ProcessWorkerCrashError(ExecutorError):
    """Raised when a supervised child worker process terminates abnormally or crashes."""

    def __init__(self, message: str, *, exit_code: int | None = None) -> None:
        self.exit_code = exit_code
        super().__init__(message)


class TaskOutputLimitExceededError(OversizedPayloadError, ExecutorError):
    """Raised when a task's serialized return value exceeds max_output_bytes."""


class TaskTimeoutError(ChimeraError):
    """Recorded when a task execution exceeds its configured timeout_seconds."""


class WorkflowNotFoundError(ChimeraError):
    """Raised when a requested workflow_id cannot be found in memory or on disk."""


class WorkflowShutdownError(ChimeraError):
    """Raised when an operation is attempted on a shut-down engine instance."""


class SimulatedProcessCrash(BaseException):
    """
    BaseException subclass used for deterministic fault/crash injection in tests.
    Inherits from BaseException (like SystemExit/KeyboardInterrupt) so standard
    `except Exception:` blocks inside task workers do not accidentally swallow it.
    """
