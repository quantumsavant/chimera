"""
Custom exception hierarchy for the CHIMERA workflow orchestration engine.
"""

from __future__ import annotations


class ChimeraError(Exception):
    """Base exception for all CHIMERA workflow engine errors."""


class WorkflowValidationError(ChimeraError):
    """Raised when a workflow DAG or recovered TaskSpec fails validation."""


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


class InvalidStateTransitionError(ChimeraError):
    """Raised when an illegal task state transition is attempted."""


class LogCorruptionError(ChimeraError):
    """
    Raised when an event log contains corrupted headers, sequence gaps,
    CRC32 checksum mismatches, mid-file truncations, or invalid transitions.
    """


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


class TaskTimeoutError(ChimeraError):
    """Recorded when a task execution exceeds its configured timeout_seconds."""


class WorkflowNotFoundError(ChimeraError):
    """Raised when a requested workflow_id cannot be found in memory or on disk."""


class SimulatedProcessCrash(BaseException):
    """
    BaseException subclass used for deterministic fault/crash injection in tests.
    Inherits from BaseException (like SystemExit/KeyboardInterrupt) so standard
    `except Exception:` blocks inside task workers do not accidentally swallow it.
    """
