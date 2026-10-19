"""
Core domain models, state enums, fencing tokens, and immutable status views.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import json
import threading
from typing import Any, Callable, Sequence

from chimera.exceptions import WorkflowValidationError


class TaskState(str, Enum):
    """Observable lifecycle states of a workflow task."""

    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_TASK_STATES: frozenset[TaskState] = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}
)

# Live execution state transitions. Note: RUNNING -> READY is disallowed during
# live execution and permitted exclusively during crash recovery reconciliation.
LIVE_ALLOWED_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset({TaskState.READY, TaskState.CANCELLED}),
    TaskState.READY: frozenset({TaskState.RUNNING, TaskState.CANCELLED}),
    TaskState.RUNNING: frozenset(
        {
            TaskState.SUCCEEDED,
            TaskState.FAILED,
            TaskState.RETRY_WAIT,
            TaskState.CANCELLED,
        }
    ),
    TaskState.RETRY_WAIT: frozenset({TaskState.READY, TaskState.CANCELLED}),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}

RECOVERY_ALLOWED_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    **LIVE_ALLOWED_TRANSITIONS,
    TaskState.RUNNING: LIVE_ALLOWED_TRANSITIONS[TaskState.RUNNING]
    | frozenset({TaskState.READY}),
}


class WorkflowStatus(str, Enum):
    """High-level lifecycle status of a workflow."""

    CREATED = "CREATED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class ClaimToken:
    """Monotonic fencing token uniquely identifying a single worker claim of a task."""

    workflow_id: str
    task_id: str
    attempt: int
    claim_epoch: int
    worker_id: str


@dataclass(frozen=True)
class TaskContext:
    """
    Execution context passed to task functions that accept `(payload, context)`.

    Attributes:
        workflow_id: Identifier of the enclosing workflow.
        task_id: Identifier of the task being executed.
        attempt: 1-based attempt number for this execution.
        claim_epoch: Strictly monotonic fencing token for this claim.
        idempotency_key: Deterministic key `<workflow_id>:<task_id>` for
            deduplicating external side effects across crash-recovery boundaries.
        dependency_results: Deep-copied mapping of upstream `task_id -> result`.
    """

    workflow_id: str
    task_id: str
    attempt: int
    claim_epoch: int
    idempotency_key: str
    dependency_results: dict[str, Any]
    _cancel_event: threading.Event = field(repr=False)

    def is_cancelled(self) -> bool:
        """Return True if cancellation or timeout supersession was signaled."""
        return self._cancel_event.is_set()

    def raise_if_cancelled(self) -> None:
        """Raise `InterruptedError` if this task attempt has been cancelled or superseded."""
        if self._cancel_event.is_set():
            raise InterruptedError(
                f"Task '{self.task_id}' (attempt={self.attempt}, epoch={self.claim_epoch}) "
                "was cancelled or superseded."
            )


TaskCallable = Callable[..., Any]


@dataclass
class TaskSpec:
    """
    Specification of a single task in a workflow DAG.

    `func` may accept:
    - `()` (0 positional arguments)
    - `(payload,)` (1 positional argument: deep-copied `input_payload`)
    - `(payload, context)` (2 positional arguments: deep-copied `input_payload`
      and `TaskContext`)
    """

    task_id: str
    func: TaskCallable
    input_payload: Any = None
    dependencies: Sequence[str] = field(default_factory=tuple)
    timeout_seconds: float | None = None
    max_retries: int = 0
    base_backoff_seconds: float = 0.05
    backoff_factor: float = 2.0
    max_backoff_seconds: float = 5.0
    retryable_exceptions: tuple[type[BaseException], ...] = (Exception,)
    retry_on_timeout: bool = False
    priority: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, str) or not self.task_id.strip():
            raise WorkflowValidationError("task_id must be a non-empty string.")
        if not callable(self.func):
            raise WorkflowValidationError(
                f"Task '{self.task_id}' func must be callable."
            )
        if self.max_retries < 0:
            raise WorkflowValidationError(
                f"Task '{self.task_id}' max_retries must be >= 0."
            )
        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise WorkflowValidationError(
                f"Task '{self.task_id}' timeout_seconds must be > 0 when specified."
            )
        if self.base_backoff_seconds < 0:
            raise WorkflowValidationError(
                f"Task '{self.task_id}' base_backoff_seconds must be >= 0."
            )
        if self.backoff_factor < 1.0:
            raise WorkflowValidationError(
                f"Task '{self.task_id}' backoff_factor must be >= 1.0."
            )
        if self.max_backoff_seconds < 0:
            raise WorkflowValidationError(
                f"Task '{self.task_id}' max_backoff_seconds must be >= 0."
            )
        object.__setattr__(self, "dependencies", tuple(self.dependencies))
        object.__setattr__(
            self, "input_payload", copy.deepcopy(self.input_payload)
        )
        object.__setattr__(
            self, "retryable_exceptions", tuple(self.retryable_exceptions)
        )

    def compute_backoff(self, attempt: int) -> float:
        """Compute deterministic exponential backoff after `attempt` failed (1-indexed)."""
        if self.base_backoff_seconds <= 0:
            return 0.0
        delay = self.base_backoff_seconds * (
            self.backoff_factor ** max(0, attempt - 1)
        )
        return float(min(delay, self.max_backoff_seconds))

    def to_metadata_dict(self) -> dict[str, Any]:
        """Serialize non-callable task configuration for the durable event log."""
        try:
            safe_payload = json.loads(
                json.dumps(self.input_payload, sort_keys=True, ensure_ascii=True)
            )
        except (TypeError, OverflowError):
            safe_payload = repr(self.input_payload)

        return {
            "task_id": self.task_id,
            "input_payload": safe_payload,
            "dependencies": list(self.dependencies),
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "base_backoff_seconds": self.base_backoff_seconds,
            "backoff_factor": self.backoff_factor,
            "max_backoff_seconds": self.max_backoff_seconds,
            "retryable_exception_names": [
                exc.__name__ for exc in self.retryable_exceptions
            ],
            "retry_on_timeout": self.retry_on_timeout,
            "priority": self.priority,
        }


@dataclass
class TaskRuntimeState:
    """Mutable internal state of a task managed under the engine lock."""

    task_id: str
    state: TaskState = TaskState.PENDING
    attempt: int = 0
    claim_epoch: int = 0
    recovery_count: int = 0
    result: Any = None
    has_result: bool = False
    error_type: str | None = None
    error_message: str | None = None
    last_transition_reason: str = "INITIALIZED"
    last_updated_at: float = 0.0
    started_at: float | None = None
    deadline_at: float | None = None
    wake_at: float | None = None
    worker_id: str | None = None


@dataclass(frozen=True)
class TaskResultView:
    """Immutable public snapshot of a single task's state and result."""

    workflow_id: str
    task_id: str
    state: TaskState
    attempt: int
    claim_epoch: int
    recovery_count: int
    result: Any
    has_result: bool
    error_type: str | None
    error_message: str | None
    last_transition_reason: str
    last_updated_at: float


@dataclass(frozen=True)
class StructuredLogEntry:
    """Structured observability record emitted for every persisted event."""

    seq: int
    timestamp: float
    workflow_id: str
    event_type: str
    task_id: str | None
    attempt: int | None
    claim_epoch: int | None
    from_state: str | None
    to_state: str | None
    reason: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "timestamp": self.timestamp,
            "workflow_id": self.workflow_id,
            "event_type": self.event_type,
            "task_id": self.task_id,
            "attempt": self.attempt,
            "claim_epoch": self.claim_epoch,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "reason": self.reason,
            "details": copy.deepcopy(self.details),
        }


@dataclass(frozen=True)
class WorkflowStatusSummary:
    """Immutable public summary of workflow status, task counts, and structured logs."""

    workflow_id: str
    status: WorkflowStatus
    cancel_requested: bool
    total_tasks: int
    counts_by_state: dict[str, int]
    tasks: dict[str, TaskResultView]
    truncated_tail_repaired: bool
    log_record_count: int
    structured_logs: list[StructuredLogEntry]
