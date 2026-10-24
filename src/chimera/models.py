"""
Core domain models, state enums, fencing tokens, execution backend policies,
failure classifications, and immutable status views.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import random
import threading
from typing import Any, Callable, Sequence

from chimera.exceptions import WorkflowValidationError
from chimera.serialization import (
    DEFAULT_MAX_PAYLOAD_BYTES,
    canonical_json_bytes,
    canonical_json_loads,
    compute_canonical_sha256,
    describe_callable_identity,
    validate_callable_for_process_backend,
    validate_canonical_json_value,
)


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


class ExecutionBackend(str, Enum):
    """Execution backend for running workflow tasks."""

    THREAD = "THREAD"
    PROCESS = "PROCESS"


class CompatibilityPolicy(str, Enum):
    """
    Policy governing workflow definition compatibility during crash recovery.

    - `STRICT`: Requires exact match of `definition_version`, `definition_hash`,
      all task configurations, and `implementation_version`. Also accepts
      v1 legacy logs if their v1 task metadata matches and `implementation_version`
      is default `"1.0"`.
    - `ALLOW_IMPLEMENTATION_UPGRADE`: Requires exact match of the DAG structure,
      task payloads, and retry/timeout configuration, but permits a task's
      `implementation_version` or `definition_version` to advance.
    - `MIGRATE_LEGACY_V1`: Explicitly migrates legacy v1 logs (which lacked
      `definition_hash` and `implementation_version`) to the v2 schema.
    """

    STRICT = "STRICT"
    ALLOW_IMPLEMENTATION_UPGRADE = "ALLOW_IMPLEMENTATION_UPGRADE"
    MIGRATE_LEGACY_V1 = "MIGRATE_LEGACY_V1"


class FailureCategory(str, Enum):
    """Structured failure classification for operational diagnostics and metrics."""

    NONE = "NONE"
    APPLICATION_ERROR = "APPLICATION_ERROR"
    TIMEOUT = "TIMEOUT"
    PROCESS_CRASH = "PROCESS_CRASH"
    SERIALIZATION_ERROR = "SERIALIZATION_ERROR"
    CANCELLED = "CANCELLED"
    DEPENDENCY_FAILED = "DEPENDENCY_FAILED"
    INFRASTRUCTURE_ERROR = "INFRASTRUCTURE_ERROR"


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
        idempotency_key: Logical operation idempotency key (`operation_key` if
            configured on `TaskSpec`, otherwise `<workflow_id>:<task_id>`).
            Remains stable across retries and crash-recovery attempts of the
            same logical task operation.
        dependency_results: Deep-copied mapping of upstream `task_id -> result`.
        operation_key: Explicit logical business operation identifier.
        attempt_idempotency_key: Attempt-scoped key
            `<idempotency_key>:attempt:<attempt>:epoch:<claim_epoch>`.
    """

    workflow_id: str
    task_id: str
    attempt: int
    claim_epoch: int
    idempotency_key: str
    dependency_results: dict[str, Any]
    _cancel_event: Any = field(default_factory=threading.Event, repr=False)
    operation_key: str = ""
    attempt_idempotency_key: str = ""

    def __post_init__(self) -> None:
        if not self.operation_key:
            object.__setattr__(self, "operation_key", self.idempotency_key)
        if not self.attempt_idempotency_key:
            object.__setattr__(
                self,
                "attempt_idempotency_key",
                f"{self.idempotency_key}:attempt:{self.attempt}:epoch:{self.claim_epoch}",
            )

    def is_cancelled(self) -> bool:
        """Return True if cancellation or timeout supersession was signaled."""
        return bool(self._cancel_event.is_set())

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
    operation_key: str | None = None
    jitter_seconds: float = 0.0
    jitter_seed: int | None = None
    implementation_version: str = "1.0"
    execution_backend: ExecutionBackend | str | None = None
    max_output_bytes: int | None = None

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
        if self.jitter_seconds < 0:
            raise WorkflowValidationError(
                f"Task '{self.task_id}' jitter_seconds must be >= 0."
            )
        if (
            self.operation_key is not None
            and (not isinstance(self.operation_key, str) or not self.operation_key.strip())
        ):
            raise WorkflowValidationError(
                f"Task '{self.task_id}' operation_key must be a non-empty string when provided."
            )
        if (
            not isinstance(self.implementation_version, str)
            or not self.implementation_version.strip()
        ):
            raise WorkflowValidationError(
                f"Task '{self.task_id}' implementation_version must be a non-empty string."
            )
        if self.max_output_bytes is not None and self.max_output_bytes <= 0:
            raise WorkflowValidationError(
                f"Task '{self.task_id}' max_output_bytes must be > 0 when specified."
            )

        resolved_backend: ExecutionBackend | None = None
        if self.execution_backend is not None:
            try:
                resolved_backend = (
                    self.execution_backend
                    if isinstance(self.execution_backend, ExecutionBackend)
                    else ExecutionBackend(str(self.execution_backend).upper())
                )
            except ValueError as exc:
                raise WorkflowValidationError(
                    f"Task '{self.task_id}' has invalid execution_backend {self.execution_backend!r}."
                ) from exc
            object.__setattr__(self, "execution_backend", resolved_backend)

        if resolved_backend == ExecutionBackend.PROCESS:
            validate_callable_for_process_backend(self.func, self.task_id)

        # Validate that input_payload is losslessly JSON-serializable upfront
        validate_canonical_json_value(
            self.input_payload, path=f"TaskSpec({self.task_id}).input_payload"
        )
        canonical_json_bytes(
            self.input_payload, max_bytes=DEFAULT_MAX_PAYLOAD_BYTES
        )

        object.__setattr__(self, "dependencies", tuple(self.dependencies))
        object.__setattr__(
            self, "input_payload", copy.deepcopy(self.input_payload)
        )
        object.__setattr__(
            self, "retryable_exceptions", tuple(self.retryable_exceptions)
        )

    def resolve_idempotency_key(self, workflow_id: str) -> str:
        """Return the logical operation idempotency key for this task."""
        if self.operation_key is not None:
            return self.operation_key
        return f"{workflow_id}:{self.task_id}"

    def compute_backoff(self, attempt: int, *, workflow_id: str = "") -> float:
        """
        Compute bounded exponential backoff after `attempt` failed (1-indexed),
        with optional deterministic/seeded jitter.
        """
        if self.base_backoff_seconds <= 0 and self.jitter_seconds <= 0:
            return 0.0
        delay = (
            self.base_backoff_seconds
            * (self.backoff_factor ** max(0, attempt - 1))
            if self.base_backoff_seconds > 0
            else 0.0
        )
        if self.jitter_seconds > 0:
            seed_material = f"{workflow_id}:{self.task_id}:{attempt}:{self.jitter_seed}".encode(
                "utf-8"
            )
            seed_int = int.from_bytes(
                hashlib.sha256(seed_material).digest()[:8], "big"
            )
            rng = random.Random(seed_int)
            delay += rng.uniform(0.0, self.jitter_seconds)
        return float(min(delay, self.max_backoff_seconds))

    def to_v1_metadata_dict(self) -> dict[str, Any]:
        """Return the legacy v1 metadata dictionary representation."""
        safe_payload = canonical_json_loads(
            canonical_json_bytes(self.input_payload, max_bytes=None)
        )
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

    def to_metadata_dict(self) -> dict[str, Any]:
        """
        Serialize recovery-relevant task configuration for the durable event log.
        Uses strict canonical JSON without lossy `repr()` fallbacks.
        """
        meta = self.to_v1_metadata_dict()
        meta["implementation_version"] = self.implementation_version
        if self.operation_key is not None:
            meta["operation_key"] = self.operation_key
        if self.jitter_seconds > 0:
            meta["jitter_seconds"] = self.jitter_seconds
        if self.jitter_seed is not None:
            meta["jitter_seed"] = self.jitter_seed
        if self.execution_backend is not None:
            meta["execution_backend"] = (
                self.execution_backend.value
                if isinstance(self.execution_backend, ExecutionBackend)
                else str(self.execution_backend)
            )
        if self.max_output_bytes is not None:
            meta["max_output_bytes"] = self.max_output_bytes
        return meta

    def core_configuration_dict(self) -> dict[str, Any]:
        """Return the configuration dictionary excluding `implementation_version`."""
        meta = self.to_metadata_dict()
        meta.pop("implementation_version", None)
        return meta

    def callable_identity(self) -> str:
        """Return a descriptive `<module>:<qualname>` string for diagnostics."""
        return describe_callable_identity(self.func)


def compute_workflow_definition_hash(
    task_specs: dict[str, TaskSpec],
    definition_version: int = 1,
) -> str:
    """Compute a deterministic SHA-256 hash over the workflow's canonical task specifications."""
    canonical_payload = {
        "definition_version": int(definition_version),
        "tasks": {
            tid: spec.to_metadata_dict()
            for tid, spec in sorted(task_specs.items())
        },
    }
    return compute_canonical_sha256(canonical_payload)


@dataclass
class TaskRuntimeState:
    """Mutable internal state of a task managed under the engine lock."""

    task_id: str
    state: TaskState = TaskState.PENDING
    attempt: int = 0
    claim_epoch: int = 0
    recovery_count: int = 0
    retry_count: int = 0
    timeout_count: int = 0
    result: Any = None
    has_result: bool = False
    error_type: str | None = None
    error_message: str | None = None
    failure_category: FailureCategory = FailureCategory.NONE
    last_transition_reason: str = "INITIALIZED"
    last_updated_at: float = 0.0
    ready_at: float | None = None
    started_at: float | None = None
    completed_at: float | None = None
    deadline_at: float | None = None
    wake_at: float | None = None
    queue_delay_seconds: float | None = None
    execution_duration_seconds: float | None = None
    worker_id: str | None = None
    worker_pid: int | None = None
    exit_code: int | None = None


@dataclass(frozen=True)
class TaskResultView:
    """Immutable public snapshot of a single task's state, metrics, and result."""

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
    retry_count: int = 0
    timeout_count: int = 0
    failure_category: FailureCategory = FailureCategory.NONE
    queue_delay_seconds: float | None = None
    execution_duration_seconds: float | None = None
    worker_pid: int | None = None
    exit_code: int | None = None


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
    failure_category: str = "NONE"
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
            "failure_category": self.failure_category,
            "details": copy.deepcopy(self.details),
        }


@dataclass(frozen=True)
class WorkflowMetrics:
    """Aggregate operational counters for a workflow execution."""

    succeeded_count: int = 0
    failed_count: int = 0
    cancelled_count: int = 0
    stale_completion_count: int = 0
    recovery_event_count: int = 0
    timeout_event_count: int = 0
    worker_crash_count: int = 0
    retry_scheduled_count: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "succeeded_count": self.succeeded_count,
            "failed_count": self.failed_count,
            "cancelled_count": self.cancelled_count,
            "stale_completion_count": self.stale_completion_count,
            "recovery_event_count": self.recovery_event_count,
            "timeout_event_count": self.timeout_event_count,
            "worker_crash_count": self.worker_crash_count,
            "retry_scheduled_count": self.retry_scheduled_count,
        }


@dataclass(frozen=True)
class WorkflowStatusSummary:
    """Immutable public summary of workflow status, task counts, metrics, and logs."""

    workflow_id: str
    status: WorkflowStatus
    cancel_requested: bool
    total_tasks: int
    counts_by_state: dict[str, int]
    tasks: dict[str, TaskResultView]
    truncated_tail_repaired: bool
    log_record_count: int
    structured_logs: list[StructuredLogEntry]
    definition_version: int = 1
    definition_hash: str = ""
    execution_backend: str = "THREAD"
    metrics: WorkflowMetrics = field(default_factory=WorkflowMetrics)
