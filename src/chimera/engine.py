"""
Core workflow orchestration engine (`WorkflowEngine`) and top-level public API.
"""

from __future__ import annotations

from contextlib import contextmanager
import copy
from pathlib import Path
import tempfile
import threading
from typing import Any, Callable, Iterator, Sequence
import uuid

from chimera.clock import Clock, SystemClock, VirtualClock
from chimera.diagnostics import MetricsSink, NullMetricsSink
from chimera.exceptions import (
    CorruptedStateTransitionError,
    InvalidStateTransitionError,
    LogCorruptionError,
    MissingWorkflowDefinitionError,
    OversizedPayloadError,
    PayloadSerializationError,
    ProcessWorkerCrashError,
    SimulatedProcessCrash,
    TaskOutputLimitExceededError,
    WorkflowNotFoundError,
    WorkflowShutdownError,
    WorkflowValidationError,
)
from chimera.executor import ProcessTaskExecutor, TaskAttemptResult
from chimera.locking import (
    DEFAULT_FILE_MODE,
    WorkflowFileLock,
    ensure_private_directory,
)
from chimera.models import (
    LIVE_ALLOWED_TRANSITIONS,
    RECOVERY_ALLOWED_TRANSITIONS,
    TERMINAL_TASK_STATES,
    ClaimToken,
    CompatibilityPolicy,
    ExecutionBackend,
    FailureCategory,
    StructuredLogEntry,
    TaskContext,
    TaskResultView,
    TaskRuntimeState,
    TaskSpec,
    TaskState,
    WorkflowMetrics,
    WorkflowStatus,
    WorkflowStatusSummary,
    compute_workflow_definition_hash,
)
from chimera.persistence import EventLog
from chimera.recovery import RecoveryManager
from chimera.scheduler import DeterministicReadyScheduler, invoke_task_callable
from chimera.serialization import (
    DEFAULT_MAX_FRAME_BYTES,
    DEFAULT_MAX_PAYLOAD_BYTES,
    canonical_json_bytes,
    canonical_json_loads,
    validate_callable_for_process_backend,
)
from chimera.validation import validate_dag, validate_workflow_id


class WorkflowEngine:
    """
    Fault-tolerant DAG workflow engine managing task state transitions, durable
    event logging, fenced worker claims, thread and child-process execution
    backends, timeouts, retries, cancellation, metrics, and crash recovery.
    """

    def __init__(
        self,
        tasks: Sequence[TaskSpec],
        max_concurrency: int = 4,
        workflow_id: str | None = None,
        storage_dir: str | Path | None = None,
        clock: Clock | None = None,
        crash_hook: Callable[[str, dict[str, Any], int], None] | None = None,
        partial_write_bytes: int | None = None,
        *,
        execution_backend: ExecutionBackend | str = ExecutionBackend.THREAD,
        definition_version: int = 1,
        compatibility_policy: CompatibilityPolicy | str = CompatibilityPolicy.STRICT,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        max_output_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        file_mode: int = DEFAULT_FILE_MODE,
        hmac_key: bytes | None = None,
        log_format_version: int | None = None,
        metrics_sink: MetricsSink | None = None,
        log_sensitive_payloads: bool = False,
        process_grace_period_seconds: float = 0.03,
        process_terminate_wait_seconds: float = 0.05,
    ) -> None:
        if max_concurrency < 1:
            raise WorkflowValidationError("max_concurrency must be >= 1.")
        if definition_version < 1:
            raise WorkflowValidationError("definition_version must be >= 1.")
        if max_payload_bytes <= 0 or max_frame_bytes <= 0 or max_output_bytes <= 0:
            raise WorkflowValidationError(
                "max_payload_bytes, max_frame_bytes, and max_output_bytes must be > 0."
            )

        raw_wf_id = workflow_id if workflow_id is not None else f"wf_{uuid.uuid4().hex[:12]}"
        self.workflow_id: str = validate_workflow_id(raw_wf_id)
        self.max_concurrency: int = int(max_concurrency)
        self.definition_version: int = int(definition_version)
        self.max_payload_bytes: int = int(max_payload_bytes)
        self.max_frame_bytes: int = int(max_frame_bytes)
        self.max_output_bytes: int = int(max_output_bytes)
        self.file_mode: int = int(file_mode)
        self.log_sensitive_payloads: bool = bool(log_sensitive_payloads)
        self._process_grace_period_seconds = float(process_grace_period_seconds)
        self._process_terminate_wait_seconds = float(process_terminate_wait_seconds)

        try:
            self.execution_backend: ExecutionBackend = (
                execution_backend
                if isinstance(execution_backend, ExecutionBackend)
                else ExecutionBackend(str(execution_backend).upper())
            )
        except ValueError as exc:
            raise WorkflowValidationError(
                f"Invalid execution_backend: {execution_backend!r}"
            ) from exc

        try:
            self.compatibility_policy: CompatibilityPolicy = (
                compatibility_policy
                if isinstance(compatibility_policy, CompatibilityPolicy)
                else CompatibilityPolicy(str(compatibility_policy).upper())
            )
        except ValueError as exc:
            raise WorkflowValidationError(
                f"Invalid compatibility_policy: {compatibility_policy!r}"
            ) from exc

        self._raw_tasks: list[TaskSpec] = list(tasks)
        self._clock: Clock = clock or SystemClock()
        self._metrics_sink: MetricsSink = metrics_sink or NullMetricsSink()

        if storage_dir is None:
            storage_dir = Path(tempfile.gettempdir()) / "chimera_workflows"
        self.storage_dir: Path = Path(storage_dir)
        ensure_private_directory(self.storage_dir)
        self.log_path: Path = self.storage_dir / f"{self.workflow_id}.chimera.log"
        self.lock_path: Path = self.storage_dir / f"{self.workflow_id}.chimera.lock"

        self._file_lock = WorkflowFileLock(
            self.lock_path, self.workflow_id, file_mode=self.file_mode
        )
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        if isinstance(self._clock, VirtualClock):
            self._clock.register_condition(self._cond)

        self._event_log = EventLog(
            log_path=self.log_path,
            workflow_id=self.workflow_id,
            clock=self._clock,
            crash_hook=crash_hook,
            file_lock=self._file_lock,
            partial_write_bytes=partial_write_bytes,
            max_payload_bytes=self.max_payload_bytes,
            max_frame_bytes=self.max_frame_bytes,
            file_mode=self.file_mode,
            hmac_key=hmac_key,
            format_version=log_format_version,
        )

        self._task_specs: dict[str, TaskSpec] = {}
        self._dependents: dict[str, list[str]] = {}
        self._task_states: dict[str, TaskRuntimeState] = {}
        self._scheduler = DeterministicReadyScheduler()
        self._definition_hash: str = ""

        self._global_claim_epoch: int = 0
        self._active_cancel_events: dict[tuple[str, int], threading.Event] = {}
        self._process_executor: ProcessTaskExecutor | None = None

        self._validated: bool = False
        self._initialized_in_log: bool = False
        self._shut_down: bool = False
        self._status: WorkflowStatus = WorkflowStatus.CREATED
        self._cancel_requested: bool = False
        self._truncated_tail_repaired: bool = False
        self._structured_logs: list[StructuredLogEntry] = []

        # Aggregate operational counters
        self._succeeded_count: int = 0
        self._failed_count: int = 0
        self._cancelled_count: int = 0
        self._stale_completion_count: int = 0
        self._recovery_event_count: int = 0
        self._timeout_event_count: int = 0
        self._worker_crash_count: int = 0
        self._retry_scheduled_count: int = 0

        # Deterministic test hooks
        self.on_task_claimed_hook: Callable[[ClaimToken], None] | None = None
        self.before_task_commit_hook: Callable[[ClaimToken, str], None] | None = None

    def __enter__(self) -> WorkflowEngine:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown(force=True)

    @contextmanager
    def lock_workflow(self) -> Iterator[None]:
        """Acquire the exclusive cross-process and in-process workflow lock."""
        with self._file_lock.acquire():
            yield

    def _effective_backend_for_task(self, spec: TaskSpec) -> ExecutionBackend:
        if spec.execution_backend is not None:
            return (
                spec.execution_backend
                if isinstance(spec.execution_backend, ExecutionBackend)
                else ExecutionBackend(str(spec.execution_backend).upper())
            )
        return self.execution_backend

    def _get_or_create_process_executor(self) -> ProcessTaskExecutor:
        with self._lock:
            if self._process_executor is None:
                self._process_executor = ProcessTaskExecutor(
                    max_workers=self.max_concurrency,
                    default_max_output_bytes=self.max_output_bytes,
                    grace_period_seconds=self._process_grace_period_seconds,
                    terminate_wait_seconds=self._process_terminate_wait_seconds,
                    on_attempt_completed=self._on_process_attempt_completed,
                )
            return self._process_executor

    def active_process_count(self) -> int:
        """Return the number of currently alive child worker processes."""
        with self._lock:
            if self._process_executor is None:
                return 0
            return self._process_executor.active_process_count()

    # ------------------------------------------------------------------
    # Validation & Initialization
    # ------------------------------------------------------------------

    def validate(self) -> bool:
        """Validate the workflow DAG, process callables, and canonical definition hash."""
        with self._lock:
            self._task_specs = validate_dag(self._raw_tasks)
            for tid, spec in self._task_specs.items():
                if self._effective_backend_for_task(spec) == ExecutionBackend.PROCESS:
                    validate_callable_for_process_backend(spec.func, tid)
                canonical_json_bytes(
                    spec.input_payload, max_bytes=self.max_payload_bytes
                )

            self._definition_hash = compute_workflow_definition_hash(
                self._task_specs, definition_version=self.definition_version
            )
            self._dependents = {tid: [] for tid in self._task_specs}
            for tid, spec in self._task_specs.items():
                for dep_id in spec.dependencies:
                    self._dependents[dep_id].append(tid)
            for tid in self._dependents:
                self._dependents[tid].sort()

            for tid in self._task_specs:
                if tid not in self._task_states:
                    self._task_states[tid] = TaskRuntimeState(
                        task_id=tid,
                        state=TaskState.PENDING,
                        last_updated_at=self._clock.now(),
                    )
            self._validated = True
            return True

    def _ensure_initialized_in_log(self) -> None:
        """Persist `WORKFLOW_CREATED` and initial `PENDING -> READY` root transitions if not yet logged."""
        with self._file_lock.acquire(), self._lock:
            if not self._validated:
                self.validate()
            if self._initialized_in_log:
                return

            spec_metadata = {
                tid: spec.to_metadata_dict()
                for tid, spec in sorted(self._task_specs.items())
            }
            rec = self._event_log.append(
                "WORKFLOW_CREATED",
                {
                    "schema_version": 2,
                    "definition_version": self.definition_version,
                    "definition_hash": self._definition_hash,
                    "execution_backend": self.execution_backend.value,
                    "max_concurrency": self.max_concurrency,
                    "tasks": spec_metadata,
                },
            )
            self._apply_event(rec, is_recovery=False)

            self._promote_eligible_pending_tasks(reason="ROOT_TASK_ELIGIBLE")

            if not self._task_specs:
                rec_done = self._event_log.append(
                    "WORKFLOW_COMPLETED",
                    {
                        "status": WorkflowStatus.SUCCEEDED.value,
                        "reason": "EMPTY_WORKFLOW",
                    },
                )
                self._apply_event(rec_done, is_recovery=False)

    # ------------------------------------------------------------------
    # Event-Sourced State Application
    # ------------------------------------------------------------------

    def _apply_event(
        self, record: dict[str, Any], is_recovery: bool = False
    ) -> DefinitionVerificationResult | None:
        """
        Deterministically apply a single committed event log record to in-memory
        workflow state. Enforces strict structural and state-transition invariants.
        """
        seq = int(record["seq"])
        ts = float(record["timestamp"])
        etype = str(record["event_type"])
        data = record["data"]

        if etype == "WORKFLOW_CREATED":
            if seq != 1 or self._initialized_in_log:
                raise CorruptedStateTransitionError(
                    f"Duplicate or out-of-position WORKFLOW_CREATED record at seq={seq}."
                )
            self._initialized_in_log = True
            self.max_concurrency = int(
                data.get("max_concurrency", self.max_concurrency)
            )
            tasks_meta = data.get("tasks", {})
            ver_res = RecoveryManager.verify_recovered_task_specs(
                self.workflow_id,
                self._task_specs,
                data,
                seq,
                definition_version=self.definition_version,
                compatibility_policy=self.compatibility_policy,
            )
            self._definition_hash = ver_res.definition_hash

            for tid in sorted(tasks_meta.keys()):
                if tid not in self._task_states:
                    self._task_states[tid] = TaskRuntimeState(
                        task_id=tid,
                        state=TaskState.PENDING,
                        last_updated_at=ts,
                    )
            if self._status == WorkflowStatus.CREATED:
                self._status = WorkflowStatus.RUNNING
            self._structured_logs.append(
                StructuredLogEntry(
                    seq=seq,
                    timestamp=ts,
                    workflow_id=self.workflow_id,
                    event_type=etype,
                    task_id=None,
                    attempt=None,
                    claim_epoch=None,
                    from_state=None,
                    to_state=WorkflowStatus.RUNNING.value,
                    reason="WORKFLOW_INITIALIZED",
                    failure_category=FailureCategory.NONE.value,
                    details={
                        "task_count": len(tasks_meta),
                        "definition_version": ver_res.definition_version,
                        "definition_hash": ver_res.definition_hash,
                    },
                )
            )
            return ver_res

        if not self._initialized_in_log:
            raise CorruptedStateTransitionError(
                f"Event '{etype}' at seq={seq} appeared before WORKFLOW_CREATED."
            )

        if etype == "DEFINITION_MIGRATED":
            self.definition_version = int(
                data.get("to_definition_version", self.definition_version)
            )
            self._definition_hash = str(
                data.get("to_definition_hash", self._definition_hash)
            )
            self._structured_logs.append(
                StructuredLogEntry(
                    seq=seq,
                    timestamp=ts,
                    workflow_id=self.workflow_id,
                    event_type=etype,
                    task_id=None,
                    attempt=None,
                    claim_epoch=None,
                    from_state=self._status.value,
                    to_state=self._status.value,
                    reason=str(data.get("reason", "DEFINITION_MIGRATED")),
                    failure_category=FailureCategory.NONE.value,
                    details=copy.deepcopy(data),
                )
            )
            return None

        if etype == "WORKFLOW_CANCEL_REQUESTED":
            self._cancel_requested = True
            reason = str(data.get("reason", "USER_CANCEL_REQUESTED"))
            self._structured_logs.append(
                StructuredLogEntry(
                    seq=seq,
                    timestamp=ts,
                    workflow_id=self.workflow_id,
                    event_type=etype,
                    task_id=None,
                    attempt=None,
                    claim_epoch=None,
                    from_state=self._status.value,
                    to_state=WorkflowStatus.CANCELLED.value,
                    reason=reason,
                    failure_category=FailureCategory.CANCELLED.value,
                )
            )
            return None

        if etype == "WORKFLOW_COMPLETED":
            try:
                new_status = WorkflowStatus(str(data["status"]))
            except (KeyError, ValueError) as exc:
                raise CorruptedStateTransitionError(
                    f"Invalid workflow status in WORKFLOW_COMPLETED at seq={seq}: {exc}"
                ) from exc
            reason = str(data.get("reason", "ALL_TASKS_TERMINAL"))
            old_status = self._status.value
            self._status = new_status
            self._structured_logs.append(
                StructuredLogEntry(
                    seq=seq,
                    timestamp=ts,
                    workflow_id=self.workflow_id,
                    event_type=etype,
                    task_id=None,
                    attempt=None,
                    claim_epoch=None,
                    from_state=old_status,
                    to_state=new_status.value,
                    reason=reason,
                    failure_category=FailureCategory.NONE.value,
                )
            )
            return None

        if etype == "STALE_COMPLETION_IGNORED":
            tid = str(data.get("task_id", ""))
            if tid not in self._task_states:
                raise CorruptedStateTransitionError(
                    f"Unknown task_id '{tid}' in STALE_COMPLETION_IGNORED at seq={seq}."
                )
            self._stale_completion_count += 1
            self._metrics_sink.inc_counter(
                "chimera.task.stale_completion",
                1,
                {"event_type": etype, "task_id": tid},
            )
            self._structured_logs.append(
                StructuredLogEntry(
                    seq=seq,
                    timestamp=ts,
                    workflow_id=self.workflow_id,
                    event_type=etype,
                    task_id=tid,
                    attempt=data.get("worker_attempt"),
                    claim_epoch=data.get("worker_claim_epoch"),
                    from_state=data.get("current_state"),
                    to_state=data.get("current_state"),
                    reason=str(data.get("reason", "STALE_WORKER_TOKEN")),
                    failure_category=FailureCategory.NONE.value,
                    details=copy.deepcopy(data),
                )
            )
            return None

        if etype == "TASK_STATE_CHANGED":
            if self._status in (
                WorkflowStatus.SUCCEEDED,
                WorkflowStatus.FAILED,
                WorkflowStatus.CANCELLED,
            ):
                raise CorruptedStateTransitionError(
                    f"Task state transition at seq={seq} occurred after workflow already reached terminal status {self._status.value}."
                )

            tid = str(data.get("task_id", ""))
            if tid not in self._task_states:
                raise CorruptedStateTransitionError(
                    f"Unknown task_id '{tid}' in TASK_STATE_CHANGED at seq={seq}."
                )

            try:
                from_state = TaskState(str(data["from_state"]))
                to_state = TaskState(str(data["to_state"]))
            except (KeyError, ValueError) as exc:
                raise CorruptedStateTransitionError(
                    f"Invalid TaskState in TASK_STATE_CHANGED at seq={seq}: {exc}"
                ) from exc

            reason = str(data.get("reason", "STATE_TRANSITION"))
            rt = self._task_states[tid]

            allowed_map = (
                RECOVERY_ALLOWED_TRANSITIONS
                if (is_recovery and reason == "RECOVERED_FROM_CRASH")
                else LIVE_ALLOWED_TRANSITIONS
            )
            if rt.state != from_state:
                raise CorruptedStateTransitionError(
                    f"State mismatch for task '{tid}' at seq={seq}: expected {from_state}, actual {rt.state}"
                )
            if to_state not in allowed_map[from_state]:
                raise CorruptedStateTransitionError(
                    f"Illegal state transition for task '{tid}' at seq={seq}: "
                    f"{from_state.value} -> {to_state.value} (reason={reason})"
                )

            if (
                to_state in (TaskState.READY, TaskState.RUNNING)
                and tid in self._task_specs
            ):
                for dep_id in self._task_specs[tid].dependencies:
                    dep_state = self._task_states[dep_id].state
                    if dep_state != TaskState.SUCCEEDED:
                        raise CorruptedStateTransitionError(
                            f"Task '{tid}' cannot enter {to_state.value} at seq={seq} while dependency "
                            f"'{dep_id}' is in state {dep_state.value}."
                        )

            if "attempt" in data and data["attempt"] is not None:
                attempt_val = int(data["attempt"])
                if attempt_val < 0:
                    raise CorruptedStateTransitionError(
                        f"Negative attempt number {attempt_val} for task '{tid}' at seq={seq}."
                    )
                rt.attempt = attempt_val

            if "claim_epoch" in data and data["claim_epoch"] is not None:
                epoch_val = int(data["claim_epoch"])
                if epoch_val < rt.claim_epoch:
                    raise CorruptedStateTransitionError(
                        f"Non-monotonic claim_epoch for task '{tid}' at seq={seq}: "
                        f"{epoch_val} < {rt.claim_epoch}."
                    )
                rt.claim_epoch = epoch_val
                if epoch_val > self._global_claim_epoch:
                    self._global_claim_epoch = epoch_val

            if "recovery_count" in data and data["recovery_count"] is not None:
                rec_cnt = int(data["recovery_count"])
                if rec_cnt < rt.recovery_count:
                    raise CorruptedStateTransitionError(
                        f"Decreasing recovery_count for task '{tid}' at seq={seq}."
                    )
                rt.recovery_count = rec_cnt

            cat_str = str(
                data.get("failure_category", FailureCategory.NONE.value)
            )
            try:
                failure_cat = FailureCategory(cat_str)
            except ValueError:
                failure_cat = FailureCategory.NONE

            rt.state = to_state
            rt.last_transition_reason = reason
            rt.last_updated_at = ts
            rt.failure_category = failure_cat

            if "worker_pid" in data and data["worker_pid"] is not None:
                rt.worker_pid = int(data["worker_pid"])
            if "exit_code" in data and data["exit_code"] is not None:
                rt.exit_code = int(data["exit_code"])

            if reason == "RECOVERED_FROM_CRASH":
                self._recovery_event_count += 1
                self._metrics_sink.inc_counter(
                    "chimera.workflow.recovered", 1, {"task_id": tid}
                )
            if "TIMEOUT" in reason or failure_cat == FailureCategory.TIMEOUT:
                rt.timeout_count += 1
                self._timeout_event_count += 1
                self._metrics_sink.inc_counter(
                    "chimera.task.timed_out", 1, {"task_id": tid}
                )
            if failure_cat == FailureCategory.PROCESS_CRASH:
                self._worker_crash_count += 1
                self._metrics_sink.inc_counter(
                    "chimera.worker.crash", 1, {"task_id": tid}
                )

            if to_state == TaskState.READY:
                rt.ready_at = ts
                rt.started_at = None
                rt.deadline_at = None
                rt.wake_at = None
                rt.worker_id = None
                prio = (
                    self._task_specs[tid].priority
                    if tid in self._task_specs
                    else 0
                )
                self._scheduler.push(tid, prio)
            else:
                self._scheduler.discard(tid)

            if to_state == TaskState.RUNNING:
                started_val = float(data.get("started_at", ts))
                if rt.ready_at is not None:
                    q_delay = max(0.0, started_val - rt.ready_at)
                    rt.queue_delay_seconds = q_delay
                    self._metrics_sink.observe_duration(
                        "chimera.task.queue_delay_seconds",
                        q_delay,
                        {"task_id": tid},
                    )
                rt.started_at = started_val
                rt.deadline_at = data.get("deadline_at")
                rt.worker_id = data.get("worker_id")
                rt.wake_at = None
            elif to_state == TaskState.RETRY_WAIT:
                if rt.started_at is not None:
                    exec_dur = max(0.0, ts - rt.started_at)
                    rt.execution_duration_seconds = exec_dur
                    self._metrics_sink.observe_duration(
                        "chimera.task.execution_duration_seconds",
                        exec_dur,
                        {"task_id": tid, "to_state": to_state.value},
                    )
                rt.retry_count += 1
                self._retry_scheduled_count += 1
                self._metrics_sink.inc_counter(
                    "chimera.task.retried",
                    1,
                    {"task_id": tid, "failure_category": failure_cat.value},
                )
                rt.started_at = None
                rt.deadline_at = None
                rt.worker_id = None
                rt.wake_at = data.get("wake_at")
                rt.error_type = data.get("error_type")
                rt.error_message = data.get("error_message")
            elif to_state == TaskState.SUCCEEDED:
                if rt.started_at is not None:
                    exec_dur = max(0.0, ts - rt.started_at)
                    rt.execution_duration_seconds = exec_dur
                    self._metrics_sink.observe_duration(
                        "chimera.task.execution_duration_seconds",
                        exec_dur,
                        {"task_id": tid, "to_state": to_state.value},
                    )
                rt.completed_at = ts
                rt.started_at = None
                rt.deadline_at = None
                rt.wake_at = None
                rt.result = copy.deepcopy(data.get("result"))
                rt.has_result = True
                rt.error_type = None
                rt.error_message = None
                rt.failure_category = FailureCategory.NONE
                self._succeeded_count += 1
                self._metrics_sink.inc_counter(
                    "chimera.task.succeeded", 1, {"task_id": tid}
                )
            elif to_state == TaskState.FAILED:
                if rt.started_at is not None:
                    exec_dur = max(0.0, ts - rt.started_at)
                    rt.execution_duration_seconds = exec_dur
                    self._metrics_sink.observe_duration(
                        "chimera.task.execution_duration_seconds",
                        exec_dur,
                        {"task_id": tid, "to_state": to_state.value},
                    )
                rt.completed_at = ts
                rt.started_at = None
                rt.deadline_at = None
                rt.wake_at = None
                rt.error_type = data.get("error_type")
                rt.error_message = data.get("error_message")
                self._failed_count += 1
                self._metrics_sink.inc_counter(
                    "chimera.task.failed",
                    1,
                    {"task_id": tid, "failure_category": failure_cat.value},
                )
            elif to_state == TaskState.CANCELLED:
                if rt.started_at is not None:
                    exec_dur = max(0.0, ts - rt.started_at)
                    rt.execution_duration_seconds = exec_dur
                rt.completed_at = ts
                rt.started_at = None
                rt.deadline_at = None
                rt.wake_at = None
                if data.get("error_type"):
                    rt.error_type = data.get("error_type")
                if data.get("error_message"):
                    rt.error_message = data.get("error_message")
                self._cancelled_count += 1
                self._metrics_sink.inc_counter(
                    "chimera.task.cancelled", 1, {"task_id": tid}
                )

            log_details: dict[str, Any] = {}
            for k, v in data.items():
                if k in {
                    "task_id",
                    "from_state",
                    "to_state",
                    "reason",
                    "attempt",
                    "claim_epoch",
                    "failure_category",
                }:
                    continue
                if k == "result" and not self.log_sensitive_payloads:
                    log_details["has_result"] = True
                    log_details["result_type"] = type(v).__name__
                else:
                    log_details[k] = copy.deepcopy(v)

            if rt.queue_delay_seconds is not None:
                log_details.setdefault(
                    "queue_delay_seconds", rt.queue_delay_seconds
                )
            if rt.execution_duration_seconds is not None and to_state in (
                TaskState.SUCCEEDED,
                TaskState.FAILED,
                TaskState.RETRY_WAIT,
            ):
                log_details.setdefault(
                    "execution_duration_seconds",
                    rt.execution_duration_seconds,
                )

            self._structured_logs.append(
                StructuredLogEntry(
                    seq=seq,
                    timestamp=ts,
                    workflow_id=self.workflow_id,
                    event_type=etype,
                    task_id=tid,
                    attempt=rt.attempt,
                    claim_epoch=rt.claim_epoch,
                    from_state=from_state.value,
                    to_state=to_state.value,
                    reason=reason,
                    failure_category=failure_cat.value,
                    details=log_details,
                )
            )
            return None

        raise LogCorruptionError(f"Unknown event_type '{etype}' at seq={seq}.")

    # ------------------------------------------------------------------
    # Internal State Transitions (Protected by self._lock)
    # ------------------------------------------------------------------

    def _record_task_transition(
        self,
        task_id: str,
        to_state: TaskState,
        reason: str,
        *,
        attempt: int | None = None,
        claim_epoch: int | None = None,
        recovery_count: int | None = None,
        started_at: float | None = None,
        deadline_at: float | None = None,
        wake_at: float | None = None,
        worker_id: str | None = None,
        worker_pid: int | None = None,
        exit_code: int | None = None,
        result: Any = None,
        include_result: bool = False,
        error_type: str | None = None,
        error_message: str | None = None,
        failure_category: FailureCategory = FailureCategory.NONE,
    ) -> None:
        """Persist a task state transition to EventLog and apply it atomically."""
        rt = self._task_states[task_id]
        spec = self._task_specs.get(task_id)
        max_out = (
            spec.max_output_bytes
            if (spec is not None and spec.max_output_bytes is not None)
            else self.max_output_bytes
        )
        payload: dict[str, Any] = {
            "task_id": task_id,
            "from_state": rt.state.value,
            "to_state": to_state.value,
            "reason": reason,
            "attempt": rt.attempt if attempt is None else attempt,
            "claim_epoch": rt.claim_epoch if claim_epoch is None else claim_epoch,
            "recovery_count": (
                rt.recovery_count if recovery_count is None else recovery_count
            ),
        }
        if failure_category != FailureCategory.NONE:
            payload["failure_category"] = failure_category.value
        if started_at is not None:
            payload["started_at"] = started_at
        if deadline_at is not None:
            payload["deadline_at"] = deadline_at
        if wake_at is not None:
            payload["wake_at"] = wake_at
        if worker_id is not None:
            payload["worker_id"] = worker_id
        if worker_pid is not None:
            payload["worker_pid"] = worker_pid
        if exit_code is not None:
            payload["exit_code"] = exit_code
        if include_result:
            # Strict lossless canonical JSON validation (raises PayloadSerializationError / TaskOutputLimitExceededError)
            encoded_res = canonical_json_bytes(
                result, max_bytes=max_out, is_task_output=True
            )
            payload["result"] = canonical_json_loads(encoded_res, max_bytes=None)
        if error_type is not None:
            payload["error_type"] = error_type
        if error_message is not None:
            payload["error_message"] = error_message

        rec = self._event_log.append("TASK_STATE_CHANGED", payload)
        self._apply_event(
            rec, is_recovery=(reason == "RECOVERED_FROM_CRASH")
        )

    def _promote_eligible_pending_tasks(self, reason: str) -> list[str]:
        if self._cancel_requested:
            return []
        eligible = DeterministicReadyScheduler.find_eligible_pending_tasks(
            self._task_states, self._task_specs
        )
        for tid in eligible:
            self._record_task_transition(tid, TaskState.READY, reason=reason)
        return eligible

    def _cascade_dependency_failure(self, failed_task_id: str) -> list[str]:
        affected = DeterministicReadyScheduler.find_downstream_dependents(
            failed_task_id, self._dependents
        )
        cancelled_ids: list[str] = []
        for tid in affected:
            rt = self._task_states[tid]
            if rt.state in (
                TaskState.PENDING,
                TaskState.READY,
                TaskState.RETRY_WAIT,
            ):
                self._global_claim_epoch += 1
                self._record_task_transition(
                    tid,
                    TaskState.CANCELLED,
                    reason=f"DEPENDENCY_FAILED:{failed_task_id}",
                    claim_epoch=self._global_claim_epoch,
                    error_type="DependencyFailedError",
                    error_message=f"Upstream dependency '{failed_task_id}' failed.",
                    failure_category=FailureCategory.DEPENDENCY_FAILED,
                )
                cancelled_ids.append(tid)
        return cancelled_ids

    def _check_and_finalize_workflow(self) -> bool:
        if not self._task_states:
            return self._status in (
                WorkflowStatus.SUCCEEDED,
                WorkflowStatus.FAILED,
                WorkflowStatus.CANCELLED,
            )

        if any(
            rt.state not in TERMINAL_TASK_STATES
            for rt in self._task_states.values()
        ):
            return False

        if self._status in (
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        ):
            return True

        states = {rt.state for rt in self._task_states.values()}
        if self._cancel_requested and TaskState.CANCELLED in states:
            final_status = WorkflowStatus.CANCELLED
            reason = "WORKFLOW_CANCELLED"
        elif TaskState.FAILED in states:
            final_status = WorkflowStatus.FAILED
            reason = "ONE_OR_MORE_TASKS_FAILED"
        elif TaskState.CANCELLED in states:
            final_status = WorkflowStatus.CANCELLED
            reason = "TASKS_CANCELLED"
        else:
            final_status = WorkflowStatus.SUCCEEDED
            reason = "ALL_TASKS_SUCCEEDED"

        rec = self._event_log.append(
            "WORKFLOW_COMPLETED",
            {"status": final_status.value, "reason": reason},
        )
        self._apply_event(rec, is_recovery=False)
        self._cond.notify_all()
        return True

    # ------------------------------------------------------------------
    # Worker Claiming, Completion & Watchdog Enforcement
    # ------------------------------------------------------------------

    def _active_running_count(self) -> int:
        return sum(
            1
            for rt in self._task_states.values()
            if rt.state == TaskState.RUNNING
        )

    def try_claim_specific_task(
        self, task_id: str, worker_id: str
    ) -> tuple[ClaimToken, TaskSpec, Any, TaskContext] | None:
        """Atomically attempt to claim a specific `READY` task for `worker_id`."""
        with self._file_lock.acquire(), self._cond:
            if self._cancel_requested or self._shut_down:
                return None
            if self._active_running_count() >= self.max_concurrency:
                return None
            rt = self._task_states.get(task_id)
            if rt is None or rt.state != TaskState.READY:
                return None

            return self._execute_claim_locked(task_id, worker_id)

    def claim_next_ready_task(
        self, worker_id: str
    ) -> tuple[ClaimToken, TaskSpec, Any, TaskContext] | None:
        """Atomically pop and claim the next deterministic `READY` task if concurrency allows."""
        with self._file_lock.acquire(), self._cond:
            if self._cancel_requested or self._shut_down:
                return None
            if self._active_running_count() >= self.max_concurrency:
                return None

            candidate_id = self._scheduler.pop_next_ready(self._task_states)
            if candidate_id is None:
                return None
            return self._execute_claim_locked(candidate_id, worker_id)

    def _execute_claim_locked(
        self, task_id: str, worker_id: str
    ) -> tuple[ClaimToken, TaskSpec, Any, TaskContext]:
        rt = self._task_states[task_id]
        spec = self._task_specs[task_id]

        for dep_id in spec.dependencies:
            if self._task_states[dep_id].state != TaskState.SUCCEEDED:
                raise InvalidStateTransitionError(
                    f"Cannot claim task '{task_id}': dependency '{dep_id}' is not SUCCEEDED."
                )

        new_attempt = rt.attempt + 1
        self._global_claim_epoch += 1
        new_epoch = self._global_claim_epoch
        now = self._clock.now()
        deadline = (
            now + spec.timeout_seconds
            if spec.timeout_seconds is not None
            else None
        )

        self._record_task_transition(
            task_id,
            TaskState.RUNNING,
            reason="CLAIMED_BY_WORKER",
            attempt=new_attempt,
            claim_epoch=new_epoch,
            started_at=now,
            deadline_at=deadline,
            worker_id=worker_id,
        )

        token = ClaimToken(
            workflow_id=self.workflow_id,
            task_id=task_id,
            attempt=new_attempt,
            claim_epoch=new_epoch,
            worker_id=worker_id,
        )
        cancel_event = threading.Event()
        self._active_cancel_events[(task_id, new_epoch)] = cancel_event

        isolated_payload = copy.deepcopy(spec.input_payload)
        dep_results = {
            dep_id: copy.deepcopy(self._task_states[dep_id].result)
            for dep_id in spec.dependencies
        }
        idem_key = spec.resolve_idempotency_key(self.workflow_id)
        ctx = TaskContext(
            workflow_id=self.workflow_id,
            task_id=task_id,
            attempt=new_attempt,
            claim_epoch=new_epoch,
            idempotency_key=idem_key,
            dependency_results=dep_results,
            _cancel_event=cancel_event,
            operation_key=spec.operation_key or idem_key,
            attempt_idempotency_key=f"{idem_key}:attempt:{new_attempt}:epoch:{new_epoch}",
        )
        return token, spec, isolated_payload, ctx

    def complete_task_attempt(
        self,
        token: ClaimToken,
        *,
        succeeded: bool,
        result: Any = None,
        exception: BaseException | None = None,
        failure_category: FailureCategory | None = None,
        worker_pid: int | None = None,
        exit_code: int | None = None,
    ) -> bool:
        """Atomically commit the outcome of a worker's task execution attempt."""
        if self.before_task_commit_hook is not None:
            self.before_task_commit_hook(
                token, "SUCCEEDED" if succeeded else "FAILED"
            )

        with self._file_lock.acquire(), self._cond:
            return self._complete_task_attempt_locked(
                token,
                succeeded=succeeded,
                result=result,
                exception=exception,
                failure_category=failure_category,
                worker_pid=worker_pid,
                exit_code=exit_code,
            )

    def _complete_task_attempt_locked(
        self,
        token: ClaimToken,
        *,
        succeeded: bool,
        result: Any = None,
        exception: BaseException | None = None,
        failure_category: FailureCategory | None = None,
        worker_pid: int | None = None,
        exit_code: int | None = None,
    ) -> bool:
        rt = self._task_states.get(token.task_id)
        if rt is None:
            return False

        if (
            self._cancel_requested
            or rt.state != TaskState.RUNNING
            or rt.claim_epoch != token.claim_epoch
            or rt.attempt != token.attempt
        ):
            rec = self._event_log.append(
                "STALE_COMPLETION_IGNORED",
                {
                    "task_id": token.task_id,
                    "worker_id": token.worker_id,
                    "worker_attempt": token.attempt,
                    "worker_claim_epoch": token.claim_epoch,
                    "current_state": rt.state.value,
                    "current_attempt": rt.attempt,
                    "current_claim_epoch": rt.claim_epoch,
                    "attempted_outcome": "SUCCEEDED" if succeeded else "FAILED",
                    "reason": f"FENCING_TOKEN_MISMATCH_OR_STATE_{rt.state.value}",
                },
            )
            self._apply_event(rec, is_recovery=False)
            self._cond.notify_all()
            return False

        self._active_cancel_events.pop(
            (token.task_id, token.claim_epoch), None
        )
        spec = self._task_specs[token.task_id]

        if succeeded:
            try:
                self._record_task_transition(
                    token.task_id,
                    TaskState.SUCCEEDED,
                    reason="EXECUTION_SUCCEEDED",
                    result=result,
                    include_result=True,
                    worker_pid=worker_pid,
                    exit_code=exit_code,
                    failure_category=FailureCategory.NONE,
                )
                self._promote_eligible_pending_tasks(
                    reason=f"DEPENDENCY_SUCCEEDED:{token.task_id}"
                )
                self._check_and_finalize_workflow()
                self._cond.notify_all()
                return True
            except (
                PayloadSerializationError,
                OversizedPayloadError,
                TaskOutputLimitExceededError,
            ) as ser_exc:
                # Output violates the durable serialization contract -> treat as non-retryable serialization failure
                exception = ser_exc
                succeeded = False
                failure_category = FailureCategory.SERIALIZATION_ERROR

        assert exception is not None
        err_type = type(exception).__name__
        err_msg = str(exception)

        if failure_category is None:
            if isinstance(exception, ProcessWorkerCrashError):
                failure_category = FailureCategory.PROCESS_CRASH
            elif isinstance(
                exception,
                (
                    PayloadSerializationError,
                    OversizedPayloadError,
                    TaskOutputLimitExceededError,
                ),
            ):
                failure_category = FailureCategory.SERIALIZATION_ERROR
            else:
                failure_category = FailureCategory.APPLICATION_ERROR

        # Serialization errors are deterministic contract violations and never retried unless explicitly listed
        if isinstance(
            exception,
            (
                PayloadSerializationError,
                OversizedPayloadError,
                TaskOutputLimitExceededError,
            ),
        ) and not any(
            issubclass(cls, PayloadSerializationError)
            for cls in spec.retryable_exceptions
            if cls is not Exception and cls is not BaseException
        ):
            is_retryable = False
        else:
            is_retryable = isinstance(exception, spec.retryable_exceptions)

        retries_used = rt.attempt - 1

        if (
            not self._cancel_requested
            and is_retryable
            and retries_used < spec.max_retries
        ):
            backoff = spec.compute_backoff(
                rt.attempt, workflow_id=self.workflow_id
            )
            wake_at = self._clock.now() + backoff
            self._record_task_transition(
                token.task_id,
                TaskState.RETRY_WAIT,
                reason=f"RETRYABLE_ERROR:{err_type}",
                wake_at=wake_at,
                worker_pid=worker_pid,
                exit_code=exit_code,
                error_type=err_type,
                error_message=err_msg,
                failure_category=failure_category,
            )
        else:
            reason = (
                f"RETRIES_EXHAUSTED:{err_type}"
                if is_retryable and retries_used >= spec.max_retries
                else f"NON_RETRYABLE_ERROR:{err_type}"
            )
            self._record_task_transition(
                token.task_id,
                TaskState.FAILED,
                reason=reason,
                worker_pid=worker_pid,
                exit_code=exit_code,
                error_type=err_type,
                error_message=err_msg,
                failure_category=failure_category,
            )
            self._cascade_dependency_failure(token.task_id)
            self._check_and_finalize_workflow()

        self._cond.notify_all()
        return True

    def _on_process_attempt_completed(
        self, outcome: TaskAttemptResult
    ) -> None:
        """Callback invoked by ProcessTaskExecutor monitor thread when a child process finishes."""
        try:
            self.complete_task_attempt(
                outcome.token,
                succeeded=outcome.succeeded,
                result=outcome.result,
                exception=outcome.exception,
                failure_category=outcome.failure_category,
                worker_pid=outcome.worker_pid,
                exit_code=outcome.exit_code,
            )
        except BaseException:
            with self._cond:
                self._cond.notify_all()

    def _enforce_timeouts_and_timers_locked(self) -> tuple[int, float | None]:
        now = self._clock.now()
        timed_out_count = 0
        next_wake: float | None = None
        processes_to_terminate: list[tuple[str, int]] = []

        for tid in sorted(self._task_states.keys()):
            rt = self._task_states[tid]
            if rt.state != TaskState.RUNNING or rt.deadline_at is None:
                continue
            if now >= rt.deadline_at:
                timed_out_count += 1
                old_epoch = rt.claim_epoch
                ev = self._active_cancel_events.pop((tid, old_epoch), None)
                if ev is not None:
                    ev.set()

                spec = self._task_specs[tid]
                # Step 1: Durably fence the expired attempt FIRST before terminating
                # the process or allowing any replacement attempt to start.
                self._global_claim_epoch += 1
                new_epoch = self._global_claim_epoch
                retries_used = rt.attempt - 1
                err_msg = (
                    f"Task '{tid}' attempt {rt.attempt} timed out after "
                    f"{spec.timeout_seconds}s."
                )

                if (
                    not self._cancel_requested
                    and spec.retry_on_timeout
                    and retries_used < spec.max_retries
                ):
                    backoff = spec.compute_backoff(
                        rt.attempt, workflow_id=self.workflow_id
                    )
                    wake_at = now + backoff
                    self._record_task_transition(
                        tid,
                        TaskState.RETRY_WAIT,
                        reason="TASK_TIMEOUT_RETRY",
                        claim_epoch=new_epoch,
                        wake_at=wake_at,
                        error_type="TaskTimeoutError",
                        error_message=err_msg,
                        failure_category=FailureCategory.TIMEOUT,
                    )
                else:
                    self._record_task_transition(
                        tid,
                        TaskState.FAILED,
                        reason="TASK_TIMEOUT",
                        claim_epoch=new_epoch,
                        error_type="TaskTimeoutError",
                        error_message=err_msg,
                        failure_category=FailureCategory.TIMEOUT,
                    )
                    self._cascade_dependency_failure(tid)

                if self._effective_backend_for_task(spec) == ExecutionBackend.PROCESS:
                    processes_to_terminate.append((tid, old_epoch))
            else:
                rem = rt.deadline_at - now
                if next_wake is None or rem < next_wake:
                    next_wake = rem

        for tid in sorted(self._task_states.keys()):
            rt = self._task_states[tid]
            if rt.state != TaskState.RETRY_WAIT:
                continue
            if self._cancel_requested:
                self._global_claim_epoch += 1
                self._record_task_transition(
                    tid,
                    TaskState.CANCELLED,
                    reason="WORKFLOW_CANCELLED",
                    claim_epoch=self._global_claim_epoch,
                    failure_category=FailureCategory.CANCELLED,
                )
                continue
            wake_at = rt.wake_at if rt.wake_at is not None else now
            if now >= wake_at:
                self._record_task_transition(
                    tid,
                    TaskState.READY,
                    reason="RETRY_BACKOFF_ELAPSED",
                )
            else:
                rem = wake_at - now
                if next_wake is None or rem < next_wake:
                    next_wake = rem

        self._check_and_finalize_workflow()

        # Step 2: Terminate and reap timed-out child processes
        if processes_to_terminate and self._process_executor is not None:
            for tid, old_epoch in processes_to_terminate:
                self._process_executor.terminate_attempt(tid, old_epoch)

        return timed_out_count, next_wake

    # ------------------------------------------------------------------
    # Cancellation & Shutdown
    # ------------------------------------------------------------------

    def cancel(
        self, reason: str = "USER_CANCEL_REQUESTED"
    ) -> WorkflowStatusSummary:
        """
        Request workflow cancellation without overwriting a completed workflow's
        terminal status. Terminates any active child worker processes.
        """
        processes_to_terminate: list[tuple[str, int]] = []
        with self._file_lock.acquire(), self._cond:
            if not self._initialized_in_log:
                self._ensure_initialized_in_log()

            if self._check_and_finalize_workflow():
                return self.get_status()

            if not self._cancel_requested:
                rec = self._event_log.append(
                    "WORKFLOW_CANCEL_REQUESTED", {"reason": reason}
                )
                self._apply_event(rec, is_recovery=False)

            for (tid, epoch), ev in list(self._active_cancel_events.items()):
                ev.set()
                processes_to_terminate.append((tid, epoch))
            self._active_cancel_events.clear()

            for tid in sorted(self._task_states.keys()):
                rt = self._task_states[tid]
                if rt.state not in TERMINAL_TASK_STATES:
                    self._global_claim_epoch += 1
                    self._record_task_transition(
                        tid,
                        TaskState.CANCELLED,
                        reason=reason,
                        claim_epoch=self._global_claim_epoch,
                        failure_category=FailureCategory.CANCELLED,
                    )

            self._check_and_finalize_workflow()
            self._cond.notify_all()

        if processes_to_terminate and self._process_executor is not None:
            for tid, epoch in processes_to_terminate:
                self._process_executor.terminate_attempt(tid, epoch)

        return self.get_status()

    def shutdown(
        self,
        *,
        grace_period_seconds: float = 0.05,
        force: bool = True,
    ) -> None:
        """
        Cleanly shut down execution backends, signaling active workers and
        reaping any child worker processes so unresponsive children never block
        process exit.
        """
        with self._cond:
            self._shut_down = True
            for (_tid, _epoch), ev in list(self._active_cancel_events.items()):
                ev.set()
            proc_exec = self._process_executor
            self._cond.notify_all()

        if proc_exec is not None:
            proc_exec.shutdown(
                grace_period_seconds=grace_period_seconds, force=force
            )

    # ------------------------------------------------------------------
    # Execution Loop
    # ------------------------------------------------------------------

    def run(self) -> WorkflowStatusSummary:
        """
        Execute the workflow until all tasks reach terminal states while holding
        the exclusive workflow file lock.
        """
        with self._file_lock.acquire():
            with self._cond:
                if self._shut_down:
                    raise WorkflowShutdownError(
                        f"WorkflowEngine '{self.workflow_id}' has been shut down."
                    )
                self._ensure_initialized_in_log()
                if self._check_and_finalize_workflow():
                    return self.get_status()

            worker_seq = 0
            active_threads: list[threading.Thread] = []

            def _worker_entry(
                initial_claim: tuple[ClaimToken, TaskSpec, Any, TaskContext]
            ) -> None:
                claim: tuple[ClaimToken, TaskSpec, Any, TaskContext] | None = (
                    initial_claim
                )
                while claim is not None:
                    token, spec, payload, ctx = claim
                    if self.on_task_claimed_hook is not None:
                        self.on_task_claimed_hook(token)

                    task_exc: BaseException | None = None
                    output: Any = None
                    try:
                        output = invoke_task_callable(spec.func, payload, ctx)
                    except SimulatedProcessCrash:
                        raise
                    except BaseException as exc:
                        task_exc = exc

                    if (
                        self.complete_task_attempt
                        != WorkflowEngine.complete_task_attempt.__get__(
                            self, WorkflowEngine
                        )
                    ):
                        if task_exc is not None:
                            committed = self.complete_task_attempt(
                                token, succeeded=False, exception=task_exc
                            )
                        else:
                            committed = self.complete_task_attempt(
                                token, succeeded=True, result=output
                            )
                        if not committed:
                            break
                        next_claim = self.claim_next_ready_task(token.worker_id)
                        if next_claim is not None and self._effective_backend_for_task(next_claim[1]) == ExecutionBackend.PROCESS:
                            # Dispatch process task via main loop / process executor
                            self._dispatch_process_claim(next_claim)
                            break
                        claim = next_claim
                    else:
                        if self.before_task_commit_hook is not None:
                            self.before_task_commit_hook(
                                token,
                                "FAILED" if task_exc is not None else "SUCCEEDED",
                            )
                        with self._cond:
                            committed = self._complete_task_attempt_locked(
                                token,
                                succeeded=(task_exc is None),
                                result=output,
                                exception=task_exc,
                            )
                            if not committed:
                                break
                            next_claim = self.claim_next_ready_task(token.worker_id)
                            if next_claim is not None and self._effective_backend_for_task(next_claim[1]) == ExecutionBackend.PROCESS:
                                self._dispatch_process_claim(next_claim)
                                break
                            claim = next_claim

            crash_exc: BaseException | None = None
            worker_crash_lock = threading.Lock()

            def _guarded_worker(
                initial_claim: tuple[ClaimToken, TaskSpec, Any, TaskContext]
            ) -> None:
                nonlocal crash_exc
                try:
                    _worker_entry(initial_claim)
                except BaseException as exc:
                    with worker_crash_lock:
                        if crash_exc is None:
                            crash_exc = exc
                    with self._cond:
                        self._cond.notify_all()

            try:
                with self._cond:
                    while True:
                        if crash_exc is not None:
                            raise crash_exc
                        if self._shut_down:
                            break

                        _, next_wait = self._enforce_timeouts_and_timers_locked()

                        if self._check_and_finalize_workflow():
                            break

                        while (
                            not self._cancel_requested
                            and not self._shut_down
                            and self._active_running_count() < self.max_concurrency
                            and self._scheduler.has_ready()
                        ):
                            worker_seq += 1
                            worker_id = f"worker-{worker_seq}"
                            claim = self.claim_next_ready_task(worker_id)
                            if claim is None:
                                break
                            token, spec, _payload, _ctx = claim
                            if self._effective_backend_for_task(spec) == ExecutionBackend.PROCESS:
                                self._dispatch_process_claim(claim)
                            else:
                                t = threading.Thread(
                                    target=_guarded_worker,
                                    args=(claim,),
                                    name=f"{self.workflow_id}-{worker_id}",
                                    daemon=True,
                                )
                                active_threads.append(t)
                                t.start()

                        if self._check_and_finalize_workflow():
                            break

                        _, next_wait = self._enforce_timeouts_and_timers_locked()
                        if self._check_and_finalize_workflow():
                            break

                        if (
                            isinstance(self._clock, VirtualClock)
                            and self._clock.auto_advance_on_wait
                            and self._active_running_count() == 0
                            and not self._scheduler.has_ready()
                            and next_wait is not None
                            and next_wait > 0
                        ):
                            self._clock.advance(next_wait)
                            continue

                        effective_wait = next_wait
                        if self._process_executor is not None and self._active_running_count() > 0:
                            effective_wait = 0.05 if effective_wait is None else min(effective_wait, 0.05)
                        self._clock.wait_condition(self._cond, timeout=effective_wait)
            finally:
                for t in active_threads:
                    t.join(timeout=0.02)
                if self._process_executor is not None and self._status in (
                    WorkflowStatus.SUCCEEDED,
                    WorkflowStatus.FAILED,
                    WorkflowStatus.CANCELLED,
                ):
                    self._process_executor.shutdown(
                        grace_period_seconds=self._process_grace_period_seconds,
                        force=True,
                    )
                    self._process_executor = None

            if crash_exc is not None:
                raise crash_exc

            return self.get_status()

    def _dispatch_process_claim(
        self, claim: tuple[ClaimToken, TaskSpec, Any, TaskContext]
    ) -> None:
        token, spec, payload, ctx = claim
        if self.on_task_claimed_hook is not None:
            self.on_task_claimed_hook(token)
        proc_exec = self._get_or_create_process_executor()
        pid = proc_exec.submit(token, spec, payload, ctx)
        rt = self._task_states.get(token.task_id)
        if rt is not None:
            rt.worker_pid = pid
            # Anchor deadline after OS process spawn completes so spawn overhead does not consume task timeout
            if spec.timeout_seconds is not None:
                rt.deadline_at = self._clock.now() + spec.timeout_seconds

    # ------------------------------------------------------------------
    # Crash Recovery & Log Replay
    # ------------------------------------------------------------------

    def recover(self, auto_run: bool = True) -> WorkflowStatusSummary:
        """
        Replay the durable event log to reconstruct workflow state after an
        interruption, repair any truncated final log record, validate task
        definitions and canonical hashes, reconcile in-flight states, and
        optionally resume execution.
        """
        with self._file_lock.acquire():
            with self._cond:
                if not self._validated:
                    self.validate()

                for (_tid, _ep), ev in list(self._active_cancel_events.items()):
                    ev.set()
                self._active_cancel_events.clear()
                if self._process_executor is not None:
                    self._process_executor.shutdown(force=True)
                    self._process_executor = None

                prior_max_epoch = self._global_claim_epoch
                self._task_states = {
                    tid: TaskRuntimeState(task_id=tid, state=TaskState.PENDING)
                    for tid in self._task_specs
                }
                self._scheduler.clear()
                self._initialized_in_log = False
                self._status = WorkflowStatus.CREATED
                self._cancel_requested = False
                self._structured_logs.clear()
                self._succeeded_count = 0
                self._failed_count = 0
                self._cancelled_count = 0
                self._stale_completion_count = 0
                self._recovery_event_count = 0
                self._timeout_event_count = 0
                self._worker_crash_count = 0
                self._retry_scheduled_count = 0

                replay_res = self._event_log.replay(repair_truncated_tail=True)
                self._truncated_tail_repaired = replay_res.truncated_tail_repaired

                ver_result = None
                for rec in replay_res.records:
                    res = self._apply_event(rec, is_recovery=True)
                    if res is not None:
                        ver_result = res

                if self._global_claim_epoch < prior_max_epoch:
                    self._global_claim_epoch = prior_max_epoch

                if (
                    ver_result is not None
                    and ver_result.migrated
                    and self._status
                    not in (
                        WorkflowStatus.SUCCEEDED,
                        WorkflowStatus.FAILED,
                        WorkflowStatus.CANCELLED,
                    )
                ):
                    mig_rec = self._event_log.append(
                        "DEFINITION_MIGRATED",
                        {
                            "to_definition_version": ver_result.definition_version,
                            "to_definition_hash": ver_result.definition_hash,
                            "policy": self.compatibility_policy.value,
                            "reason": ver_result.migration_reason
                            or "COMPATIBILITY_POLICY_MIGRATION",
                        },
                    )
                    self._apply_event(mig_rec, is_recovery=True)

                if replay_res.truncated_tail_repaired:
                    self._structured_logs.append(
                        StructuredLogEntry(
                            seq=len(replay_res.records),
                            timestamp=self._clock.now(),
                            workflow_id=self.workflow_id,
                            event_type="FINAL_RECORD_TRUNCATED",
                            task_id=None,
                            attempt=None,
                            claim_epoch=None,
                            from_state=None,
                            to_state=None,
                            reason="TRUNCATED_TAIL_REPAIRED",
                            failure_category=FailureCategory.INFRASTRUCTURE_ERROR.value,
                            details={
                                "truncated_offset": replay_res.truncated_offset,
                                "warnings": list(replay_res.warnings),
                            },
                        )
                    )

                if not self._initialized_in_log:
                    self._ensure_initialized_in_log()
                elif self._status not in (
                    WorkflowStatus.SUCCEEDED,
                    WorkflowStatus.FAILED,
                    WorkflowStatus.CANCELLED,
                ):
                    if self._cancel_requested:
                        for tid in sorted(self._task_states.keys()):
                            rt = self._task_states[tid]
                            if rt.state not in TERMINAL_TASK_STATES:
                                self._global_claim_epoch += 1
                                self._record_task_transition(
                                    tid,
                                    TaskState.CANCELLED,
                                    reason="WORKFLOW_CANCELLED_ON_RECOVERY",
                                    claim_epoch=self._global_claim_epoch,
                                    failure_category=FailureCategory.CANCELLED,
                                )
                    else:
                        for tid in sorted(self._task_states.keys()):
                            if self._task_states[tid].state == TaskState.FAILED:
                                self._cascade_dependency_failure(tid)

                        for tid in sorted(self._task_states.keys()):
                            rt = self._task_states[tid]
                            if rt.state == TaskState.RUNNING:
                                self._global_claim_epoch += 1
                                restored_attempt = max(0, rt.attempt - 1)
                                self._record_task_transition(
                                    tid,
                                    TaskState.READY,
                                    reason="RECOVERED_FROM_CRASH",
                                    attempt=restored_attempt,
                                    claim_epoch=self._global_claim_epoch,
                                    recovery_count=rt.recovery_count + 1,
                                )

                        self._promote_eligible_pending_tasks(
                            reason="ELIGIBLE_ON_RECOVERY"
                        )

                    self._scheduler.rebuild(self._task_states, self._task_specs)
                    self._check_and_finalize_workflow()

            if auto_run and self._status not in (
                WorkflowStatus.SUCCEEDED,
                WorkflowStatus.FAILED,
                WorkflowStatus.CANCELLED,
            ):
                return self.run()

            return self.get_status()

    # ------------------------------------------------------------------
    # Observability Queries
    # ------------------------------------------------------------------

    def get_status(self) -> WorkflowStatusSummary:
        """Return a thread-safe, deep-copied immutable snapshot of workflow status."""
        with self._lock:
            counts: dict[str, int] = {s.value: 0 for s in TaskState}
            task_views: dict[str, TaskResultView] = {}
            for tid, rt in sorted(self._task_states.items()):
                counts[rt.state.value] += 1
                task_views[tid] = TaskResultView(
                    workflow_id=self.workflow_id,
                    task_id=tid,
                    state=rt.state,
                    attempt=rt.attempt,
                    claim_epoch=rt.claim_epoch,
                    recovery_count=rt.recovery_count,
                    result=copy.deepcopy(rt.result),
                    has_result=rt.has_result,
                    error_type=rt.error_type,
                    error_message=rt.error_message,
                    last_transition_reason=rt.last_transition_reason,
                    last_updated_at=rt.last_updated_at,
                    retry_count=rt.retry_count,
                    timeout_count=rt.timeout_count,
                    failure_category=rt.failure_category,
                    queue_delay_seconds=rt.queue_delay_seconds,
                    execution_duration_seconds=rt.execution_duration_seconds,
                    worker_pid=rt.worker_pid,
                    exit_code=rt.exit_code,
                )

            copied_logs = [
                StructuredLogEntry(
                    seq=entry.seq,
                    timestamp=entry.timestamp,
                    workflow_id=entry.workflow_id,
                    event_type=entry.event_type,
                    task_id=entry.task_id,
                    attempt=entry.attempt,
                    claim_epoch=entry.claim_epoch,
                    from_state=entry.from_state,
                    to_state=entry.to_state,
                    reason=entry.reason,
                    failure_category=entry.failure_category,
                    details=copy.deepcopy(entry.details),
                )
                for entry in self._structured_logs
            ]

            metrics = WorkflowMetrics(
                succeeded_count=self._succeeded_count,
                failed_count=self._failed_count,
                cancelled_count=self._cancelled_count,
                stale_completion_count=self._stale_completion_count,
                recovery_event_count=self._recovery_event_count,
                timeout_event_count=self._timeout_event_count,
                worker_crash_count=self._worker_crash_count,
                retry_scheduled_count=self._retry_scheduled_count,
            )

            return WorkflowStatusSummary(
                workflow_id=self.workflow_id,
                status=self._status,
                cancel_requested=self._cancel_requested,
                total_tasks=len(self._task_states),
                counts_by_state=counts,
                tasks=task_views,
                truncated_tail_repaired=self._truncated_tail_repaired,
                log_record_count=len(copied_logs),
                structured_logs=copied_logs,
                definition_version=self.definition_version,
                definition_hash=self._definition_hash,
                execution_backend=self.execution_backend.value,
                metrics=metrics,
            )

    def get_task_result(self, task_id: str) -> TaskResultView:
        """Return a thread-safe snapshot of a single task's status and output."""
        with self._lock:
            if task_id not in self._task_states:
                raise KeyError(
                    f"Task '{task_id}' does not exist in workflow '{self.workflow_id}'."
                )
            rt = self._task_states[task_id]
            return TaskResultView(
                workflow_id=self.workflow_id,
                task_id=task_id,
                state=rt.state,
                attempt=rt.attempt,
                claim_epoch=rt.claim_epoch,
                recovery_count=rt.recovery_count,
                result=copy.deepcopy(rt.result),
                has_result=rt.has_result,
                error_type=rt.error_type,
                error_message=rt.error_message,
                last_transition_reason=rt.last_transition_reason,
                last_updated_at=rt.last_updated_at,
                retry_count=rt.retry_count,
                timeout_count=rt.timeout_count,
                failure_category=rt.failure_category,
                queue_delay_seconds=rt.queue_delay_seconds,
                execution_duration_seconds=rt.execution_duration_seconds,
                worker_pid=rt.worker_pid,
                exit_code=rt.exit_code,
            )


# ============================================================================
# Public Registry & Top-Level API Functions
# ============================================================================

_REGISTRY_LOCK = threading.RLock()
_WORKFLOW_REGISTRY: dict[str, WorkflowEngine] = {}
_WORKFLOW_SPEC_CACHE: dict[str, dict[str, Any]] = {}


def create_workflow(
    tasks: Sequence[TaskSpec],
    max_concurrency: int = 4,
    *,
    workflow_id: str | None = None,
    storage_dir: str | Path | None = None,
    clock: Clock | None = None,
    crash_hook: Callable[[str, dict[str, Any], int], None] | None = None,
    partial_write_bytes: int | None = None,
    execution_backend: ExecutionBackend | str = ExecutionBackend.THREAD,
    definition_version: int = 1,
    compatibility_policy: CompatibilityPolicy | str = CompatibilityPolicy.STRICT,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    max_output_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
    file_mode: int = DEFAULT_FILE_MODE,
    hmac_key: bytes | None = None,
    log_format_version: int | None = None,
    metrics_sink: MetricsSink | None = None,
    log_sensitive_payloads: bool = False,
) -> WorkflowEngine:
    """Create and register a new workflow instance."""
    engine = WorkflowEngine(
        tasks=tasks,
        max_concurrency=max_concurrency,
        workflow_id=workflow_id,
        storage_dir=storage_dir,
        clock=clock,
        crash_hook=crash_hook,
        partial_write_bytes=partial_write_bytes,
        execution_backend=execution_backend,
        definition_version=definition_version,
        compatibility_policy=compatibility_policy,
        max_payload_bytes=max_payload_bytes,
        max_frame_bytes=max_frame_bytes,
        max_output_bytes=max_output_bytes,
        file_mode=file_mode,
        hmac_key=hmac_key,
        log_format_version=log_format_version,
        metrics_sink=metrics_sink,
        log_sensitive_payloads=log_sensitive_payloads,
    )
    with _REGISTRY_LOCK:
        _WORKFLOW_REGISTRY[engine.workflow_id] = engine
        _WORKFLOW_SPEC_CACHE[engine.workflow_id] = {
            "tasks": list(tasks),
            "storage_dir": engine.storage_dir,
            "max_concurrency": engine.max_concurrency,
            "clock": engine._clock,
            "execution_backend": engine.execution_backend,
            "definition_version": engine.definition_version,
            "compatibility_policy": engine.compatibility_policy,
            "max_payload_bytes": engine.max_payload_bytes,
            "max_frame_bytes": engine.max_frame_bytes,
            "max_output_bytes": engine.max_output_bytes,
            "file_mode": engine.file_mode,
            "hmac_key": hmac_key,
            "metrics_sink": metrics_sink,
            "log_sensitive_payloads": log_sensitive_payloads,
        }
    return engine


def validate_workflow(workflow: WorkflowEngine | Sequence[TaskSpec]) -> bool:
    """Validate a workflow's DAG structure (duplicate IDs, missing dependencies, and cycles)."""
    if isinstance(workflow, WorkflowEngine):
        return workflow.validate()
    validate_dag(workflow)
    return True


def run_workflow(workflow: WorkflowEngine | str) -> WorkflowStatusSummary:
    """Validate, persist, and execute a workflow to completion."""
    engine = _resolve_workflow(workflow)
    return engine.run()


def cancel_workflow(
    workflow_id: str | WorkflowEngine,
    *,
    reason: str = "USER_CANCEL_REQUESTED",
) -> WorkflowStatusSummary:
    """Request cancellation of a running or pending workflow by `workflow_id`."""
    engine = _resolve_workflow(workflow_id)
    return engine.cancel(reason=reason)


def recover_workflow(
    workflow_id: str | WorkflowEngine,
    *,
    tasks: Sequence[TaskSpec] | None = None,
    storage_dir: str | Path | None = None,
    max_concurrency: int | None = None,
    clock: Clock | None = None,
    auto_run: bool = True,
    execution_backend: ExecutionBackend | str | None = None,
    definition_version: int | None = None,
    compatibility_policy: CompatibilityPolicy | str | None = None,
    hmac_key: bytes | None = None,
    metrics_sink: MetricsSink | None = None,
) -> WorkflowStatusSummary:
    """
    Recover a workflow from its durable on-disk event log after a process crash
    or restart.

    Note on Workflow Definition Verification:
    `recover_workflow` verifies the persisted `WORKFLOW_CREATED` definition
    version, canonical SHA-256 `definition_hash`, and task specifications
    against the provided `tasks` (or the in-process registered `TaskSpec`
    definitions if recovering within the same process). Across a fresh OS
    process boundary, callers MUST pass `tasks` explicitly; omitting `tasks`
    raises `WorkflowNotFoundError` / `MissingWorkflowDefinitionError`. For
    read-only inspection without task callables, use `inspect_workflow_log()`.
    """
    if isinstance(workflow_id, WorkflowEngine):
        wf_id = workflow_id.workflow_id
        resolved_tasks = (
            list(tasks) if tasks is not None else list(workflow_id._raw_tasks)
        )
        resolved_dir = (
            Path(storage_dir)
            if storage_dir is not None
            else workflow_id.storage_dir
        )
        resolved_conc = (
            max_concurrency
            if max_concurrency is not None
            else workflow_id.max_concurrency
        )
        resolved_clock = clock if clock is not None else workflow_id._clock
        resolved_backend = (
            execution_backend
            if execution_backend is not None
            else workflow_id.execution_backend
        )
        resolved_def_ver = (
            definition_version
            if definition_version is not None
            else workflow_id.definition_version
        )
        resolved_policy = (
            compatibility_policy
            if compatibility_policy is not None
            else workflow_id.compatibility_policy
        )
        resolved_hmac = (
            hmac_key
            if hmac_key is not None
            else workflow_id._event_log._hmac_key
        )
        resolved_sink = (
            metrics_sink
            if metrics_sink is not None
            else workflow_id._metrics_sink
        )
    else:
        wf_id = validate_workflow_id(workflow_id)
        with _REGISTRY_LOCK:
            cached = _WORKFLOW_SPEC_CACHE.get(wf_id)
            existing = _WORKFLOW_REGISTRY.get(wf_id)

        if tasks is not None:
            resolved_tasks = list(tasks)
        elif existing is not None:
            resolved_tasks = list(existing._raw_tasks)
        elif cached is not None:
            resolved_tasks = list(cached["tasks"])
        else:
            raise WorkflowNotFoundError(
                f"Workflow '{wf_id}' is not registered in memory; pass `tasks` and `storage_dir` "
                "explicitly to verify the workflow definition across a fresh process boundary, "
                "or use `inspect_workflow_log()` for read-only log inspection."
            )

        if storage_dir is not None:
            resolved_dir = Path(storage_dir)
        elif existing is not None:
            resolved_dir = existing.storage_dir
        elif cached is not None:
            resolved_dir = cached["storage_dir"]
        else:
            resolved_dir = Path(tempfile.gettempdir()) / "chimera_workflows"

        if max_concurrency is not None:
            resolved_conc = max_concurrency
        elif existing is not None:
            resolved_conc = existing.max_concurrency
        elif cached is not None:
            resolved_conc = cached["max_concurrency"]
        else:
            resolved_conc = 4

        if clock is not None:
            resolved_clock = clock
        elif existing is not None:
            resolved_clock = existing._clock
        elif cached is not None:
            resolved_clock = cached["clock"]
        else:
            resolved_clock = SystemClock()

        if execution_backend is not None:
            resolved_backend = execution_backend
        elif existing is not None:
            resolved_backend = existing.execution_backend
        elif cached is not None:
            resolved_backend = cached["execution_backend"]
        else:
            resolved_backend = ExecutionBackend.THREAD

        if definition_version is not None:
            resolved_def_ver = definition_version
        elif existing is not None:
            resolved_def_ver = existing.definition_version
        elif cached is not None:
            resolved_def_ver = cached["definition_version"]
        else:
            resolved_def_ver = 1

        if compatibility_policy is not None:
            resolved_policy = compatibility_policy
        elif existing is not None:
            resolved_policy = existing.compatibility_policy
        elif cached is not None:
            resolved_policy = cached["compatibility_policy"]
        else:
            resolved_policy = CompatibilityPolicy.STRICT

        if hmac_key is not None:
            resolved_hmac = hmac_key
        elif existing is not None:
            resolved_hmac = existing._event_log._hmac_key
        elif cached is not None:
            resolved_hmac = cached["hmac_key"]
        else:
            resolved_hmac = None

        if metrics_sink is not None:
            resolved_sink = metrics_sink
        elif existing is not None:
            resolved_sink = existing._metrics_sink
        elif cached is not None:
            resolved_sink = cached["metrics_sink"]
        else:
            resolved_sink = None

    fresh_engine = WorkflowEngine(
        tasks=resolved_tasks,
        max_concurrency=resolved_conc,
        workflow_id=wf_id,
        storage_dir=resolved_dir,
        clock=resolved_clock,
        crash_hook=None,
        execution_backend=resolved_backend,
        definition_version=resolved_def_ver,
        compatibility_policy=resolved_policy,
        hmac_key=resolved_hmac,
        metrics_sink=resolved_sink,
    )
    with _REGISTRY_LOCK:
        if wf_id in _WORKFLOW_REGISTRY:
            fresh_engine._global_claim_epoch = _WORKFLOW_REGISTRY[
                wf_id
            ]._global_claim_epoch

    summary = fresh_engine.recover(auto_run=auto_run)
    with _REGISTRY_LOCK:
        _WORKFLOW_REGISTRY[wf_id] = fresh_engine
    return summary


def get_workflow_status(
    workflow_id: str | WorkflowEngine,
) -> WorkflowStatusSummary:
    """Return the current `WorkflowStatusSummary` for `workflow_id`."""
    engine = _resolve_workflow(workflow_id)
    return engine.get_status()


def get_task_result(
    workflow_id: str | WorkflowEngine, task_id: str
) -> TaskResultView:
    """Return the `TaskResultView` for `(workflow_id, task_id)`."""
    engine = _resolve_workflow(workflow_id)
    return engine.get_task_result(task_id)


def _resolve_workflow(workflow_or_id: WorkflowEngine | str) -> WorkflowEngine:
    if isinstance(workflow_or_id, WorkflowEngine):
        with _REGISTRY_LOCK:
            _WORKFLOW_REGISTRY.setdefault(
                workflow_or_id.workflow_id, workflow_or_id
            )
        return workflow_or_id
    with _REGISTRY_LOCK:
        engine = _WORKFLOW_REGISTRY.get(workflow_or_id)
    if engine is None:
        raise WorkflowNotFoundError(
            f"Workflow '{workflow_or_id}' not found in registry."
        )
    return engine
