"""
Core workflow orchestration engine (`WorkflowEngine`) and top-level public API.
"""

from __future__ import annotations

from contextlib import contextmanager
import copy
import json
from pathlib import Path
import tempfile
import threading
from typing import Any, Callable, Iterator, Sequence
import uuid

from chimera.clock import Clock, SystemClock, VirtualClock
from chimera.exceptions import (
    CorruptedStateTransitionError,
    InvalidStateTransitionError,
    LogCorruptionError,
    SimulatedProcessCrash,
    WorkflowNotFoundError,
    WorkflowValidationError,
)
from chimera.locking import WorkflowFileLock
from chimera.models import (
    LIVE_ALLOWED_TRANSITIONS,
    RECOVERY_ALLOWED_TRANSITIONS,
    TERMINAL_TASK_STATES,
    ClaimToken,
    StructuredLogEntry,
    TaskContext,
    TaskResultView,
    TaskRuntimeState,
    TaskSpec,
    TaskState,
    WorkflowStatus,
    WorkflowStatusSummary,
)
from chimera.persistence import EventLog
from chimera.recovery import RecoveryManager
from chimera.scheduler import DeterministicReadyScheduler, invoke_task_callable
from chimera.validation import validate_dag


class WorkflowEngine:
    """
    Fault-tolerant DAG workflow engine managing task state transitions, durable
    event logging, fenced worker claims, timeouts, retries, cancellation, and
    crash recovery.
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
    ) -> None:
        if max_concurrency < 1:
            raise WorkflowValidationError("max_concurrency must be >= 1.")

        self.workflow_id: str = workflow_id or f"wf_{uuid.uuid4().hex[:12]}"
        self.max_concurrency: int = int(max_concurrency)
        self._raw_tasks: list[TaskSpec] = list(tasks)
        self._clock: Clock = clock or SystemClock()

        if storage_dir is None:
            storage_dir = Path(tempfile.gettempdir()) / "chimera_workflows"
        self.storage_dir: Path = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.log_path: Path = self.storage_dir / f"{self.workflow_id}.chimera.log"
        self.lock_path: Path = self.storage_dir / f"{self.workflow_id}.chimera.lock"

        self._file_lock = WorkflowFileLock(self.lock_path, self.workflow_id)
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
        )

        self._task_specs: dict[str, TaskSpec] = {}
        self._dependents: dict[str, list[str]] = {}
        self._task_states: dict[str, TaskRuntimeState] = {}
        self._scheduler = DeterministicReadyScheduler()

        self._global_claim_epoch: int = 0
        self._active_cancel_events: dict[tuple[str, int], threading.Event] = {}

        self._validated: bool = False
        self._initialized_in_log: bool = False
        self._status: WorkflowStatus = WorkflowStatus.CREATED
        self._cancel_requested: bool = False
        self._truncated_tail_repaired: bool = False
        self._structured_logs: list[StructuredLogEntry] = []

        # Deterministic test hooks
        self.on_task_claimed_hook: Callable[[ClaimToken], None] | None = None
        self.before_task_commit_hook: Callable[[ClaimToken, str], None] | None = None

    @contextmanager
    def lock_workflow(self) -> Iterator[None]:
        """Acquire the exclusive cross-process and in-process workflow lock."""
        with self._file_lock.acquire():
            yield

    # ------------------------------------------------------------------
    # Validation & Initialization
    # ------------------------------------------------------------------

    def validate(self) -> bool:
        """Validate the workflow DAG and initialize graph adjacency maps."""
        with self._lock:
            self._task_specs = validate_dag(self._raw_tasks)
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
    ) -> None:
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
            RecoveryManager.verify_recovered_task_specs(
                self.workflow_id, self._task_specs, tasks_meta, seq
            )

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
                    details={"task_count": len(tasks_meta)},
                )
            )
            return

        if not self._initialized_in_log:
            raise CorruptedStateTransitionError(
                f"Event '{etype}' at seq={seq} appeared before WORKFLOW_CREATED."
            )

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
                )
            )
            return

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
                )
            )
            return

        if etype == "STALE_COMPLETION_IGNORED":
            tid = str(data.get("task_id", ""))
            if tid not in self._task_states:
                raise CorruptedStateTransitionError(
                    f"Unknown task_id '{tid}' in STALE_COMPLETION_IGNORED at seq={seq}."
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
                    details=dict(data),
                )
            )
            return

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

            rt.state = to_state
            rt.last_transition_reason = reason
            rt.last_updated_at = ts

            if to_state == TaskState.READY:
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
                rt.started_at = data.get("started_at", ts)
                rt.deadline_at = data.get("deadline_at")
                rt.worker_id = data.get("worker_id")
                rt.wake_at = None
            elif to_state == TaskState.RETRY_WAIT:
                rt.started_at = None
                rt.deadline_at = None
                rt.worker_id = None
                rt.wake_at = data.get("wake_at")
                rt.error_type = data.get("error_type")
                rt.error_message = data.get("error_message")
            elif to_state == TaskState.SUCCEEDED:
                rt.started_at = None
                rt.deadline_at = None
                rt.wake_at = None
                rt.result = copy.deepcopy(data.get("result"))
                rt.has_result = True
                rt.error_type = None
                rt.error_message = None
            elif to_state == TaskState.FAILED:
                rt.started_at = None
                rt.deadline_at = None
                rt.wake_at = None
                rt.error_type = data.get("error_type")
                rt.error_message = data.get("error_message")
            elif to_state == TaskState.CANCELLED:
                rt.started_at = None
                rt.deadline_at = None
                rt.wake_at = None
                if data.get("error_type"):
                    rt.error_type = data.get("error_type")
                if data.get("error_message"):
                    rt.error_message = data.get("error_message")

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
                    details={
                        k: v
                        for k, v in data.items()
                        if k
                        not in {
                            "task_id",
                            "from_state",
                            "to_state",
                            "reason",
                            "attempt",
                            "claim_epoch",
                        }
                    },
                )
            )
            return

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
        result: Any = None,
        include_result: bool = False,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Persist a task state transition to EventLog and apply it atomically."""
        rt = self._task_states[task_id]
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
        if started_at is not None:
            payload["started_at"] = started_at
        if deadline_at is not None:
            payload["deadline_at"] = deadline_at
        if wake_at is not None:
            payload["wake_at"] = wake_at
        if worker_id is not None:
            payload["worker_id"] = worker_id
        if include_result:
            payload["result"] = self._make_json_safe(result)
        if error_type is not None:
            payload["error_type"] = error_type
        if error_message is not None:
            payload["error_message"] = error_message

        rec = self._event_log.append("TASK_STATE_CHANGED", payload)
        self._apply_event(
            rec, is_recovery=(reason == "RECOVERED_FROM_CRASH")
        )
        if include_result:
            self._task_states[task_id].result = copy.deepcopy(result)

    @staticmethod
    def _make_json_safe(val: Any) -> Any:
        try:
            json.dumps(val)
            return copy.deepcopy(val)
        except (TypeError, OverflowError):
            return repr(val)

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
            if self._cancel_requested:
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
            if self._cancel_requested:
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
        ctx = TaskContext(
            workflow_id=self.workflow_id,
            task_id=task_id,
            attempt=new_attempt,
            claim_epoch=new_epoch,
            idempotency_key=f"{self.workflow_id}:{task_id}",
            dependency_results=dep_results,
            _cancel_event=cancel_event,
        )
        return token, spec, isolated_payload, ctx

    def complete_task_attempt(
        self,
        token: ClaimToken,
        *,
        succeeded: bool,
        result: Any = None,
        exception: BaseException | None = None,
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
            )

    def _complete_task_attempt_locked(
        self,
        token: ClaimToken,
        *,
        succeeded: bool,
        result: Any = None,
        exception: BaseException | None = None,
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
            self._record_task_transition(
                token.task_id,
                TaskState.SUCCEEDED,
                reason="EXECUTION_SUCCEEDED",
                result=result,
                include_result=True,
            )
            self._promote_eligible_pending_tasks(
                reason=f"DEPENDENCY_SUCCEEDED:{token.task_id}"
            )
            self._check_and_finalize_workflow()
            self._cond.notify_all()
            return True

        assert exception is not None
        err_type = type(exception).__name__
        err_msg = str(exception)
        is_retryable = isinstance(exception, spec.retryable_exceptions)
        retries_used = rt.attempt - 1

        if (
            not self._cancel_requested
            and is_retryable
            and retries_used < spec.max_retries
        ):
            backoff = spec.compute_backoff(rt.attempt)
            wake_at = self._clock.now() + backoff
            self._record_task_transition(
                token.task_id,
                TaskState.RETRY_WAIT,
                reason=f"RETRYABLE_ERROR:{err_type}",
                wake_at=wake_at,
                error_type=err_type,
                error_message=err_msg,
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
                error_type=err_type,
                error_message=err_msg,
            )
            self._cascade_dependency_failure(token.task_id)
            self._check_and_finalize_workflow()

        self._cond.notify_all()
        return True

    def _enforce_timeouts_and_timers_locked(self) -> tuple[int, float | None]:
        now = self._clock.now()
        timed_out_count = 0
        next_wake: float | None = None

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
                    backoff = spec.compute_backoff(rt.attempt)
                    wake_at = now + backoff
                    self._record_task_transition(
                        tid,
                        TaskState.RETRY_WAIT,
                        reason="TASK_TIMEOUT_RETRY",
                        claim_epoch=new_epoch,
                        wake_at=wake_at,
                        error_type="TaskTimeoutError",
                        error_message=err_msg,
                    )
                else:
                    self._record_task_transition(
                        tid,
                        TaskState.FAILED,
                        reason="TASK_TIMEOUT",
                        claim_epoch=new_epoch,
                        error_type="TaskTimeoutError",
                        error_message=err_msg,
                    )
                    self._cascade_dependency_failure(tid)
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
        return timed_out_count, next_wake

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    def cancel(
        self, reason: str = "USER_CANCEL_REQUESTED"
    ) -> WorkflowStatusSummary:
        """
        Request workflow cancellation without overwriting a completed workflow's
        terminal status.
        """
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

            for (_tid, _epoch), ev in list(self._active_cancel_events.items()):
                ev.set()
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
                    )

            self._check_and_finalize_workflow()
            self._cond.notify_all()
            return self.get_status()

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
                        claim = self.claim_next_ready_task(token.worker_id)
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
                            claim = self.claim_next_ready_task(token.worker_id)

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

            with self._cond:
                while True:
                    if crash_exc is not None:
                        raise crash_exc

                    _, next_wait = self._enforce_timeouts_and_timers_locked()

                    if self._check_and_finalize_workflow():
                        break

                    while (
                        not self._cancel_requested
                        and self._active_running_count() < self.max_concurrency
                        and self._scheduler.has_ready()
                    ):
                        worker_seq += 1
                        worker_id = f"worker-{worker_seq}"
                        claim = self.claim_next_ready_task(worker_id)
                        if claim is None:
                            break
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

                    self._clock.wait_condition(self._cond, timeout=next_wait)

            for t in active_threads:
                t.join(timeout=0.02)

            if crash_exc is not None:
                raise crash_exc

            return self.get_status()

    # ------------------------------------------------------------------
    # Crash Recovery & Log Replay
    # ------------------------------------------------------------------

    def recover(self, auto_run: bool = True) -> WorkflowStatusSummary:
        """
        Replay the durable event log to reconstruct workflow state after an
        interruption, repair any truncated final log record, validate task
        definitions, reconcile in-flight states, and optionally resume execution.
        """
        with self._file_lock.acquire():
            with self._cond:
                if not self._validated:
                    self.validate()

                for (_tid, _ep), ev in list(self._active_cancel_events.items()):
                    ev.set()
                self._active_cancel_events.clear()

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

                replay_res = self._event_log.replay(repair_truncated_tail=True)
                self._truncated_tail_repaired = replay_res.truncated_tail_repaired

                for rec in replay_res.records:
                    self._apply_event(rec, is_recovery=True)

                if self._global_claim_epoch < prior_max_epoch:
                    self._global_claim_epoch = prior_max_epoch

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
                            details={
                                "truncated_offset": replay_res.truncated_offset,
                                "warnings": replay_res.warnings,
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
        """Return a thread-safe snapshot of the workflow status and structured logs."""
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
                )

            return WorkflowStatusSummary(
                workflow_id=self.workflow_id,
                status=self._status,
                cancel_requested=self._cancel_requested,
                total_tasks=len(self._task_states),
                counts_by_state=counts,
                tasks=task_views,
                truncated_tail_repaired=self._truncated_tail_repaired,
                log_record_count=len(self._structured_logs),
                structured_logs=list(self._structured_logs),
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
            )


# ============================================================================
# Public Registry & Top-Level API Functions
# ============================================================================

_REGISTRY_LOCK = threading.RLock()
_WORKFLOW_REGISTRY: dict[str, WorkflowEngine] = {}
_WORKFLOW_SPEC_CACHE: dict[str, tuple[list[TaskSpec], Path, int, Clock]] = {}


def create_workflow(
    tasks: Sequence[TaskSpec],
    max_concurrency: int = 4,
    *,
    workflow_id: str | None = None,
    storage_dir: str | Path | None = None,
    clock: Clock | None = None,
    crash_hook: Callable[[str, dict[str, Any], int], None] | None = None,
    partial_write_bytes: int | None = None,
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
    )
    with _REGISTRY_LOCK:
        _WORKFLOW_REGISTRY[engine.workflow_id] = engine
        _WORKFLOW_SPEC_CACHE[engine.workflow_id] = (
            list(tasks),
            engine.storage_dir,
            engine.max_concurrency,
            engine._clock,
        )
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
) -> WorkflowStatusSummary:
    """Recover a workflow from its durable on-disk event log after a process crash or restart."""
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
    else:
        wf_id = workflow_id
        with _REGISTRY_LOCK:
            cached = _WORKFLOW_SPEC_CACHE.get(wf_id)
            existing = _WORKFLOW_REGISTRY.get(wf_id)

        if tasks is not None:
            resolved_tasks = list(tasks)
        elif existing is not None:
            resolved_tasks = list(existing._raw_tasks)
        elif cached is not None:
            resolved_tasks = list(cached[0])
        else:
            raise WorkflowNotFoundError(
                f"Workflow '{wf_id}' is not in memory; pass `tasks` and `storage_dir` "
                "to reconstruct across a fresh process boundary."
            )

        if storage_dir is not None:
            resolved_dir = Path(storage_dir)
        elif existing is not None:
            resolved_dir = existing.storage_dir
        elif cached is not None:
            resolved_dir = cached[1]
        else:
            resolved_dir = Path(tempfile.gettempdir()) / "chimera_workflows"

        if max_concurrency is not None:
            resolved_conc = max_concurrency
        elif existing is not None:
            resolved_conc = existing.max_concurrency
        elif cached is not None:
            resolved_conc = cached[2]
        else:
            resolved_conc = 4

        if clock is not None:
            resolved_clock = clock
        elif existing is not None:
            resolved_clock = existing._clock
        elif cached is not None:
            resolved_clock = cached[3]
        else:
            resolved_clock = SystemClock()

    fresh_engine = WorkflowEngine(
        tasks=resolved_tasks,
        max_concurrency=resolved_conc,
        workflow_id=wf_id,
        storage_dir=resolved_dir,
        clock=resolved_clock,
        crash_hook=None,
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
