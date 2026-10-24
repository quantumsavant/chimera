"""
Operational diagnostics, metrics sink protocol, and read-only event log
inspection utility (`inspect_workflow_log`).

Operational Logs vs. Authoritative Event Log:
- The authoritative `EventLog` (`<workflow_id>.chimera.log`) stores the exact
  canonical JSON state transitions and task results required for deterministic
  crash recovery.
- Operational logs (`StructuredLogEntry` and `MetricsSink`) are designed for
  monitoring, alerting, and debugging. By default (`log_sensitive_payloads=False`),
  operational `StructuredLogEntry.details` omit raw `input_payload` and raw
  `result` values, recording only safe structural metadata (`has_result`,
  `result_type`, `failure_category`, `queue_delay_seconds`,
  `execution_duration_seconds`, `worker_pid`, `exit_code`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import threading
from typing import Any, Mapping, Protocol

from chimera.exceptions import (
    ChimeraError,
    CorruptedStateTransitionError,
    LogCorruptionError,
)
from chimera.models import (
    LIVE_ALLOWED_TRANSITIONS,
    RECOVERY_ALLOWED_TRANSITIONS,
    FailureCategory,
    TaskState,
    WorkflowMetrics,
    WorkflowStatus,
)
from chimera.persistence import EventLog


class MetricsSink(Protocol):
    """
    Lightweight operational metrics interface without third-party dependencies.

    Cardinality Note:
    Implementations exporting to time-series backends (e.g., Prometheus or
    OpenTelemetry) should bound label cardinality—prefer tagging by
    `event_type` and `failure_category` rather than unbounded `workflow_id`
    labels in high-churn environments.
    """

    def inc_counter(
        self,
        name: str,
        value: int = 1,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Increment an operational counter metric."""
        ...

    def observe_duration(
        self,
        name: str,
        seconds: float,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Record a duration observation in seconds."""
        ...


class NullMetricsSink:
    """No-op default metrics sink."""

    def inc_counter(
        self,
        name: str,
        value: int = 1,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        return None

    def observe_duration(
        self,
        name: str,
        seconds: float,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        return None


class InMemoryMetricsSink:
    """Thread-safe in-memory metrics collector for testing and local diagnostics."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.counters: dict[str, int] = {}
        self.tagged_counters: list[tuple[str, int, dict[str, str]]] = []
        self.durations: dict[str, list[float]] = {}
        self.tagged_durations: list[tuple[str, float, dict[str, str]]] = []

    def inc_counter(
        self,
        name: str,
        value: int = 1,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + int(value)
            self.tagged_counters.append(
                (name, int(value), dict(tags) if tags else {})
            )

    def observe_duration(
        self,
        name: str,
        seconds: float,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        with self._lock:
            self.durations.setdefault(name, []).append(float(seconds))
            self.tagged_durations.append(
                (name, float(seconds), dict(tags) if tags else {})
            )


@dataclass(frozen=True)
class LogInspectionReport:
    """Read-only diagnostic summary produced by `inspect_workflow_log()`."""

    log_path: str
    workflow_id: str
    exists: bool
    is_healthy: bool
    format_version: int
    schema_version: int
    definition_version: int
    definition_hash: str
    record_count: int
    workflow_status: str
    cancel_requested: bool
    truncated_tail_detected: bool
    truncated_offset: int | None
    warnings: list[str]
    corruption_error: str | None
    counts_by_state: dict[str, int]
    tasks: dict[str, dict[str, Any]]
    metrics: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "log_path": self.log_path,
            "workflow_id": self.workflow_id,
            "exists": self.exists,
            "is_healthy": self.is_healthy,
            "format_version": self.format_version,
            "schema_version": self.schema_version,
            "definition_version": self.definition_version,
            "definition_hash": self.definition_hash,
            "record_count": self.record_count,
            "workflow_status": self.workflow_status,
            "cancel_requested": self.cancel_requested,
            "truncated_tail_detected": self.truncated_tail_detected,
            "truncated_offset": self.truncated_offset,
            "warnings": list(self.warnings),
            "corruption_error": self.corruption_error,
            "counts_by_state": dict(self.counts_by_state),
            "tasks": dict(self.tasks),
            "metrics": dict(self.metrics),
        }


def inspect_workflow_log(
    log_path: str | Path,
    *,
    workflow_id: str | None = None,
    hmac_key: bytes | None = None,
) -> LogInspectionReport:
    """
    Perform a strictly read-only inspection of an on-disk `.chimera.log` file.
    Does not acquire an exclusive lock or modify/truncate the file.
    """
    path = Path(log_path)
    inferred_wf_id = workflow_id
    if inferred_wf_id is None:
        name = path.name
        if name.endswith(".chimera.log"):
            inferred_wf_id = name[: -len(".chimera.log")]
        else:
            inferred_wf_id = path.stem

    if not path.exists():
        return LogInspectionReport(
            log_path=str(path),
            workflow_id=inferred_wf_id,
            exists=False,
            is_healthy=False,
            format_version=1,
            schema_version=1,
            definition_version=1,
            definition_hash="",
            record_count=0,
            workflow_status="NOT_FOUND",
            cancel_requested=False,
            truncated_tail_detected=False,
            truncated_offset=None,
            warnings=["Log file does not exist."],
            corruption_error="Log file does not exist.",
            counts_by_state={},
            tasks={},
            metrics=WorkflowMetrics().to_dict(),
        )

    event_log = EventLog(
        log_path=path,
        workflow_id=inferred_wf_id,
        hmac_key=hmac_key,
    )

    try:
        replay = event_log.replay(repair_truncated_tail=False, read_only=True)
    except ChimeraError as exc:
        return LogInspectionReport(
            log_path=str(path),
            workflow_id=inferred_wf_id,
            exists=True,
            is_healthy=False,
            format_version=event_log.format_version,
            schema_version=1,
            definition_version=1,
            definition_hash="",
            record_count=0,
            workflow_status="CORRUPTED",
            cancel_requested=False,
            truncated_tail_detected=False,
            truncated_offset=None,
            warnings=[],
            corruption_error=f"{type(exc).__name__}: {exc}",
            counts_by_state={},
            tasks={},
            metrics=WorkflowMetrics().to_dict(),
        )

    # Validate state-machine transitions across replayed records
    schema_version = 1
    definition_version = 1
    definition_hash = ""
    wf_status = WorkflowStatus.CREATED.value
    cancel_requested = False
    initialized = False
    tasks: dict[str, dict[str, Any]] = {}
    dependencies_map: dict[str, list[str]] = {}

    succeeded_count = 0
    failed_count = 0
    cancelled_count = 0
    stale_count = 0
    recovery_count = 0
    timeout_count = 0
    crash_count = 0
    retry_count = 0

    try:
        for rec in replay.records:
            seq = int(rec["seq"])
            etype = str(rec["event_type"])
            data = rec["data"]

            if etype == "WORKFLOW_CREATED":
                if seq != 1 or initialized:
                    raise CorruptedStateTransitionError(
                        f"Duplicate or misplaced WORKFLOW_CREATED at seq={seq}."
                    )
                initialized = True
                schema_version = int(data.get("schema_version", 1))
                definition_version = int(data.get("definition_version", 1))
                definition_hash = str(data.get("definition_hash", ""))
                tasks_meta = data.get("tasks", {})
                if not isinstance(tasks_meta, dict):
                    raise CorruptedStateTransitionError(
                        f"Invalid tasks dict in WORKFLOW_CREATED at seq={seq}."
                    )
                for tid, meta in sorted(tasks_meta.items()):
                    dependencies_map[tid] = list(meta.get("dependencies", []))
                    tasks[tid] = {
                        "task_id": tid,
                        "state": TaskState.PENDING.value,
                        "attempt": 0,
                        "claim_epoch": 0,
                        "recovery_count": 0,
                        "retry_count": 0,
                        "timeout_count": 0,
                        "failure_category": FailureCategory.NONE.value,
                        "last_reason": "INITIALIZED",
                    }
                wf_status = WorkflowStatus.RUNNING.value

            elif etype == "DEFINITION_MIGRATED":
                definition_version = int(
                    data.get("to_definition_version", definition_version)
                )
                definition_hash = str(
                    data.get("to_definition_hash", definition_hash)
                )

            elif etype == "WORKFLOW_CANCEL_REQUESTED":
                cancel_requested = True

            elif etype == "WORKFLOW_COMPLETED":
                wf_status = str(data["status"])

            elif etype == "STALE_COMPLETION_IGNORED":
                stale_count += 1

            elif etype == "TASK_STATE_CHANGED":
                tid = str(data.get("task_id", ""))
                if tid not in tasks:
                    raise CorruptedStateTransitionError(
                        f"Unknown task_id '{tid}' at seq={seq}."
                    )
                t_entry = tasks[tid]
                from_st = TaskState(str(data["from_state"]))
                to_st = TaskState(str(data["to_state"]))
                reason = str(data.get("reason", ""))
                allowed = (
                    RECOVERY_ALLOWED_TRANSITIONS
                    if reason == "RECOVERED_FROM_CRASH"
                    else LIVE_ALLOWED_TRANSITIONS
                )
                if TaskState(t_entry["state"]) != from_st or to_st not in allowed[from_st]:
                    raise CorruptedStateTransitionError(
                        f"Illegal state transition for '{tid}' at seq={seq}: {from_st.value} -> {to_st.value}"
                    )
                if to_st in (TaskState.READY, TaskState.RUNNING):
                    for dep_id in dependencies_map.get(tid, []):
                        if tasks[dep_id]["state"] != TaskState.SUCCEEDED.value:
                            raise CorruptedStateTransitionError(
                                f"Task '{tid}' entered {to_st.value} before dependency '{dep_id}' succeeded."
                            )
                epoch_val = int(data.get("claim_epoch", t_entry["claim_epoch"]))
                if epoch_val < t_entry["claim_epoch"]:
                    raise CorruptedStateTransitionError(
                        f"Non-monotonic claim_epoch for '{tid}' at seq={seq}."
                    )
                t_entry["state"] = to_st.value
                t_entry["attempt"] = int(data.get("attempt", t_entry["attempt"]))
                t_entry["claim_epoch"] = epoch_val
                t_entry["recovery_count"] = int(
                    data.get("recovery_count", t_entry["recovery_count"])
                )
                t_entry["failure_category"] = str(
                    data.get("failure_category", t_entry["failure_category"])
                )
                t_entry["last_reason"] = reason

                if reason == "RECOVERED_FROM_CRASH":
                    recovery_count += 1
                if "TIMEOUT" in reason:
                    timeout_count += 1
                    t_entry["timeout_count"] += 1
                if t_entry["failure_category"] == FailureCategory.PROCESS_CRASH.value:
                    crash_count += 1
                if to_st == TaskState.RETRY_WAIT:
                    retry_count += 1
                    t_entry["retry_count"] += 1
                elif to_st == TaskState.SUCCEEDED:
                    succeeded_count += 1
                elif to_st == TaskState.FAILED:
                    failed_count += 1
                elif to_st == TaskState.CANCELLED:
                    cancelled_count += 1
    except ChimeraError as exc:
        return LogInspectionReport(
            log_path=str(path),
            workflow_id=inferred_wf_id,
            exists=True,
            is_healthy=False,
            format_version=replay.format_version,
            schema_version=schema_version,
            definition_version=definition_version,
            definition_hash=definition_hash,
            record_count=len(replay.records),
            workflow_status="CORRUPTED",
            cancel_requested=cancel_requested,
            truncated_tail_detected=(replay.truncated_offset is not None),
            truncated_offset=replay.truncated_offset,
            warnings=replay.warnings,
            corruption_error=f"{type(exc).__name__}: {exc}",
            counts_by_state={},
            tasks=tasks,
            metrics=WorkflowMetrics().to_dict(),
        )

    counts: dict[str, int] = {s.value: 0 for s in TaskState}
    for t_info in tasks.values():
        counts[t_info["state"]] += 1

    metrics = WorkflowMetrics(
        succeeded_count=succeeded_count,
        failed_count=failed_count,
        cancelled_count=cancelled_count,
        stale_completion_count=stale_count,
        recovery_event_count=recovery_count,
        timeout_event_count=timeout_count,
        worker_crash_count=crash_count,
        retry_scheduled_count=retry_count,
    )

    return LogInspectionReport(
        log_path=str(path),
        workflow_id=inferred_wf_id,
        exists=True,
        is_healthy=True,
        format_version=replay.format_version,
        schema_version=schema_version,
        definition_version=definition_version,
        definition_hash=definition_hash,
        record_count=len(replay.records),
        workflow_status=wf_status,
        cancel_requested=cancel_requested,
        truncated_tail_detected=(replay.truncated_offset is not None),
        truncated_offset=replay.truncated_offset,
        warnings=replay.warnings,
        corruption_error=None,
        counts_by_state=counts,
        tasks=tasks,
        metrics=metrics.to_dict(),
    )
