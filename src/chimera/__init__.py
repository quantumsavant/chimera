"""
CHIMERA — Fault-Tolerant DAG Workflow Engine.
"""

from __future__ import annotations

from chimera.clock import Clock, SystemClock, VirtualClock
from chimera.engine import (
    WorkflowEngine,
    cancel_workflow,
    create_workflow,
    get_task_result,
    get_workflow_status,
    recover_workflow,
    run_workflow,
    validate_workflow,
)
from chimera.exceptions import (
    ChimeraError,
    ConcurrentWorkflowAccessError,
    CorruptedStateTransitionError,
    CycleDetectedError,
    DuplicateTaskIdError,
    InvalidStateTransitionError,
    LogCorruptionError,
    MissingDependencyError,
    SimulatedProcessCrash,
    TaskTimeoutError,
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
from chimera.persistence import (
    FRAME_HEADER_LEN,
    FRAME_MAGIC,
    EventLog,
    ReplayResult,
)
from chimera.recovery import RecoveryManager
from chimera.scheduler import DeterministicReadyScheduler
from chimera.validation import validate_dag

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "Clock",
    "SystemClock",
    "VirtualClock",
    "ChimeraError",
    "WorkflowValidationError",
    "DuplicateTaskIdError",
    "MissingDependencyError",
    "CycleDetectedError",
    "InvalidStateTransitionError",
    "LogCorruptionError",
    "CorruptedStateTransitionError",
    "ConcurrentWorkflowAccessError",
    "TaskTimeoutError",
    "WorkflowNotFoundError",
    "SimulatedProcessCrash",
    "WorkflowFileLock",
    "TaskState",
    "TERMINAL_TASK_STATES",
    "LIVE_ALLOWED_TRANSITIONS",
    "RECOVERY_ALLOWED_TRANSITIONS",
    "WorkflowStatus",
    "ClaimToken",
    "TaskContext",
    "TaskSpec",
    "TaskRuntimeState",
    "TaskResultView",
    "StructuredLogEntry",
    "WorkflowStatusSummary",
    "FRAME_MAGIC",
    "FRAME_HEADER_LEN",
    "ReplayResult",
    "EventLog",
    "RecoveryManager",
    "DeterministicReadyScheduler",
    "validate_dag",
    "WorkflowEngine",
    "create_workflow",
    "validate_workflow",
    "run_workflow",
    "cancel_workflow",
    "recover_workflow",
    "get_workflow_status",
    "get_task_result",
]
