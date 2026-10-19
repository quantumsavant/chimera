"""
Recovery validation and reconciliation helpers (`RecoveryManager`).
"""

from __future__ import annotations

from typing import Any, Mapping

from chimera.exceptions import (
    CorruptedStateTransitionError,
    WorkflowValidationError,
)
from chimera.models import TaskSpec


class RecoveryManager:
    """
    Validates persisted workflow metadata against provided `TaskSpec` definitions
    during crash recovery.
    """

    @staticmethod
    def verify_recovered_task_specs(
        workflow_id: str,
        task_specs: Mapping[str, TaskSpec],
        logged_tasks_meta: Any,
        seq: int,
    ) -> None:
        if not isinstance(logged_tasks_meta, dict):
            raise CorruptedStateTransitionError(
                f"Corrupted tasks metadata in WORKFLOW_CREATED at seq={seq}."
            )
        if not task_specs:
            return

        if set(task_specs.keys()) != set(logged_tasks_meta.keys()):
            raise WorkflowValidationError(
                f"Recovered workflow '{workflow_id}' task IDs {sorted(logged_tasks_meta.keys())} "
                f"do not match provided TaskSpecs {sorted(task_specs.keys())}."
            )
        for tid, meta in logged_tasks_meta.items():
            expected_meta = task_specs[tid].to_metadata_dict()
            if expected_meta != meta:
                raise WorkflowValidationError(
                    f"Recovered task '{tid}' definition does not match persisted "
                    f"WORKFLOW_CREATED metadata: persisted={meta} vs provided={expected_meta}"
                )
