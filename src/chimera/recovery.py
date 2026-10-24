"""
Recovery validation, canonical definition hash verification, and schema
migration helpers (`RecoveryManager`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from chimera.exceptions import (
    CorruptedStateTransitionError,
    LegacyLogMigrationError,
    MissingWorkflowDefinitionError,
    WorkflowDefinitionMismatchError,
)
from chimera.models import (
    CompatibilityPolicy,
    TaskSpec,
    compute_workflow_definition_hash,
)


@dataclass(frozen=True)
class DefinitionVerificationResult:
    """Result of verifying persisted WORKFLOW_CREATED metadata against TaskSpecs."""

    schema_version: int
    definition_version: int
    definition_hash: str
    migrated: bool = False
    migration_reason: str | None = None


class RecoveryManager:
    """
    Validates persisted workflow metadata, canonical SHA-256 definition hashes,
    and implementation versions against caller-supplied `TaskSpec` definitions
    during crash recovery.
    """

    @staticmethod
    def verify_recovered_task_specs(
        workflow_id: str,
        task_specs: Mapping[str, TaskSpec],
        logged_data: Any,
        seq: int,
        *,
        definition_version: int = 1,
        compatibility_policy: CompatibilityPolicy = CompatibilityPolicy.STRICT,
        read_only_inspection: bool = False,
    ) -> DefinitionVerificationResult:
        """
        Verify the `WORKFLOW_CREATED` payload against `task_specs` under
        `compatibility_policy`.

        Note: `logged_data` may be either the full `WORKFLOW_CREATED` `data`
        dict (containing `tasks`, `schema_version`, `definition_version`,
        `definition_hash`) or a legacy raw `tasks` metadata dict.
        """
        if not isinstance(logged_data, dict):
            raise CorruptedStateTransitionError(
                f"Corrupted WORKFLOW_CREATED payload at seq={seq}."
            )

        if "tasks" in logged_data and isinstance(logged_data.get("tasks"), dict):
            logged_tasks_meta = logged_data["tasks"]
            schema_version = int(logged_data.get("schema_version", 1))
            logged_def_version = int(logged_data.get("definition_version", 1))
            logged_def_hash = str(logged_data.get("definition_hash", ""))
        else:
            # Direct tasks_meta dict passed by legacy callers
            logged_tasks_meta = logged_data
            schema_version = 1
            logged_def_version = 1
            logged_def_hash = ""

        if not isinstance(logged_tasks_meta, dict):
            raise CorruptedStateTransitionError(
                f"Corrupted tasks metadata in WORKFLOW_CREATED at seq={seq}."
            )

        if read_only_inspection and not task_specs:
            return DefinitionVerificationResult(
                schema_version=schema_version,
                definition_version=logged_def_version,
                definition_hash=logged_def_hash,
                migrated=False,
            )

        if not task_specs and logged_tasks_meta:
            raise MissingWorkflowDefinitionError(
                f"Cannot recover workflow '{workflow_id}' with empty TaskSpecs when the "
                f"persisted workflow contains {len(logged_tasks_meta)} tasks: "
                f"{sorted(logged_tasks_meta.keys())}."
            )

        if set(task_specs.keys()) != set(logged_tasks_meta.keys()):
            raise WorkflowDefinitionMismatchError(
                f"Recovered workflow '{workflow_id}' task IDs {sorted(logged_tasks_meta.keys())} "
                f"do not match provided TaskSpecs {sorted(task_specs.keys())}."
            )

        expected_hash = compute_workflow_definition_hash(
            dict(task_specs), definition_version=definition_version
        )

        # Case 1: Legacy v1 log (no definition_hash persisted)
        if schema_version == 1 and not logged_def_hash:
            for tid, meta in logged_tasks_meta.items():
                if not isinstance(meta, dict):
                    raise CorruptedStateTransitionError(
                        f"Corrupted task metadata for '{tid}' at seq={seq}."
                    )
                spec = task_specs[tid]
                # Compare v1 fields
                expected_v1 = spec.to_v1_metadata_dict()
                logged_v1 = {k: meta[k] for k in expected_v1 if k in meta}
                if expected_v1 != logged_v1:
                    raise WorkflowDefinitionMismatchError(
                        f"Recovered task '{tid}' definition does not match persisted v1 "
                        f"WORKFLOW_CREATED metadata: persisted={logged_v1} vs provided={expected_v1}"
                    )
                if (
                    compatibility_policy == CompatibilityPolicy.STRICT
                    and spec.implementation_version != meta.get("implementation_version", "1.0")
                ):
                    raise LegacyLogMigrationError(
                        f"Task '{tid}' specifies implementation_version={spec.implementation_version!r} "
                        "against a legacy v1 log; pass CompatibilityPolicy.MIGRATE_LEGACY_V1 or "
                        "CompatibilityPolicy.ALLOW_IMPLEMENTATION_UPGRADE."
                    )

            if (
                compatibility_policy == CompatibilityPolicy.STRICT
                and definition_version != 1
            ):
                raise LegacyLogMigrationError(
                    f"Workflow specifies definition_version={definition_version} against a legacy v1 log; "
                    "use CompatibilityPolicy.MIGRATE_LEGACY_V1 to migrate."
                )

            migrated = compatibility_policy in (
                CompatibilityPolicy.MIGRATE_LEGACY_V1,
                CompatibilityPolicy.ALLOW_IMPLEMENTATION_UPGRADE,
            )
            return DefinitionVerificationResult(
                schema_version=2 if migrated else 1,
                definition_version=definition_version,
                definition_hash=expected_hash,
                migrated=migrated,
                migration_reason="MIGRATED_FROM_LEGACY_V1" if migrated else None,
            )

        # Case 2: Schema v2 log with canonical definition_hash
        if compatibility_policy == CompatibilityPolicy.STRICT:
            if logged_def_version != definition_version:
                raise WorkflowDefinitionMismatchError(
                    f"Workflow '{workflow_id}' definition_version mismatch: "
                    f"persisted={logged_def_version}, provided={definition_version}."
                )
            for tid, meta in logged_tasks_meta.items():
                expected_meta = task_specs[tid].to_metadata_dict()
                if expected_meta != meta:
                    raise WorkflowDefinitionMismatchError(
                        f"Recovered task '{tid}' definition does not match persisted "
                        f"WORKFLOW_CREATED metadata: persisted={meta} vs provided={expected_meta}"
                    )
            if logged_def_hash and logged_def_hash != expected_hash:
                raise WorkflowDefinitionMismatchError(
                    f"Workflow '{workflow_id}' canonical definition_hash mismatch: "
                    f"persisted={logged_def_hash}, computed={expected_hash}."
                )
            return DefinitionVerificationResult(
                schema_version=schema_version,
                definition_version=definition_version,
                definition_hash=expected_hash,
                migrated=False,
            )

        # CompatibilityPolicy.ALLOW_IMPLEMENTATION_UPGRADE or MIGRATE_LEGACY_V1 on v2 log
        migrated_tasks: list[str] = []
        for tid, meta in logged_tasks_meta.items():
            if not isinstance(meta, dict):
                raise CorruptedStateTransitionError(
                    f"Corrupted task metadata for '{tid}' at seq={seq}."
                )
            spec = task_specs[tid]
            expected_core = spec.core_configuration_dict()
            logged_core = dict(meta)
            logged_impl_ver = logged_core.pop("implementation_version", "1.0")
            if expected_core != logged_core:
                raise WorkflowDefinitionMismatchError(
                    f"Recovered task '{tid}' core configuration (DAG/payload/retry policy) "
                    f"cannot be altered under {compatibility_policy.value}: "
                    f"persisted={logged_core} vs provided={expected_core}"
                )
            if spec.implementation_version != logged_impl_ver:
                migrated_tasks.append(
                    f"{tid}:{logged_impl_ver}->{spec.implementation_version}"
                )

        def_ver_changed = logged_def_version != definition_version
        did_migrate = bool(migrated_tasks) or def_ver_changed
        reason_parts: list[str] = []
        if def_ver_changed:
            reason_parts.append(
                f"definition_version:{logged_def_version}->{definition_version}"
            )
        if migrated_tasks:
            reason_parts.append(f"tasks:[{','.join(migrated_tasks)}]")

        return DefinitionVerificationResult(
            schema_version=2,
            definition_version=definition_version,
            definition_hash=expected_hash,
            migrated=did_migrate,
            migration_reason=";".join(reason_parts) if did_migrate else None,
        )
