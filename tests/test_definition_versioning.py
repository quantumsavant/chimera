"""
Tests for Phase 4: Workflow Definition Identity, Canonical SHA-256 Hashing,
Implementation Versioning, Legacy v1 Migration, and Safe Recovery.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from chimera import (
    CompatibilityPolicy,
    LegacyLogMigrationError,
    MissingWorkflowDefinitionError,
    SimulatedProcessCrash,
    TaskSpec,
    TaskState,
    VirtualClock,
    WorkflowDefinitionMismatchError,
    WorkflowEngine,
    WorkflowNotFoundError,
    WorkflowStatus,
    compute_workflow_definition_hash,
    create_workflow,
    get_task_result,
    recover_workflow,
    run_workflow,
)


class TestDefinitionVersioningAndRecovery(unittest.TestCase):
    """Verify canonical hashing, implementation versioning, and recovery policies."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage_dir = Path(self._tmp.name)
        self.clock = VirtualClock(auto_advance_on_wait=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_canonical_definition_hash_deterministic_across_dict_key_orders(
        self,
    ) -> None:
        t1 = {
            "a": TaskSpec(
                "a",
                func=lambda p: p,
                input_payload={"z": 1, "a": [2, 3], "m": {"k2": "v2", "k1": "v1"}},
                implementation_version="1.2.0",
            ),
            "b": TaskSpec("b", func=lambda: 1, dependencies=["a"]),
        }
        t2 = {
            "b": TaskSpec("b", func=lambda: 2, dependencies=["a"]),
            "a": TaskSpec(
                "a",
                func=lambda p: p,
                input_payload={"m": {"k1": "v1", "k2": "v2"}, "z": 1, "a": (2, 3)},
                implementation_version="1.2.0",
            ),
        }
        h1 = compute_workflow_definition_hash(t1, definition_version=3)
        h2 = compute_workflow_definition_hash(t2, definition_version=3)
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)

        # Changing implementation_version changes the canonical hash
        t3 = {
            "a": TaskSpec(
                "a",
                func=lambda p: p,
                input_payload={"z": 1, "a": [2, 3], "m": {"k2": "v2", "k1": "v1"}},
                implementation_version="1.2.1",
            ),
            "b": TaskSpec("b", func=lambda: 1, dependencies=["a"]),
        }
        self.assertNotEqual(
            h1, compute_workflow_definition_hash(t3, definition_version=3)
        )

    def test_strict_recovery_rejects_implementation_or_definition_version_changes(
        self,
    ) -> None:
        wf = create_workflow(
            [
                TaskSpec(
                    "t1",
                    func=lambda: "v1_out",
                    implementation_version="1.0.0",
                )
            ],
            workflow_id="wf_strict_ver",
            storage_dir=self.storage_dir,
            clock=self.clock,
            definition_version=1,
        )
        run_workflow(wf)

        # 1. Changed implementation_version rejected under STRICT
        with self.assertRaises(WorkflowDefinitionMismatchError):
            recover_workflow(
                "wf_strict_ver",
                tasks=[
                    TaskSpec(
                        "t1",
                        func=lambda: "v2_out",
                        implementation_version="2.0.0",
                    )
                ],
                storage_dir=self.storage_dir,
                definition_version=1,
                compatibility_policy=CompatibilityPolicy.STRICT,
            )

        # 2. Changed definition_version rejected under STRICT
        with self.assertRaises(WorkflowDefinitionMismatchError):
            recover_workflow(
                "wf_strict_ver",
                tasks=[
                    TaskSpec(
                        "t1",
                        func=lambda: "v1_out",
                        implementation_version="1.0.0",
                    )
                ],
                storage_dir=self.storage_dir,
                definition_version=2,
                compatibility_policy=CompatibilityPolicy.STRICT,
            )

    def test_allow_implementation_upgrade_policy_permits_code_upgrade_and_logs_migration(
        self,
    ) -> None:
        crashed = False

        def crash_after_t1(stage: str, rec: dict, _seq: int) -> None:
            nonlocal crashed
            if (
                not crashed
                and stage == "AFTER_FSYNC"
                and rec.get("event_type") == "TASK_STATE_CHANGED"
                and rec["data"].get("task_id") == "t1"
                and rec["data"].get("to_state") == TaskState.SUCCEEDED.value
            ):
                crashed = True
                raise SimulatedProcessCrash("Crash before t2 runs")

        wf = create_workflow(
            [
                TaskSpec("t1", func=lambda: 10, implementation_version="1.0"),
                TaskSpec(
                    "t2",
                    func=lambda _p, ctx: ctx.dependency_results["t1"] + 5,
                    dependencies=["t1"],
                    implementation_version="1.0",
                ),
            ],
            max_concurrency=1,
            workflow_id="wf_impl_upgrade",
            storage_dir=self.storage_dir,
            clock=self.clock,
            crash_hook=crash_after_t1,
            definition_version=1,
        )
        with self.assertRaises(SimulatedProcessCrash):
            run_workflow(wf)

        # Upgrade t2 implementation_version to 2.0 and definition_version to 2
        upgraded_tasks = [
            TaskSpec("t1", func=lambda: 10, implementation_version="1.0"),
            TaskSpec(
                "t2",
                func=lambda _p, ctx: ctx.dependency_results["t1"] * 3,
                dependencies=["t1"],
                implementation_version="2.0",
            ),
        ]
        summary = recover_workflow(
            "wf_impl_upgrade",
            tasks=upgraded_tasks,
            storage_dir=self.storage_dir,
            clock=self.clock,
            definition_version=2,
            compatibility_policy=CompatibilityPolicy.ALLOW_IMPLEMENTATION_UPGRADE,
        )
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(summary.definition_version, 2)
        self.assertEqual(get_task_result("wf_impl_upgrade", "t2").result, 30)
        migration_events = [
            e
            for e in summary.structured_logs
            if e.event_type == "DEFINITION_MIGRATED"
        ]
        self.assertEqual(len(migration_events), 1)

        # Even under ALLOW_IMPLEMENTATION_UPGRADE, changing dependencies or payload is rejected
        with self.assertRaises(WorkflowDefinitionMismatchError):
            recover_workflow(
                "wf_impl_upgrade",
                tasks=[
                    TaskSpec(
                        "t1",
                        func=lambda: 10,
                        input_payload={"changed": True},
                        implementation_version="1.0",
                    ),
                    TaskSpec(
                        "t2",
                        func=lambda: 30,
                        dependencies=["t1"],
                        implementation_version="2.0",
                    ),
                ],
                storage_dir=self.storage_dir,
                definition_version=2,
                compatibility_policy=CompatibilityPolicy.ALLOW_IMPLEMENTATION_UPGRADE,
            )

    def test_legacy_v1_log_compatibility_and_explicit_migration(self) -> None:
        # Construct a genuine v1-schema log record (no schema_version or definition_hash)
        wf = WorkflowEngine(
            tasks=[TaskSpec("legacy_task", func=lambda: "legacy_ok")],
            workflow_id="wf_legacy_v1",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        wf.validate()
        v1_meta = {"legacy_task": wf._task_specs["legacy_task"].to_v1_metadata_dict()}
        wf._event_log.append(
            "WORKFLOW_CREATED",
            {"max_concurrency": 1, "tasks": v1_meta},
        )
        wf._event_log.append(
            "TASK_STATE_CHANGED",
            {
                "task_id": "legacy_task",
                "from_state": "PENDING",
                "to_state": "READY",
                "reason": "ROOT_TASK_ELIGIBLE",
                "attempt": 0,
                "claim_epoch": 0,
                "recovery_count": 0,
            },
        )

        # Attempting to recover with definition_version=2 under STRICT raises LegacyLogMigrationError
        with self.assertRaises(LegacyLogMigrationError):
            recover_workflow(
                "wf_legacy_v1",
                tasks=[
                    TaskSpec(
                        "legacy_task",
                        func=lambda: "legacy_ok",
                        implementation_version="2.0",
                    )
                ],
                storage_dir=self.storage_dir,
                clock=self.clock,
                definition_version=2,
                compatibility_policy=CompatibilityPolicy.STRICT,
            )

        # Recovering with MIGRATE_LEGACY_V1 succeeds and logs DEFINITION_MIGRATED
        summary = recover_workflow(
            "wf_legacy_v1",
            tasks=[
                TaskSpec(
                    "legacy_task",
                    func=lambda: "legacy_ok",
                    implementation_version="2.0",
                )
            ],
            storage_dir=self.storage_dir,
            clock=self.clock,
            definition_version=2,
            compatibility_policy=CompatibilityPolicy.MIGRATE_LEGACY_V1,
        )
        self.assertEqual(summary.status, WorkflowStatus.SUCCEEDED)
        self.assertEqual(
            get_task_result("wf_legacy_v1", "legacy_task").result, "legacy_ok"
        )

    def test_recovery_rejects_missing_task_definitions(self) -> None:
        wf = create_workflow(
            [TaskSpec("t1", func=lambda: 1)],
            workflow_id="wf_non_empty",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        run_workflow(wf)

        # Attempt to recover a non-empty persisted workflow using an engine with tasks=[]
        empty_engine = WorkflowEngine(
            tasks=[],
            workflow_id="wf_non_empty",
            storage_dir=self.storage_dir,
            clock=self.clock,
        )
        with self.assertRaises(MissingWorkflowDefinitionError):
            empty_engine.recover()

        # Attempt to recover an unregistered workflow ID without passing tasks
        with self.assertRaises(WorkflowNotFoundError):
            recover_workflow("wf_unregistered_id", storage_dir=self.storage_dir)


if __name__ == "__main__":
    unittest.main()
