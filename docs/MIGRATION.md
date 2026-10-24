# Migration Guide: Upgrading CHIMERA from `v0.1.x` to `v0.2.0`

CHIMERA `v0.2.0` preserves backward compatibility for standard workflow creation, execution, cancellation, and recovery while hardening durability, process isolation, definition identity, and filesystem safety. This guide documents every behavioral refinement and how to migrate existing code and event logs.

## 1. Summary of Behavioral Changes

| Area | `v0.1.x` Behavior | `v0.2.0` Behavior | Action Required |
| :--- | :--- | :--- | :--- |
| **Durable Payloads & Results** | Non-JSON types (`set`, `object()`, `NaN`) fell back to `repr(val)` strings in the event log. | Strictly validated as canonical RFC 8259 JSON (`allow_nan=False`). Unsupported types raise `PayloadSerializationError` (`WorkflowValidationError`). | Convert `set` to `list`/`tuple` and custom objects to `dict` before passing as `input_payload` or returning from tasks. |
| **Workflow Identifier (`workflow_id`)** | Any non-empty string accepted. | Validated against `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`; rejects `..`, `/`, `\`, spaces, and Windows reserved device names (`CON`, `NUL`, etc.) with `InvalidWorkflowIdError`. | Use alphanumeric workflow IDs with `-`, `_`, or `.`. |
| **Default File Permissions** | Log and lock files created with `0o644`. | Log and lock files created with `0o600` (owner read/write only); directories created with `0o700`. | Pass `file_mode=0o644` to `create_workflow()` only if shared group/world read access is intentionally required. |
| **Operational Log Redaction** | `StructuredLogEntry.details` included raw task `result` values. | Raw `result` values are omitted from `StructuredLogEntry.details` by default (`log_sensitive_payloads=False`), recording `has_result=True` and `result_type` instead. | Use `get_task_result(wf_id, task_id).result` to read task outputs, or pass `log_sensitive_payloads=True` if raw results are needed in operational logs. |
| **Workflow Definition Hashing** | `WORKFLOW_CREATED` stored task metadata without `definition_hash` or `implementation_version`. | `WORKFLOW_CREATED` stores `schema_version=2`, `definition_version`, canonical SHA-256 `definition_hash`, and per-task `implementation_version`. | Existing `v0.1.x` logs with default `implementation_version="1.0"` recover automatically; use `CompatibilityPolicy.MIGRATE_LEGACY_V1` when upgrading task versions simultaneously. |

## 2. Migrating Legacy `v0.1.x` Event Logs

### 2.1 Recovering Existing `@CHIMERA1` Logs with Unchanged Task Definitions
If your task definitions (`input_payload`, `dependencies`, retry/timeout settings) have not changed, `recover_workflow()` automatically accepts legacy `v0.1.x` logs without any code changes:

```python
summary = recover_workflow("wf_existing_v1", tasks=tasks, storage_dir=storage_dir)
```

### 2.2 Upgrading Task Implementation Versions During Recovery
When you deploy updated code for a task callable and bump `TaskSpec(..., implementation_version="2.0")` or `definition_version=2`:
- Under `CompatibilityPolicy.STRICT` (the default), `recover_workflow()` raises `WorkflowDefinitionMismatchError` (or `LegacyLogMigrationError` on a v1 log) so accidental code drift never goes unnoticed.
- To explicitly authorize the implementation upgrade while still verifying that the DAG topology, `input_payload`, and retry settings are unchanged, pass `CompatibilityPolicy.ALLOW_IMPLEMENTATION_UPGRADE` (or `CompatibilityPolicy.MIGRATE_LEGACY_V1`):

```python
from chimera import CompatibilityPolicy, recover_workflow

summary = recover_workflow(
    "wf_existing_v1",
    tasks=upgraded_tasks,
    storage_dir=storage_dir,
    definition_version=2,
    compatibility_policy=CompatibilityPolicy.ALLOW_IMPLEMENTATION_UPGRADE,
)
```
This appends an auditable `DEFINITION_MIGRATED` event to the workflow log before resuming execution.

### 2.3 Upgrading `@CHIMERA1` Logs to `@CHIMERA2` Authenticated HMAC-SHA256 Framing
To upgrade an existing `@CHIMERA1` log file in place to `@CHIMERA2` (which adds a 64-hex-character chained HMAC-SHA256 tag to every frame):

```python
from chimera import EventLog

log = EventLog(log_path="/path/to/wf_prod.chimera.log", workflow_id="wf_prod")
migrated_records = log.migrate_v1_to_v2(hmac_key=b"your-32-byte-secret-hmac-key")
```
`migrate_v1_to_v2()` replays and verifies all frames, writes the `@CHIMERA2` frames to a temporary file, calls `os.fsync()`, and atomically replaces the log file via `os.replace()`.

## 3. Adopting Process-Isolated Execution (`ExecutionBackend.PROCESS`)

In `v0.1.x`, all tasks executed in daemon threads (`ExecutionBackend.THREAD`). In `v0.2.0`, you can opt into process-isolated execution either for the entire workflow or per task:

```python
from chimera import ExecutionBackend, TaskSpec, create_workflow

def top_level_heavy_task(payload: dict) -> dict:
    # Must be a top-level importable module function (not a lambda or closure)
    return {"processed": payload["items"]}

wf = create_workflow(
    [
        TaskSpec(
            task_id="heavy_step",
            func=top_level_heavy_task,
            input_payload={"items": [1, 2, 3]},
            timeout_seconds=5.0,
            execution_backend=ExecutionBackend.PROCESS,
        )
    ],
    storage_dir="/var/lib/chimera",
)
```
