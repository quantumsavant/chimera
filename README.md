# CHIMERA — Fault-Tolerant DAG Workflow Engine (`v0.2.0`)

CHIMERA is a zero-runtime-dependency, crash-recoverable Directed Acyclic Graph (DAG) workflow orchestration engine written in pure Python (`3.11+` standard library only). It is engineered as reliability-critical systems software with event-sourced persistence, CRC32 and optional chained HMAC-SHA256 framed logs, monotonic fencing tokens, process-isolated hard timeouts (`spawn`), canonical SHA-256 workflow definition hashing, cross-process file locking, and a 69-test deterministic verification suite.

## Verified Capabilities vs. Non-Goals

| Capability | Status | Implementation Details |
| :--- | :--- | :--- |
| **DAG Validation & Cycle Detection** | **Verified** | Rejects duplicate IDs, missing dependencies, and deep cycles via iterative 3-color DFS (`CycleDetectedError` with exact cycle path). |
| **Dual Execution Backends (`THREAD` & `PROCESS`)** | **Verified** | `ExecutionBackend.THREAD` for fast cooperative tasks; `ExecutionBackend.PROCESS` (`multiprocessing.get_context("spawn")`) for enforceable hard deadlines (`cancel -> terminate() -> kill()`) and crash isolation. |
| **Monotonic Fencing (`ClaimToken`)** | **Verified** | Every claim, timeout, cancellation, and recovery increments `claim_epoch`. Timed-out attempts are durably fenced in the log *before* child processes are killed or replacement attempts start. |
| **Lossless Canonical JSON Persistence** | **Verified** | Strict RFC 8259 JSON (`allow_nan=False`, no `repr()` fallbacks) with configurable `max_payload_bytes` (`4 MiB`), `max_output_bytes`, and `max_frame_bytes` (`8 MiB`). |
| **Framed Event Log (`@CHIMERA1` & `@CHIMERA2`)** | **Verified** | `@CHIMERA1` (CRC32) and `@CHIMERA2` (CRC32 + chained HMAC-SHA256) framing; repairs truncated final records (`os.ftruncate` + `os.fsync`) while raising `LogCorruptionError` on any interior or semantic corruption. |
| **Workflow Definition Hashing & Migration** | **Verified** | Canonical SHA-256 `definition_hash`, `definition_version`, per-task `implementation_version`, and `CompatibilityPolicy` (`STRICT`, `ALLOW_IMPLEMENTATION_UPGRADE`, `MIGRATE_LEGACY_V1`). |
| **Retries, Seeded Jitter & Idempotency Keys** | **Verified** | Bounded exponential backoff with optional deterministic jitter (`jitter_seconds`, `jitter_seed`), `<workflow_id>:<task_id>`, custom `operation_key`, and `attempt_idempotency_key`. |
| **Cross-Process File Locking & Path Safety** | **Verified** | `WorkflowFileLock` (`fcntl.flock` on POSIX, `msvcrt.locking` on Windows with `O_BINARY`, fork-safe PID guard, `0o600` file permissions, and `validate_workflow_id` path-traversal protection). |
| **Operational Diagnostics, Metrics & CLI Inspector** | **Verified** | `FailureCategory`, per-task queue/execution latency, `WorkflowMetrics`, `MetricsSink`, payload redaction, and read-only `scripts/inspect_workflow.py` CLI. |
| **Multi-Node Distributed Execution** | **Design Proposal Only** | Single-host engine; see [`docs/DISTRIBUTED_ARCHITECTURE_PROPOSAL.md`](docs/DISTRIBUTED_ARCHITECTURE_PROPOSAL.md) for the multi-node PostgreSQL/fencing architecture design. |

## Quick Start & Installation

Requires **Python 3.11+** (tested on Python 3.11, 3.12, and 3.13; zero third-party runtime dependencies).

```bash
git clone https://github.com/quantumsavant/chimera.git
cd chimera
python3 -m pip install -e .
```

Run the complete verification suite (bytecode compilation, 69 unit/integration/adversarial tests, and all 6 runnable examples) from a clean checkout:
```bash
python3 scripts/run_tests.py
```

## Usage Examples

### 1. Thread Backend vs. Process Backend with Hard Timeouts
```python
import tempfile
import time
from chimera import (
    ExecutionBackend,
    TaskContext,
    TaskSpec,
    create_workflow,
    get_task_result,
    run_workflow,
)

def top_level_worker(payload: dict, ctx: TaskContext) -> str:
    if ctx.attempt == 1:
        # Uncooperative infinite loop on attempt 1 -> killed by ProcessTaskExecutor
        while True:
            time.sleep(0.01)
    return f"completed_by_attempt_{ctx.attempt}_key_{ctx.idempotency_key}"

with tempfile.TemporaryDirectory() as tmpdir:
    wf = create_workflow(
        [
            TaskSpec(
                task_id="enforced_deadline_task",
                func=top_level_worker,
                input_payload={"job": 42},
                timeout_seconds=0.8,
                max_retries=1,
                retry_on_timeout=True,
                operation_key="billing:job_42",
                implementation_version="1.0.0",
            )
        ],
        max_concurrency=1,
        workflow_id="wf_demo_proc",
        storage_dir=tmpdir,
        execution_backend=ExecutionBackend.PROCESS,
    )
    summary = run_workflow(wf)
    result = get_task_result(wf.workflow_id, "enforced_deadline_task")
    print(summary.status.value, result.result, summary.metrics.to_dict())
```

### 2. Runnable Example Scripts (`examples/`)
- `python3 examples/basic_workflow.py` — Diamond DAG with concurrent branches and payload isolation.
- `python3 examples/retry_example.py` — Deterministic exponential backoff and exception filtering with `VirtualClock`.
- `python3 examples/recovery_example.py` — Crash recovery from a mid-write truncated final log record without re-executing committed tasks.
- `python3 examples/process_isolation_example.py` — Supervised `PROCESS` backend killing an uncooperative infinite loop and recovering on retry.
- `python3 examples/idempotent_api_example.py` — External API side-effect deduplication using `ctx.idempotency_key` across a pre-commit crash.
- `python3 examples/transactional_deduplication_example.py` — Application-level ACID deduplication using standard-library `sqlite3`.

### 3. Read-Only Log Inspection CLI & Reproducible Benchmarks
```bash
# Inspect any .chimera.log file in read-only mode (human-readable or --json)
python3 scripts/inspect_workflow.py /path/to/wf_id.chimera.log --json

# Run the reproducible performance & resource benchmark suite
python3 scripts/run_benchmarks.py
```

## Documentation Index

- [`docs/ENGINEERING_ASSESSMENT.md`](docs/ENGINEERING_ASSESSMENT.md) — Phase 1 Architecture, Correctness & Risk Audit (Findings F-01 through F-09).
- [`docs/architecture.md`](docs/architecture.md) — Module responsibilities, state machine, and core invariants ($I_1$–$I_5$).
- [`docs/design_decisions.md`](docs/design_decisions.md) — Framing, fencing tokens, per-process IPC pipes, and locking trade-offs.
- [`docs/limitations.md`](docs/limitations.md) — External side-effect delivery semantics, OS process isolation limits, and `fsync` hardware assumptions.
- [`docs/MIGRATION.md`](docs/MIGRATION.md) — Upgrading from `v0.1.x` to `v0.2.0` and migrating `@CHIMERA1` logs to `@CHIMERA2`.
- [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) — Actual measured scheduling, `fsync`, recovery, memory, and process-timeout benchmarks.
- [`docs/DISTRIBUTED_ARCHITECTURE_PROPOSAL.md`](docs/DISTRIBUTED_ARCHITECTURE_PROPOSAL.md) — Multi-node distributed backend design proposal (PostgreSQL vs. Broker vs. Consensus Log).
- [`SECURITY.md`](SECURITY.md) — Canonical JSON safety, `0o600` permissions, HMAC-SHA256 framing, and secret handling.
