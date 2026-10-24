# Changelog

## [0.2.0] - 2026-10-19
### Added
- **Supervised Process Execution Backend (`ExecutionBackend.PROCESS`)**: Explicit `multiprocessing.get_context("spawn")` child-process execution (`src/chimera/executor.py`) with dedicated per-worker pipes, monotonic deadline enforcement, 3-stage `cancel -> terminate() -> kill()` escalation, zombie process reaping, and `max_output_bytes` enforcement.
- **Canonical Lossless Serialization (`src/chimera/serialization.py`)**: Strict RFC 8259 JSON validation (`allow_nan=False`), removal of silent `repr()` fallbacks, `PayloadSerializationError`, `OversizedPayloadError`, and `OversizedFrameError`.
- **Workflow Definition Identity & Versioning (`src/chimera/recovery.py`)**: Explicit `definition_version`, canonical SHA-256 `definition_hash`, per-task `implementation_version`, and `CompatibilityPolicy` (`STRICT`, `ALLOW_IMPLEMENTATION_UPGRADE`, `MIGRATE_LEGACY_V1`).
- **Authenticated `@CHIMERA2` Event Log Format**: Optional chained HMAC-SHA256 frame authentication (`hmac_key`) and atomic `EventLog.migrate_v1_to_v2()` migration utility.
- **Idempotency & Retry Enhancements**: Explicit `TaskSpec(operation_key=...)`, `ctx.attempt_idempotency_key`, deterministic seeded backoff jitter (`jitter_seconds`, `jitter_seed`), and runnable external API and SQLite transactional deduplication examples.
- **Operational Diagnostics & Metrics (`src/chimera/diagnostics.py`, `scripts/inspect_workflow.py`)**: `FailureCategory` classification, per-task `queue_delay_seconds` and `execution_duration_seconds`, `WorkflowMetrics`, `MetricsSink` (`InMemoryMetricsSink`), default redaction of sensitive task outputs in operational logs, and read-only CLI log inspection.
- **Filesystem & Lock Hardening**: `validate_workflow_id()` path-traversal and Windows reserved-name protection, default `0o600` file and `0o700` directory modes, and fork-safe PID tracking in `WorkflowFileLock`.
- **CI, Benchmarks & Engineering Docs**: Multi-OS/multi-Python GitHub Actions workflow (`.github/workflows/ci.yml`), `scripts/run_benchmarks.py`, `docs/ENGINEERING_ASSESSMENT.md`, `docs/BENCHMARKS.md`, `docs/MIGRATION.md`, and `docs/DISTRIBUTED_ARCHITECTURE_PROPOSAL.md`.

## [0.1.0] - 2026-10-14
### Added
- Initial release of CHIMERA fault-tolerant DAG workflow engine with `@CHIMERA1` CRC32 event log, monotonic `ClaimToken` fencing, cross-process file locking (`O_BINARY` safe on Windows), and 43-test adversarial verification suite.
