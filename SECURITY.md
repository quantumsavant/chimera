# Security Policy & Boundaries (`v0.2.0`)

## 1. Safe Canonical JSON Serialization (No `pickle`)
CHIMERA never uses `pickle`, `marshal`, or `eval` when persisting or replaying workflow logs or communicating with child worker processes.
- All event log frames and child-process IPC messages use strict canonical UTF-8/ASCII JSON (`allow_nan=False`, `ensure_ascii=True`, `sort_keys=True`) with configurable size caps (`max_payload_bytes = 4 MiB`, `max_frame_bytes = 8 MiB`).
- Task callables (`TaskSpec.func`) are resolved by importing top-level module attributes (`module_name`, `qualname`) and are never deserialized from raw bytecode on disk.

## 2. Filesystem Path Validation & Restrictive Permissions
- **Workflow ID Validation**: `validate_workflow_id()` enforces `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$` and rejects `..`, `/`, `\`, NUL bytes, and Windows reserved device names (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9`, `LPT1`–`LPT9`), preventing path traversal outside `storage_dir`.
- **Restrictive File Modes**: By default, `<workflow_id>.chimera.log` and `<workflow_id>.chimera.lock` are created with mode `0o600` (owner read/write only), and `storage_dir` is created with mode `0o700`.

## 3. Operational Log Redaction vs. Authoritative Event Log
- By default (`log_sensitive_payloads=False`), operational `StructuredLogEntry.details` omit raw task `result` contents, recording only `has_result` and `result_type`.
- Because the authoritative `EventLog` (`<workflow_id>.chimera.log`) must persist task `input_payload` and `result` values for deterministic crash recovery, do not pass plaintext credentials, private keys, or bearer tokens inside `input_payload` or task return values—pass secret identifiers and resolve them inside the task callable.

## 4. CRC32 vs. Optional HMAC-SHA256 (`@CHIMERA2`) Integrity
- `@CHIMERA1` frames use 32-bit CRC32 checksums designed to detect accidental storage corruption (bit flips, torn writes), **not** intentional tampering by an attacker with write access to `storage_dir`.
- When local tamper detection is required, pass `hmac_key=b"..."` (at least 32 bytes from a KMS or environment secret) to enable `@CHIMERA2` framing, which authenticates each record with a chained HMAC-SHA256 tag (`prev_mac` + `seq` + `crc32` + `payload`). Note that while chained HMAC-SHA256 detects any record modification, insertion, or interior deletion, an attacker with full disk write access can still truncate the entire file to an older valid prefix or delete it unless combined with external monotonic state.

## 5. Process Backend Isolation Boundaries
`ExecutionBackend.PROCESS` runs tasks in separate OS child processes (`multiprocessing.get_context("spawn")`) so timed-out or crashing tasks (`SIGSEGV`, `os._exit`, infinite CPU loops) can be forcibly terminated (`SIGTERM` $\rightarrow$ `SIGKILL`) without corrupting the parent Python interpreter.
- **Not a Security Sandbox**: A child OS process still runs under the same OS user UID/GID and inherits filesystem and network access. Executing untrusted third-party code requires OS-level sandboxing (Linux namespaces, cgroups v2, `seccomp-bpf`, or isolated microVMs).
