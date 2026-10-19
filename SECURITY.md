# Security Policy & Considerations

## Safe Serialization (No `pickle`)
CHIMERA never uses `pickle`, `marshal`, or `eval` when persisting or replaying workflow logs. All event log payloads are serialized as strict ASCII JSON (`json.dumps(..., ensure_ascii=True, sort_keys=True)`) protected by a 32-bit CRC32 frame checksum.

## Local Filesystem Trust Boundary
- Workflow logs (`<workflow_id>.chimera.log`) and lock files (`<workflow_id>.chimera.lock`) are created with mode `0o644` inside the caller-specified `storage_dir`.
- Do not point `storage_dir` at a world-writable shared directory without appropriate directory permissions (such as sticky bit `1777` or a private user directory `0700`).
- Task callables (`TaskSpec.func`) are supplied in-memory by the host Python application and are never deserialized from disk.

## Reporting a Vulnerability
If you discover a security or correctness issue, please open a detailed issue with a minimal reproducible test case using `unittest`.
