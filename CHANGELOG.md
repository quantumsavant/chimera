# Changelog

All notable changes to CHIMERA are documented in this file.

## [0.1.0] - 2026-10-15

### Added
- Modular `src/chimera/` package architecture (`models`, `validation`, `scheduler`, `persistence`, `recovery`, `locking`, `clock`, `exceptions`, `engine`).
- Iterative 3-color DFS DAG validator detecting duplicate task IDs, missing dependencies, and arbitrarily deep dependency cycles with exact cycle paths.
- Framed, CRC32-checksummed append-only event log (`@CHIMERA1|<seq:010d>|<len:08d>|<crc32:08x>|<json>\n`) with automatic tail-truncation repair (`os.ftruncate` + `os.fsync`) and interior corruption rejection (`LogCorruptionError`).
- Monotonic `ClaimToken` (`claim_epoch`) fencing to prevent duplicate task claims and reject stale completions from timed-out, cancelled, or superseded workers.
- Cross-process (`fcntl.flock` on POSIX, `msvcrt.locking` on Windows) and in-process (`_ACTIVE_IN_PROCESS_LOCKS`) exclusive workflow file locking (`WorkflowFileLock`).
- Injectable `Clock` interface (`SystemClock` and deterministic `VirtualClock`) for zero-sleep testing of timeouts and exponential backoff retries.
- Full `TaskSpec` metadata verification during `recover_workflow` to detect altered payloads, retry budgets, timeouts, or dependency graphs.
