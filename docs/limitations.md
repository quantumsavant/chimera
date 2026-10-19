# Known Limitations & Platform Assumptions

CHIMERA is a single-host, standard-library workflow engine designed for clarity, determinism, and verifiable crash recovery. It intentionally does not claim distributed execution or hardware-fault immunity where Python or the OS cannot guarantee them.

## 1. External Side-Effect vs. Local Commit Gap
Between the moment a task callable performs an external side effect (e.g., mutating an external file or calling an HTTP API) and the moment CHIMERA writes and `fsync`s the `TASK_STATE_CHANGED(to_state="SUCCEEDED")` frame to the local log, a process crash or power loss can occur.
- Upon `recover_workflow()`, any task left in `RUNNING` (without a committed `SUCCEEDED` frame) is reset to `READY` (`RECOVERED_FROM_CRASH`) and re-executed (**at-least-once execution prior to durable commit**).
- To achieve effective once-only external mutations, tasks must deduplicate external operations using `ctx.idempotency_key` (`<workflow_id>:<task_id>`).

## 2. Thread-Based Worker Execution & Uncooperative Blocking Calls
Tasks execute in daemon `threading.Thread` workers within the host Python process.
- If a task times out while blocked in an uncooperative C extension or infinite loop, CHIMERA fences off its `ClaimToken` and spawns a replacement worker so the workflow continues, but the timed-out background thread remains in the process until it returns or the process exits.
- Removing this limitation requires running each task attempt in a separate `multiprocessing` or `subprocess` worker that can be terminated via `SIGKILL`.

## 3. JSON-Serializable Payloads and Results Across Recovery
During live execution, `input_payload` and task results are preserved in memory via `copy.deepcopy`. For durable persistence in `<workflow_id>.chimera.log`, payloads and results must be JSON-serializable; non-JSON-serializable objects degrade to `repr(val)` strings when replayed from disk.

## 4. Operating-System File Locking & `fsync` Semantics
- **POSIX (Linux / macOS)**: Uses `fcntl.flock(LOCK_EX | LOCK_NB)` and `os.fsync` on both the log file descriptor and parent directory descriptor. Network filesystems (such as older NFS mounts without local lockd support) may not honor `flock`.
- **Windows (`nt`)**: All low-level `os.open()` calls pass `O_BINARY_FLAG` (`getattr(os, "O_BINARY", 0)`) so the Windows CRT never translates `\n` (`0x0A`) to `\r\n` (`0x0D 0x0A`). Uses `msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)` on the lock file; parent-directory `os.fsync` is skipped on Windows where opening directories with `os.open` is not supported.
- **Storage Hardware**: True power-loss durability requires the underlying kernel and disk controller to honor `fsync` cache flushes.
