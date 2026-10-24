# CHIMERA Reproducible Benchmark Report

> **Important Note**: Benchmarks measure execution latency, I/O overhead, and memory footprint under controlled workloads. They are **not** a substitute for correctness verification (see `tests/` and `docs/testing.md`). All numbers below were genuinely executed in the build environment using `python3 scripts/run_benchmarks.py`.

## 1. Test Environment

| Parameter | Measured Value |
| :--- | :--- |
| **Python Runtime** | `CPython 3.11.2` |
| **Operating System / Kernel** | `Linux 4.19.0-gvisor x86_64 (glibc 2.36)` |
| **Logical CPUs (`os.cpu_count()`)** | `2` |
| **Storage Backing** | Container local filesystem (`os.fsync` enabled on every frame) |
| **Command Executed** | `python3 scripts/run_benchmarks.py --json` |

## 2. Scheduling & Linear DAG Validation Overhead

**Workload**: A 100-task linear dependency chain (`t_0 -> t_1 -> ... -> t_99`), validated with `validate_workflow()` (iterative 3-color DFS) and executed to completion with durable `EventLog` persistence (`302` total framed event records).

| Metric | Measured Value |
| :--- | :--- |
| **100-Task DAG Validation (`validate_workflow`)** | `0.218 ms` |
| **Total 100-Task Execution Time (with `fsync`)** | `196.929 ms` |
| **Mean Per-Task Orchestration + Persistence Overhead** | `1.969 ms / task` |
| **Total Committed Event Log Frames** | `302 records` |

## 3. Task Throughput vs. Concurrency (`ExecutionBackend.THREAD`)

**Workload**: `64` independent CPU/memory tasks (`max_concurrency` varied across `1`, `2`, `4`, `8`), including `WORKFLOW_CREATED`, `64` `PENDING -> READY -> RUNNING -> SUCCEEDED` transitions, and `WORKFLOW_COMPLETED` (`194` `fsync`ed log records per run).

| `max_concurrency` | Total Elapsed Time (`ms`) | Throughput (`tasks / sec`) |
| :---: | :---: | :---: |
| `1` | `122.95 ms` | `520.5 tasks/s` |
| `2` | `117.99 ms` | `542.4 tasks/s` |
| `4` | `110.02 ms` | `581.7 tasks/s` |
| `8` | `121.72 ms` | `525.8 tasks/s` |

*Analysis*: Because every task transition (`READY -> RUNNING` and `RUNNING -> SUCCEEDED`) acquires the workflow file lock and calls `os.fsync(fd)` on the append-only log, throughput for sub-millisecond in-memory tasks is bounded by serialized `fsync` I/O (~0.3 ms/frame) and the 2-core container environment.

## 4. Event-Log Append & `fsync` Latency (`@CHIMERA1` vs. `@CHIMERA2`)

**Workload**: `100` consecutive `EventLog.append()` calls with `os.fsync()` on every frame, comparing `@CHIMERA1` (39-byte header with CRC32) against `@CHIMERA2` (104-byte header with CRC32 + chained HMAC-SHA256).

| Frame Format | Samples | Mean (`ms`) | Median `p50` (`ms`) | `p95` (`ms`) | Max (`ms`) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **`@CHIMERA1` (CRC32)** | `100` | `0.294 ms` | `0.269 ms` | `0.452 ms` | `1.050 ms` |
| **`@CHIMERA2` (CRC32 + Chained HMAC-SHA256)** | `100` | `0.620 ms` | `0.363 ms` | `2.286 ms` | `2.999 ms` |

## 5. Crash Recovery Replay Time vs. Log Length

**Workload**: Replaying an on-disk `.chimera.log` file, verifying CRC32 checksums, canonical JSON schemas, `definition_hash`, and state-machine invariants (`recover_workflow(..., auto_run=False)`).

| Tasks in Workflow | Log Records (`seq`) | Log File Size (`bytes`) | Recovery Time (`ms`) | Replay Rate (`records / ms`) |
| :---: | :---: | :---: | :---: | :---: |
| `15` | `47` | `19,064 B` | `4.982 ms` | `9.4 records/ms` |
| `60` | `182` | `74,872 B` | `16.297 ms` | `11.2 records/ms` |
| `150` | `452` | `187,209 B` | `35.986 ms` | `12.6 records/ms` |

## 6. Heap Memory Footprint (`tracemalloc`)

**Workload**: A 200-task sequential DAG storing structured dictionary results and `602` structured log entries in memory.

| Metric | Measured Value |
| :--- | :--- |
| **Workflow Size** | `200 tasks` (`602` log records) |
| **Peak Traced Memory Delta** | `1,241.70 KiB` (~1.21 MiB) |
| **Retained Memory Delta After Completion** | `936.53 KiB` |
| **Peak Traced Memory per Task** | `6,357.5 bytes / task` |

## 7. Process-Isolated Backend (`spawn`) & Hard Timeout Cleanup Latency

**Workload**:
1. Executing `4` sequential tasks in isolated child processes using `multiprocessing.get_context("spawn")`.
2. Enforcing a `100.0 ms` (`0.10s`) hard timeout on an uncooperative child process executing `while True: time.sleep(0.02)`, escalating through cooperative event $ightarrow$ `SIGTERM` (`terminate()`) $ightarrow$ `SIGKILL` (`kill()`) and `proc.join()` reaping.

| Metric | Measured Value |
| :--- | :--- |
| **Mean `spawn` Child Process Task Latency** | `307.59 ms / task` |
| **Configured Task Timeout** | `100.0 ms` |
| **Total Wall Time to Timeout + Kill + Reap** | `144.74 ms` |
| **Termination Escalation & Process Reap Overhead** | `44.74 ms` |
| **Active Child Processes Remaining After Cleanup** | `0` |

## 8. Limitations of Benchmark Measurements
- **Virtualized Container I/O**: In a virtualized Linux container (`gvisor`), `os.fsync()` latency reflects the hypervisor virtual filesystem flush rather than physical NVMe/SSD controller cache flush times (which typically range from `0.5 ms` to `5 ms` on bare-metal enterprise SSDs).
- **`spawn` Cold-Start Cost**: Because `ProcessTaskExecutor` spawns a fresh OS process per task attempt (`multiprocessing.get_context("spawn")`) for strict memory and fault isolation, each process-backed task incurs ~150–300 ms of Python interpreter startup overhead. Use `ExecutionBackend.THREAD` for sub-millisecond trusted tasks and `ExecutionBackend.PROCESS` for untrusted, crash-prone, or hard-deadline tasks.
