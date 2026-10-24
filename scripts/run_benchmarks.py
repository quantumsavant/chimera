"""
Reproducible performance and reliability micro-benchmark suite for CHIMERA.

Measures:
1. Scheduling & DAG validation overhead (100-task DAG).
2. Task throughput across concurrency levels (max_concurrency = 1, 2, 4, 8).
3. Event-log append latency with fsync (@CHIMERA1 and @CHIMERA2 HMAC-SHA256).
4. Crash recovery replay time vs. log length (50, 200, and 500 records).
5. Peak heap memory footprint (tracemalloc) for a 200-task workflow.
6. Child process startup & IPC round-trip latency (ProcessTaskExecutor).
7. Hard timeout detection, process kill escalation, and zombie cleanup latency.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import statistics
import sys
import tempfile
import time
import tracemalloc
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from chimera import (
    EventLog,
    ExecutionBackend,
    TaskSpec,
    WorkflowStatus,
    create_workflow,
    recover_workflow,
    run_workflow,
    validate_workflow,
)


def top_level_noop_task(payload: dict[str, int]) -> int:
    """Importable top-level callable for process benchmark."""
    return payload.get("x", 0) + 1


def top_level_infinite_sleep(_payload: Any) -> None:
    """Importable top-level callable that blocks until terminated by timeout."""
    while True:
        time.sleep(0.02)


def measure_scheduling_overhead(num_tasks: int = 100) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmpdir:
        tasks: list[TaskSpec] = []
        for i in range(num_tasks):
            deps = [f"t_{i - 1}"] if i > 0 else []
            tasks.append(
                TaskSpec(
                    task_id=f"t_{i}",
                    func=lambda p: p["idx"] + 1,
                    input_payload={"idx": i},
                    dependencies=deps,
                )
            )
        t0 = time.perf_counter()
        validate_workflow(tasks)
        val_ms = (time.perf_counter() - t0) * 1000.0

        t1 = time.perf_counter()
        wf = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="bench_sched_100",
            storage_dir=tmpdir,
        )
        summary = run_workflow(wf)
        run_ms = (time.perf_counter() - t1) * 1000.0
        assert summary.status == WorkflowStatus.SUCCEEDED
        return {
            "num_tasks": num_tasks,
            "dag_validation_ms": round(val_ms, 3),
            "total_execution_ms": round(run_ms, 3),
            "per_task_overhead_ms": round(run_ms / num_tasks, 3),
            "log_records": summary.log_record_count,
        }


def measure_throughput_by_concurrency(
    num_tasks: int = 64,
    concurrency_levels: tuple[int, ...] = (1, 2, 4, 8),
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for conc in concurrency_levels:
        with tempfile.TemporaryDirectory() as tmpdir:
            tasks = [
                TaskSpec(
                    task_id=f"par_{i:03d}",
                    func=lambda p: p["v"] * 2,
                    input_payload={"v": i},
                )
                for i in range(num_tasks)
            ]
            t0 = time.perf_counter()
            wf = create_workflow(
                tasks,
                max_concurrency=conc,
                workflow_id=f"bench_conc_{conc}",
                storage_dir=tmpdir,
            )
            summary = run_workflow(wf)
            elapsed_s = time.perf_counter() - t0
            assert summary.status == WorkflowStatus.SUCCEEDED
            results.append(
                {
                    "max_concurrency": conc,
                    "num_tasks": num_tasks,
                    "elapsed_ms": round(elapsed_s * 1000.0, 2),
                    "tasks_per_sec": round(num_tasks / max(elapsed_s, 1e-6), 1),
                }
            )
    return results


def measure_event_log_append_latency(samples: int = 100) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for label, fmt_ver, key in (
        ("v1_crc32", 1, None),
        ("v2_hmac_sha256", 2, b"benchmark-secret-hmac-key-32byte"),
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            log = EventLog(
                log_path=Path(tmpdir) / f"bench_{label}.chimera.log",
                workflow_id=f"bench_{label}",
                format_version=fmt_ver,
                hmac_key=key,
            )
            latencies_ms: list[float] = []
            for i in range(samples):
                etype = "WORKFLOW_CREATED" if i == 0 else "TASK_STATE_CHANGED"
                t0 = time.perf_counter()
                log.append(
                    etype,
                    {
                        "task_id": f"t_{i}",
                        "from_state": "PENDING",
                        "to_state": "READY",
                        "attempt": 0,
                        "claim_epoch": i,
                    },
                )
                latencies_ms.append((time.perf_counter() - t0) * 1000.0)
            latencies_ms.sort()
            p95_idx = min(len(latencies_ms) - 1, int(len(latencies_ms) * 0.95))
            out[label] = {
                "samples": samples,
                "mean_ms": round(statistics.mean(latencies_ms), 3),
                "median_p50_ms": round(statistics.median(latencies_ms), 3),
                "p95_ms": round(latencies_ms[p95_idx], 3),
                "max_ms": round(max(latencies_ms), 3),
            }
    return out


def measure_recovery_time_vs_log_length(
    task_counts: tuple[int, ...] = (15, 60, 150),
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for n_tasks in task_counts:
        with tempfile.TemporaryDirectory() as tmpdir:
            tasks = [
                TaskSpec(
                    task_id=f"t_{i:03d}",
                    func=lambda p: p["i"],
                    input_payload={"i": i},
                )
                for i in range(n_tasks)
            ]
            wf_id = f"bench_rec_{n_tasks}"
            wf = create_workflow(
                tasks,
                max_concurrency=4,
                workflow_id=wf_id,
                storage_dir=tmpdir,
            )
            summary = run_workflow(wf)
            record_count = summary.log_record_count
            log_size_bytes = wf.log_path.stat().st_size

            t0 = time.perf_counter()
            rec_summary = recover_workflow(
                wf_id, tasks=tasks, storage_dir=tmpdir, auto_run=False
            )
            rec_ms = (time.perf_counter() - t0) * 1000.0
            assert rec_summary.status == WorkflowStatus.SUCCEEDED
            rows.append(
                {
                    "num_tasks": n_tasks,
                    "log_records": record_count,
                    "log_size_bytes": log_size_bytes,
                    "recovery_ms": round(rec_ms, 3),
                    "records_per_ms": round(record_count / max(rec_ms, 1e-6), 1),
                }
            )
    return rows


def measure_memory_usage(num_tasks: int = 200) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmpdir:
        tracemalloc.start()
        snapshot_before = tracemalloc.take_snapshot()
        base_current, _ = tracemalloc.get_traced_memory()

        tasks = [
            TaskSpec(
                task_id=f"mem_{i:03d}",
                func=lambda p: {"index": p["i"], "squared": p["i"] * p["i"]},
                input_payload={"i": i},
                dependencies=[f"mem_{i - 1:03d}"] if i > 0 else [],
            )
            for i in range(num_tasks)
        ]
        wf = create_workflow(
            tasks,
            max_concurrency=2,
            workflow_id="bench_memory_200",
            storage_dir=tmpdir,
        )
        summary = run_workflow(wf)
        assert summary.status == WorkflowStatus.SUCCEEDED
        after_current, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        del snapshot_before

        return {
            "num_tasks": num_tasks,
            "peak_traced_kib": round((peak_bytes - base_current) / 1024.0, 2),
            "retained_traced_kib": round((after_current - base_current) / 1024.0, 2),
            "per_task_peak_bytes": round((peak_bytes - base_current) / num_tasks, 1),
        }


def measure_process_backend_and_timeout() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Process startup & IPC latency across 4 sequential tasks
        n_proc_tasks = 4
        tasks = [
            TaskSpec(
                task_id=f"proc_t_{i}",
                func=top_level_noop_task,
                input_payload={"x": i},
            )
            for i in range(n_proc_tasks)
        ]
        t0 = time.perf_counter()
        wf_ok = create_workflow(
            tasks,
            max_concurrency=1,
            workflow_id="bench_proc_startup",
            storage_dir=tmpdir,
            execution_backend=ExecutionBackend.PROCESS,
        )
        summary_ok = run_workflow(wf_ok)
        proc_total_ms = (time.perf_counter() - t0) * 1000.0
        assert summary_ok.status == WorkflowStatus.SUCCEEDED

        # 2. Hard timeout & cleanup latency
        timeout_sec = 0.10
        t_to = time.perf_counter()
        wf_to = create_workflow(
            [
                TaskSpec(
                    task_id="hung_child",
                    func=top_level_infinite_sleep,
                    timeout_seconds=timeout_sec,
                    max_retries=0,
                )
            ],
            max_concurrency=1,
            workflow_id="bench_proc_timeout",
            storage_dir=tmpdir,
            execution_backend=ExecutionBackend.PROCESS,
        )
        summary_to = run_workflow(wf_to)
        total_timeout_wall_ms = (time.perf_counter() - t_to) * 1000.0
        cleanup_overhead_ms = max(
            0.0, total_timeout_wall_ms - (timeout_sec * 1000.0)
        )
        assert summary_to.status == WorkflowStatus.FAILED
        assert wf_to.active_process_count() == 0

        return {
            "process_tasks_sample": n_proc_tasks,
            "mean_process_task_ms": round(proc_total_ms / n_proc_tasks, 2),
            "configured_timeout_ms": round(timeout_sec * 1000.0, 1),
            "total_timeout_wall_ms": round(total_timeout_wall_ms, 2),
            "termination_and_reap_overhead_ms": round(cleanup_overhead_ms, 2),
            "active_processes_after_cleanup": wf_to.active_process_count(),
        }


def run_all_benchmarks() -> dict[str, Any]:
    return {
        "environment": {
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "system": platform.system(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
        },
        "scheduling_overhead": measure_scheduling_overhead(100),
        "throughput_by_concurrency": measure_throughput_by_concurrency(64),
        "event_log_append_latency": measure_event_log_append_latency(100),
        "recovery_vs_log_length": measure_recovery_time_vs_log_length(),
        "memory_usage_200_tasks": measure_memory_usage(200),
        "process_isolation_and_timeout": measure_process_backend_and_timeout(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run CHIMERA benchmark suite.")
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    args = parser.parse_args()

    results = run_all_benchmarks()
    if args.json:
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    env = results["environment"]
    print(f"=== CHIMERA Benchmark Suite ({env['python_version']} on {env['platform']}) ===")
    sched = results["scheduling_overhead"]
    print(
        f"[1] 100-Task Linear DAG: validate={sched['dag_validation_ms']}ms, "
        f"run={sched['total_execution_ms']}ms ({sched['per_task_overhead_ms']}ms/task)"
    )
    print("[2] Throughput vs. Concurrency (64 independent tasks):")
    for row in results["throughput_by_concurrency"]:
        print(
            f"    concurrency={row['max_concurrency']}: {row['elapsed_ms']}ms "
            f"({row['tasks_per_sec']} tasks/sec)"
        )
    print("[3] EventLog Append + fsync Latency (100 frames):")
    for k, v in results["event_log_append_latency"].items():
        print(
            f"    {k}: mean={v['mean_ms']}ms, p50={v['median_p50_ms']}ms, "
            f"p95={v['p95_ms']}ms, max={v['max_ms']}ms"
        )
    print("[4] Crash Recovery Replay Time vs. Log Length:")
    for row in results["recovery_vs_log_length"]:
        print(
            f"    tasks={row['num_tasks']} ({row['log_records']} records, "
            f"{row['log_size_bytes']} B): {row['recovery_ms']}ms "
            f"({row['records_per_ms']} records/ms)"
        )
    mem = results["memory_usage_200_tasks"]
    print(
        f"[5] Memory Footprint (200 tasks): peak_delta={mem['peak_traced_kib']} KiB "
        f"({mem['per_task_peak_bytes']} B/task)"
    )
    proc = results["process_isolation_and_timeout"]
    print(
        f"[6] Process Backend (`spawn`): mean_task={proc['mean_process_task_ms']}ms, "
        f"timeout_termination_overhead={proc['termination_and_reap_overhead_ms']}ms, "
        f"leaked_processes={proc['active_processes_after_cleanup']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
