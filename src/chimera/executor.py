"""
Supervised task execution backends (`ThreadTaskExecutor` and `ProcessTaskExecutor`).

Architecture & Isolation Guarantees:
1. `ThreadTaskExecutor`: Runs lightweight, cooperative tasks in daemon threads
   within the host process. Supports cooperative cancellation via
   `TaskContext.is_cancelled()`. Cannot forcibly kill a thread blocked in an
   uncooperative C extension or infinite CPU loop.
2. `ProcessTaskExecutor`: Uses an explicit `multiprocessing.get_context("spawn")`
   context to execute each task attempt in an isolated child OS process with a
   dedicated unidirectional `Pipe(duplex=False)` and shared `Event`.
   - Enforces monotonic execution deadlines.
   - Escalates termination cleanly: cooperative cancel signal -> `proc.terminate()`
     (`SIGTERM` / `TerminateProcess`) -> `proc.kill()` (`SIGKILL`), followed by
     `proc.join()` and `proc.close()` to reap zombie processes and close OS
     file descriptors.
   - Enforces `max_output_bytes` on serialized return payloads.
   - Rejects lambdas, closures, and unimportable callables eagerly with
     `UnserializableCallableError` rather than silently falling back to threads.

Security & Resource Isolation Caveat:
Process isolation enables hard timeout termination and isolates the parent
engine's memory space from child crashes (`SIGSEGV`, `os._exit`), but a child
process is NOT a security sandbox—it inherits the OS user's filesystem,
network, and CPU/memory privileges unless wrapped in OS-level containerization
(cgroups, namespaces, seccomp, or job objects).
"""

from __future__ import annotations

import builtins
from dataclasses import dataclass
import multiprocessing
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
import os
import sys
import threading
import time
from typing import Any, Callable

from chimera.exceptions import (
    ExecutorError,
    OversizedPayloadError,
    PayloadSerializationError,
    ProcessWorkerCrashError,
    TaskOutputLimitExceededError,
)
from chimera.models import ClaimToken, FailureCategory, TaskContext, TaskSpec
from chimera.scheduler import invoke_task_callable
from chimera.serialization import (
    DEFAULT_MAX_PAYLOAD_BYTES,
    canonical_json_bytes,
    canonical_json_loads,
    resolve_process_callable,
    validate_callable_for_process_backend,
)


@dataclass(frozen=True)
class TaskAttemptResult:
    """Outcome of a single task execution attempt returned by an executor."""

    token: ClaimToken
    succeeded: bool
    result: Any = None
    exception: BaseException | None = None
    failure_category: FailureCategory = FailureCategory.NONE
    worker_pid: int | None = None
    exit_code: int | None = None


def _reconstruct_exception(
    error_type: str, error_message: str
) -> Exception:
    """Reconstruct a standard library exception instance by class name when possible."""
    builtin_cls = getattr(builtins, error_type, None)
    if (
        isinstance(builtin_cls, type)
        and issubclass(builtin_cls, Exception)
    ):
        try:
            return builtin_cls(error_message)
        except Exception:
            pass
    if error_type == "TaskOutputLimitExceededError":
        return TaskOutputLimitExceededError(error_message)
    if error_type == "PayloadSerializationError":
        return PayloadSerializationError(error_message)
    if error_type == "OversizedPayloadError":
        return OversizedPayloadError(error_message)

    # Create a dynamic Exception subclass whose __name__ matches `error_type`
    # so error_type reporting is preserved.
    dynamic_cls = type(error_type, (RuntimeError,), {})
    return dynamic_cls(error_message)


def _process_worker_entrypoint(
    sys_path: list[str],
    main_file: str | None,
    module_name: str,
    qualname: str,
    workflow_id: str,
    task_id: str,
    attempt: int,
    claim_epoch: int,
    idempotency_key: str,
    operation_key: str,
    attempt_idempotency_key: str,
    payload_bytes: bytes,
    dep_results_bytes: bytes,
    max_output_bytes: int,
    cancel_event: Any,
    send_conn: Connection,
) -> None:
    """
    Top-level entrypoint executed inside each spawned child worker process.
    Communicates its outcome over `send_conn` as a canonical JSON envelope.
    """
    try:
        for p in reversed(sys_path):
            if p not in sys.path:
                sys.path.insert(0, p)

        func = resolve_process_callable(module_name, qualname, main_file=main_file)
        payload = canonical_json_loads(payload_bytes, max_bytes=None)
        dep_results = canonical_json_loads(dep_results_bytes, max_bytes=None)

        ctx = TaskContext(
            workflow_id=workflow_id,
            task_id=task_id,
            attempt=attempt,
            claim_epoch=claim_epoch,
            idempotency_key=idempotency_key,
            dependency_results=dep_results,
            _cancel_event=cancel_event,
            operation_key=operation_key,
            attempt_idempotency_key=attempt_idempotency_key,
        )

        output = invoke_task_callable(func, payload, ctx)
        # Validate and size-check the task output before wrapping in the IPC envelope
        canonical_json_bytes(
            output, max_bytes=max_output_bytes, is_task_output=True
        )
        envelope = {
            "status": "SUCCEEDED",
            "result": output,
            "worker_pid": os.getpid(),
        }
        encoded = canonical_json_bytes(envelope, max_bytes=None)
        send_conn.send_bytes(encoded)
    except TaskOutputLimitExceededError as exc:
        err_env = {
            "status": "EXCEPTION",
            "error_type": "TaskOutputLimitExceededError",
            "error_message": str(exc),
            "failure_category": FailureCategory.SERIALIZATION_ERROR.value,
            "worker_pid": os.getpid(),
        }
        try:
            send_conn.send_bytes(canonical_json_bytes(err_env, max_bytes=None))
        except Exception:
            pass
    except PayloadSerializationError as exc:
        err_env = {
            "status": "EXCEPTION",
            "error_type": "PayloadSerializationError",
            "error_message": str(exc),
            "failure_category": FailureCategory.SERIALIZATION_ERROR.value,
            "worker_pid": os.getpid(),
        }
        try:
            send_conn.send_bytes(canonical_json_bytes(err_env, max_bytes=None))
        except Exception:
            pass
    except BaseException as exc:
        err_env = {
            "status": "EXCEPTION",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "failure_category": FailureCategory.APPLICATION_ERROR.value,
            "worker_pid": os.getpid(),
        }
        try:
            send_conn.send_bytes(canonical_json_bytes(err_env, max_bytes=None))
        except Exception:
            pass
    finally:
        try:
            send_conn.close()
        except Exception:
            pass


@dataclass
class _SupervisedChildProcess:
    token: ClaimToken
    proc: BaseProcess
    recv_conn: Connection
    cancel_event: Any
    max_output_bytes: int
    pid: int | None = None
    exit_code: int | None = None
    terminated_by_supervisor: bool = False
    cleaned_up: bool = False


def _safe_is_alive(proc: BaseProcess) -> bool:
    try:
        return bool(proc.is_alive())
    except (ValueError, OSError, AttributeError):
        return False


def _safe_exitcode(proc: BaseProcess, fallback: int | None = None) -> int | None:
    try:
        code = proc.exitcode
        return code if code is not None else fallback
    except (ValueError, OSError, AttributeError):
        return fallback


class ProcessTaskExecutor:
    """
    Supervised child-process executor using `multiprocessing.get_context("spawn")`.
    """

    def __init__(
        self,
        max_workers: int = 4,
        *,
        default_max_output_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        grace_period_seconds: float = 0.03,
        terminate_wait_seconds: float = 0.05,
        on_attempt_completed: Callable[[TaskAttemptResult], None] | None = None,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1.")
        self.max_workers = max_workers
        self.default_max_output_bytes = default_max_output_bytes
        self.grace_period_seconds = grace_period_seconds
        self.terminate_wait_seconds = terminate_wait_seconds
        self._on_attempt_completed = on_attempt_completed
        self._mp_ctx = multiprocessing.get_context("spawn")
        self._lock = threading.RLock()
        self._active: dict[tuple[str, int], _SupervisedChildProcess] = {}
        self._monitor_threads: list[threading.Thread] = []
        self._shutting_down = False

    def active_process_count(self) -> int:
        with self._lock:
            items = list(self._active.values())
        return sum(1 for item in items if _safe_is_alive(item.proc))

    def submit(
        self,
        token: ClaimToken,
        spec: TaskSpec,
        payload: Any,
        context: TaskContext,
    ) -> int:
        """
        Validate and dispatch a task attempt to a fresh `spawn` child process.
        Returns the child process PID.
        """
        module_name, qualname = validate_callable_for_process_backend(
            spec.func, spec.task_id
        )
        max_out = (
            spec.max_output_bytes
            if spec.max_output_bytes is not None
            else self.default_max_output_bytes
        )
        payload_bytes = canonical_json_bytes(payload, max_bytes=max_out)
        dep_results_bytes = canonical_json_bytes(
            context.dependency_results, max_bytes=None
        )

        with self._lock:
            if self._shutting_down:
                raise ExecutorError(
                    "Cannot submit task to ProcessTaskExecutor after shutdown."
                )
            active_alive = sum(
                1 for item in self._active.values() if _safe_is_alive(item.proc)
            )
            if active_alive >= self.max_workers:
                raise ExecutorError(
                    f"ProcessTaskExecutor concurrency limit ({self.max_workers}) reached."
                )

            recv_conn, send_conn = self._mp_ctx.Pipe(duplex=False)
            cancel_event = self._mp_ctx.Event()
            proc = self._mp_ctx.Process(
                target=_process_worker_entrypoint,
                args=(
                    list(sys.path),
                    getattr(sys.modules.get("__main__"), "__file__", None),
                    module_name,
                    qualname,
                    token.workflow_id,
                    token.task_id,
                    token.attempt,
                    token.claim_epoch,
                    context.idempotency_key,
                    context.operation_key,
                    context.attempt_idempotency_key,
                    payload_bytes,
                    dep_results_bytes,
                    max_out,
                    cancel_event,
                    send_conn,
                ),
                name=f"chimera-{token.workflow_id}-{token.task_id}-e{token.claim_epoch}",
                daemon=False,
            )
            proc.start()
            # Close write end in parent immediately so EOF is detected if child crashes
            send_conn.close()

            pid = proc.pid or -1
            supervised = _SupervisedChildProcess(
                token=token,
                proc=proc,
                recv_conn=recv_conn,
                cancel_event=cancel_event,
                max_output_bytes=max_out,
                pid=pid,
            )
            key = (token.task_id, token.claim_epoch)
            self._active[key] = supervised

            monitor = threading.Thread(
                target=self._watch_child_process,
                args=(key, supervised),
                name=f"monitor-{token.task_id}-e{token.claim_epoch}",
                daemon=True,
            )
            self._monitor_threads.append(monitor)
            monitor.start()
            return pid

    def _watch_child_process(
        self,
        key: tuple[str, int],
        supervised: _SupervisedChildProcess,
    ) -> None:
        proc = supervised.proc
        recv_conn = supervised.recv_conn
        token = supervised.token
        pid = supervised.pid
        raw_msg: bytes | None = None
        pipe_err: Exception | None = None
        outcome: TaskAttemptResult | None = None

        try:
            try:
                raw_msg = recv_conn.recv_bytes(
                    supervised.max_output_bytes + 65536
                )
            except EOFError:
                raw_msg = None
            except BufferError:
                pipe_err = TaskOutputLimitExceededError(
                    f"Task '{token.task_id}' child process output exceeded buffer limit ({supervised.max_output_bytes} bytes)."
                )
            except Exception as exc:
                pipe_err = exc

            try:
                proc.join(timeout=1.0)
                if _safe_is_alive(proc):
                    self._terminate_and_reap_process(supervised)
            except Exception:
                pass

            exit_code = _safe_exitcode(proc, supervised.exit_code)
            supervised.exit_code = exit_code

            outcome = self._decode_child_outcome(
                supervised=supervised,
                raw_msg=raw_msg,
                pipe_err=pipe_err,
                pid=pid,
                exit_code=exit_code,
            )
        except BaseException as unexpected_exc:
            outcome = TaskAttemptResult(
                token=token,
                succeeded=False,
                exception=ProcessWorkerCrashError(
                    f"Monitor error for worker process (pid={pid}): {unexpected_exc}",
                    exit_code=supervised.exit_code,
                ),
                failure_category=FailureCategory.PROCESS_CRASH,
                worker_pid=pid,
                exit_code=supervised.exit_code,
            )
        finally:
            self._cleanup_child_resources(key, supervised)
            if outcome is not None and self._on_attempt_completed is not None:
                self._on_attempt_completed(outcome)

    def _decode_child_outcome(
        self,
        *,
        supervised: _SupervisedChildProcess,
        raw_msg: bytes | None,
        pipe_err: Exception | None,
        pid: int | None,
        exit_code: int | None,
    ) -> TaskAttemptResult:
        token = supervised.token

        if isinstance(pipe_err, TaskOutputLimitExceededError):
            return TaskAttemptResult(
                token=token,
                succeeded=False,
                exception=pipe_err,
                failure_category=FailureCategory.SERIALIZATION_ERROR,
                worker_pid=pid,
                exit_code=exit_code,
            )

        if raw_msg is not None and not supervised.terminated_by_supervisor:
            try:
                envelope = canonical_json_loads(
                    raw_msg, max_bytes=supervised.max_output_bytes + 65536
                )
                if not isinstance(envelope, dict) or "status" not in envelope:
                    raise ValueError("Missing 'status' field in worker IPC envelope.")
            except Exception as exc:
                crash_exc = ProcessWorkerCrashError(
                    f"Malformed IPC message from worker process (pid={pid}, exit_code={exit_code}): {exc}",
                    exit_code=exit_code,
                )
                return TaskAttemptResult(
                    token=token,
                    succeeded=False,
                    exception=crash_exc,
                    failure_category=FailureCategory.PROCESS_CRASH,
                    worker_pid=pid,
                    exit_code=exit_code,
                )

            status = envelope.get("status")
            if status == "SUCCEEDED" and (exit_code == 0 or exit_code is None):
                result_val = envelope.get("result")
                try:
                    canonical_json_bytes(
                        result_val,
                        max_bytes=supervised.max_output_bytes,
                        is_task_output=True,
                    )
                except TaskOutputLimitExceededError as exc:
                    return TaskAttemptResult(
                        token=token,
                        succeeded=False,
                        exception=exc,
                        failure_category=FailureCategory.SERIALIZATION_ERROR,
                        worker_pid=pid,
                        exit_code=exit_code,
                    )
                return TaskAttemptResult(
                    token=token,
                    succeeded=True,
                    result=result_val,
                    failure_category=FailureCategory.NONE,
                    worker_pid=pid,
                    exit_code=0 if exit_code is None else exit_code,
                )

            if status == "EXCEPTION":
                err_type = str(envelope.get("error_type", "RuntimeError"))
                err_msg = str(envelope.get("error_message", "Worker exception"))
                cat_str = str(
                    envelope.get(
                        "failure_category",
                        FailureCategory.APPLICATION_ERROR.value,
                    )
                )
                try:
                    cat = FailureCategory(cat_str)
                except ValueError:
                    cat = FailureCategory.APPLICATION_ERROR
                reconstructed = _reconstruct_exception(err_type, err_msg)
                return TaskAttemptResult(
                    token=token,
                    succeeded=False,
                    exception=reconstructed,
                    failure_category=cat,
                    worker_pid=pid,
                    exit_code=exit_code,
                )

        crash_exc = ProcessWorkerCrashError(
            f"Worker process (pid={pid}) for task '{token.task_id}' (attempt={token.attempt}, "
            f"epoch={token.claim_epoch}) terminated abruptly with exit_code={exit_code}.",
            exit_code=exit_code,
        )
        return TaskAttemptResult(
            token=token,
            succeeded=False,
            exception=crash_exc,
            failure_category=FailureCategory.PROCESS_CRASH,
            worker_pid=pid,
            exit_code=exit_code,
        )

    def terminate_attempt(self, task_id: str, claim_epoch: int) -> bool:
        """
        Terminate and reap a specific in-flight task attempt using 3-stage
        escalation: cooperative event -> terminate() -> kill() -> join().
        """
        key = (task_id, claim_epoch)
        with self._lock:
            supervised = self._active.get(key)
        if supervised is None:
            return False

        self._terminate_and_reap_process(supervised)
        return True

    def _terminate_and_reap_process(
        self, supervised: _SupervisedChildProcess
    ) -> None:
        supervised.terminated_by_supervisor = True
        proc = supervised.proc
        try:
            supervised.cancel_event.set()
        except Exception:
            pass

        try:
            if not _safe_is_alive(proc):
                proc.join(timeout=0.05)
                supervised.exit_code = _safe_exitcode(proc, supervised.exit_code)
                return

            # Stage 1: Wait briefly for cooperative shutdown
            proc.join(timeout=self.grace_period_seconds)
            if not _safe_is_alive(proc):
                supervised.exit_code = _safe_exitcode(proc, supervised.exit_code)
                return

            # Stage 2: Graceful OS termination (SIGTERM / TerminateProcess)
            try:
                proc.terminate()
            except (OSError, ValueError, AttributeError):
                pass
            proc.join(timeout=self.terminate_wait_seconds)
            if not _safe_is_alive(proc):
                supervised.exit_code = _safe_exitcode(proc, supervised.exit_code)
                return

            # Stage 3: Hard kill (SIGKILL)
            try:
                proc.kill()
            except (OSError, ValueError, AttributeError):
                pass
            proc.join(timeout=1.0)
            supervised.exit_code = _safe_exitcode(proc, supervised.exit_code)
        except (ValueError, OSError, AttributeError):
            pass
        finally:
            try:
                supervised.recv_conn.close()
            except Exception:
                pass

    def _cleanup_child_resources(
        self, key: tuple[str, int], supervised: _SupervisedChildProcess
    ) -> None:
        with self._lock:
            self._active.pop(key, None)
            if supervised.cleaned_up:
                return
            supervised.cleaned_up = True
            supervised.exit_code = _safe_exitcode(
                supervised.proc, supervised.exit_code
            )
        try:
            supervised.recv_conn.close()
        except Exception:
            pass
        try:
            if not _safe_is_alive(supervised.proc):
                supervised.proc.close()
        except (ValueError, OSError, AttributeError):
            pass

    def shutdown(
        self,
        *,
        grace_period_seconds: float | None = None,
        force: bool = True,
    ) -> None:
        """
        Terminate and reap all active child worker processes and join monitor threads.
        Guarantees an unresponsive child cannot permanently block shutdown.
        """
        with self._lock:
            self._shutting_down = True
            items = list(self._active.items())
            monitors = list(self._monitor_threads)
            self._monitor_threads.clear()

        if grace_period_seconds is not None:
            old_grace = self.grace_period_seconds
            self.grace_period_seconds = grace_period_seconds
        else:
            old_grace = self.grace_period_seconds

        try:
            for _key, supervised in items:
                if force or _safe_is_alive(supervised.proc):
                    self._terminate_and_reap_process(supervised)
        finally:
            self.grace_period_seconds = old_grace

        for m in monitors:
            if m is not threading.current_thread():
                m.join(timeout=0.5)

        for key, supervised in items:
            self._cleanup_child_resources(key, supervised)
