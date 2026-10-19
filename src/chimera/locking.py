"""
Cross-process and in-process advisory file locking (`WorkflowFileLock`).

Platform support:
- POSIX (Linux, macOS): Uses `fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)`
  on `<workflow_id>.chimera.lock`.
- Windows (`nt`): Uses `msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)` on the first
  byte of `<workflow_id>.chimera.lock`, opened with `os.O_BINARY`.
- In-process protection: Uses a process-wide canonical path table
  (`_ACTIVE_IN_PROCESS_LOCKS`) normalized via `os.path.normcase(os.path.realpath(...))`
  so two separate `WorkflowEngine` or `EventLog` instances within the same
  Python process cannot concurrently mutate the same workflow log.
"""

from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import threading
from typing import Iterator

from chimera.exceptions import ConcurrentWorkflowAccessError

if os.name == "posix":
    import fcntl
elif os.name == "nt":  # pragma: no cover
    import msvcrt

O_BINARY_FLAG: int = getattr(os, "O_BINARY", 0)

_PROCESS_LOCK_GUARD = threading.RLock()
_ACTIVE_IN_PROCESS_LOCKS: dict[str, int] = {}  # canonical lock_path -> owner_id


def _os_try_lock_exclusive(fd: int) -> None:
    """Acquire a non-blocking OS-level exclusive lock on `fd`."""
    if os.name == "posix":
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    elif os.name == "nt":  # pragma: no cover
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)


def _os_unlock(fd: int) -> None:
    """Release an OS-level exclusive lock on `fd`."""
    if os.name == "posix":
        fcntl.flock(fd, fcntl.LOCK_UN)
    elif os.name == "nt":  # pragma: no cover
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


class WorkflowFileLock:
    """
    Per-engine exclusive workflow lock.

    Threads belonging to the SAME `WorkflowEngine` instance share ownership of
    this lock via atomic reference counting (`self._depth`) without blocking
    one another across `yield`, while any second `WorkflowEngine` instance (in
    the same process or a separate OS process) attempting to acquire the lock
    on the same path fails immediately with `ConcurrentWorkflowAccessError`.
    """

    def __init__(self, lock_path: str | Path, workflow_id: str) -> None:
        self.lock_path = Path(lock_path)
        self.workflow_id = workflow_id
        self._owner_id = id(self)
        self._state_mutex = threading.Lock()
        self._depth = 0
        self._fd: int | None = None

    def _acquire_internal(self) -> str:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        canonical_key = os.path.normcase(os.path.realpath(str(self.lock_path)))

        with self._state_mutex:
            if self._depth > 0:
                self._depth += 1
                return canonical_key

            with _PROCESS_LOCK_GUARD:
                existing_owner = _ACTIVE_IN_PROCESS_LOCKS.get(canonical_key)
                if (
                    existing_owner is not None
                    and existing_owner != self._owner_id
                ):
                    raise ConcurrentWorkflowAccessError(
                        f"Workflow '{self.workflow_id}' ({canonical_key}) is already locked "
                        "by another active instance in this process."
                    )

                fd = os.open(
                    str(self.lock_path),
                    os.O_CREAT | os.O_RDWR | O_BINARY_FLAG,
                    0o644,
                )
                try:
                    _os_try_lock_exclusive(fd)
                except (BlockingIOError, PermissionError, OSError) as exc:
                    os.close(fd)
                    raise ConcurrentWorkflowAccessError(
                        f"Workflow '{self.workflow_id}' ({canonical_key}) is locked by another "
                        f"concurrent process: {exc}"
                    ) from exc

                _ACTIVE_IN_PROCESS_LOCKS[canonical_key] = self._owner_id
                self._fd = fd
                self._depth = 1
                return canonical_key

    def _release_internal(self, canonical_key: str) -> None:
        with self._state_mutex:
            self._depth -= 1
            if self._depth == 0 and self._fd is not None:
                fd_to_close = self._fd
                self._fd = None
                with _PROCESS_LOCK_GUARD:
                    _ACTIVE_IN_PROCESS_LOCKS.pop(canonical_key, None)
                    try:
                        _os_unlock(fd_to_close)
                    except OSError:
                        pass
                    try:
                        os.close(fd_to_close)
                    except OSError:
                        pass

    @contextmanager
    def acquire(self) -> Iterator[None]:
        canonical_key = self._acquire_internal()
        try:
            yield
        finally:
            self._release_internal(canonical_key)
