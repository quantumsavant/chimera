"""
Injectable clock abstractions (`Clock`, `SystemClock`, `VirtualClock`) for
deterministic scheduling, timeout enforcement, and backoff testing without
arbitrary `time.sleep` calls.
"""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Clock(Protocol):
    """Injectable clock interface for deterministic time and condition waiting."""

    def now(self) -> float:
        """Return current monotonic/logical timestamp in seconds."""
        ...

    def wait_condition(
        self, cond: threading.Condition, timeout: float | None
    ) -> None:
        """Wait on a `threading.Condition` up to `timeout` seconds."""
        ...


class SystemClock:
    """Production clock backed by `time.monotonic` anchored to wall time."""

    def __init__(self) -> None:
        self._base_wall = time.time()
        self._base_mono = time.monotonic()

    def now(self) -> float:
        return self._base_wall + (time.monotonic() - self._base_mono)

    def wait_condition(
        self, cond: threading.Condition, timeout: float | None
    ) -> None:
        cond.wait(timeout=timeout)


class VirtualClock:
    """
    Deterministic controllable clock for testing timeouts, retries, and
    exponential backoff without sleeping.

    When `auto_advance_on_wait` is True, the workflow scheduler automatically
    advances this clock to the next `RETRY_WAIT` wake-up timestamp whenever no
    worker threads are actively running (`active_running_count == 0` and no
    tasks are `READY`). Active task timeouts are never prematurely triggered
    by auto-advance while worker threads are executing.
    """

    def __init__(
        self,
        initial_time: float = 1_700_000_000.0,
        auto_advance_on_wait: bool = False,
    ) -> None:
        self._lock = threading.RLock()
        self._now = float(initial_time)
        self.auto_advance_on_wait = auto_advance_on_wait
        self._conditions: list[threading.Condition] = []

    def register_condition(self, cond: threading.Condition) -> None:
        with self._lock:
            if cond not in self._conditions:
                self._conditions.append(cond)

    def now(self) -> float:
        with self._lock:
            return self._now

    def advance(self, delta_seconds: float) -> float:
        if delta_seconds < 0:
            raise ValueError("Cannot advance VirtualClock by a negative delta.")
        with self._lock:
            self._now += float(delta_seconds)
            current = self._now
            conds = list(self._conditions)
        for cond in conds:
            with cond:
                cond.notify_all()
        return current

    def set_time(self, timestamp: float) -> None:
        with self._lock:
            if timestamp < self._now:
                raise ValueError("VirtualClock time cannot move backward.")
            self._now = float(timestamp)
            conds = list(self._conditions)
        for cond in conds:
            with cond:
                cond.notify_all()

    def wait_condition(
        self, cond: threading.Condition, timeout: float | None
    ) -> None:
        cond.wait(
            timeout=0.002 if timeout is None else min(max(timeout, 0.0005), 0.002)
        )
