"""
Deterministic task readiness queue (`DeterministicReadyScheduler`) and task
callable invocation helpers.
"""

from __future__ import annotations

import heapq
import inspect
from typing import Any, Mapping

from chimera.models import (
    TaskCallable,
    TaskContext,
    TaskRuntimeState,
    TaskSpec,
    TaskState,
)


def invoke_task_callable(
    func: TaskCallable, payload: Any, context: TaskContext
) -> Any:
    """
    Invoke a task function with 0, 1, or 2 arguments (`()`, `(payload,)`, or
    `(payload, context)`), inspecting its signature cleanly.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    has_varargs = any(
        p.kind == inspect.Parameter.VAR_POSITIONAL for p in params
    )
    positional_params = [
        p
        for p in params
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    if has_varargs or len(positional_params) >= 2:
        return func(payload, context)
    if len(positional_params) == 1:
        return func(payload)
    return func()


class DeterministicReadyScheduler:
    """
    Min-heap ready queue ordered deterministically by `(priority, task_id)`.

    Fairness & Starvation Note:
    Tasks with lower numerical `priority` values execute first, with ties
    broken lexicographically by `task_id`. Because a workflow DAG is finite and
    acyclic, all eligible higher-priority tasks complete in finite steps
    (bounded by `max_retries`), guaranteeing freedom from indefinite starvation
    within a single workflow execution.
    """

    def __init__(self) -> None:
        self._heap: list[tuple[int, str]] = []
        self._in_queue: set[str] = set()

    def clear(self) -> None:
        self._heap.clear()
        self._in_queue.clear()

    def push(self, task_id: str, priority: int = 0) -> None:
        if task_id not in self._in_queue:
            heapq.heappush(self._heap, (priority, task_id))
            self._in_queue.add(task_id)

    def discard(self, task_id: str) -> None:
        self._in_queue.discard(task_id)

    def has_ready(self) -> bool:
        return bool(self._in_queue)

    def ready_count(self) -> int:
        return len(self._in_queue)

    def pop_next_ready(
        self, task_states: Mapping[str, TaskRuntimeState]
    ) -> str | None:
        while self._heap:
            _, candidate_id = heapq.heappop(self._heap)
            if candidate_id not in self._in_queue:
                continue
            self._in_queue.discard(candidate_id)
            rt = task_states.get(candidate_id)
            if rt is not None and rt.state == TaskState.READY:
                return candidate_id
        return None

    def rebuild(
        self,
        task_states: Mapping[str, TaskRuntimeState],
        task_specs: Mapping[str, TaskSpec],
    ) -> None:
        self.clear()
        for tid, rt in task_states.items():
            if rt.state == TaskState.READY:
                prio = task_specs[tid].priority if tid in task_specs else 0
                self.push(tid, prio)

    @staticmethod
    def find_eligible_pending_tasks(
        task_states: Mapping[str, TaskRuntimeState],
        task_specs: Mapping[str, TaskSpec],
    ) -> list[str]:
        """
        Return all `PENDING` task IDs whose dependencies are all `SUCCEEDED`,
        sorted deterministically by `(priority, task_id)`.
        """
        eligible: list[tuple[int, str]] = []
        for tid, rt in task_states.items():
            if rt.state != TaskState.PENDING:
                continue
            spec = task_specs[tid]
            if all(
                task_states[dep_id].state == TaskState.SUCCEEDED
                for dep_id in spec.dependencies
            ):
                eligible.append((spec.priority, tid))
        eligible.sort(key=lambda item: (item[0], item[1]))
        return [tid for _, tid in eligible]

    @staticmethod
    def find_downstream_dependents(
        failed_task_id: str,
        dependents_map: Mapping[str, list[str]],
    ) -> list[str]:
        """
        Return all transitive downstream dependents of `failed_task_id` in
        deterministic breadth-first order.
        """
        visited: set[str] = set()
        queue: list[str] = list(dependents_map.get(failed_task_id, []))
        affected: list[str] = []

        while queue:
            curr = queue.pop(0)
            if curr in visited:
                continue
            visited.add(curr)
            affected.append(curr)
            for child in dependents_map.get(curr, []):
                if child not in visited:
                    queue.append(child)
        return affected
