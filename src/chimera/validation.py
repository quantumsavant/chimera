"""
DAG structural validation and workflow identifier filesystem safety checks.
"""

from __future__ import annotations

import re
from typing import Sequence

from chimera.exceptions import (
    CycleDetectedError,
    DuplicateTaskIdError,
    InvalidWorkflowIdError,
    MissingDependencyError,
    WorkflowValidationError,
)
from chimera.models import TaskSpec

_WORKFLOW_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

_WINDOWS_RESERVED_NAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }
)


def validate_workflow_id(workflow_id: str) -> str:
    """
    Validate that `workflow_id` is safe for use in local filesystem filenames
    across POSIX and Windows.

    Rejects empty strings, strings > 128 chars, path separators (`/`, `\`),
    parent directory traversal (`..`), NUL bytes, leading dots, and Windows
    reserved device names (`CON`, `NUL`, `AUX`, `PRN`, `COM1`..`COM9`, `LPT1`..`LPT9`).
    """
    if not isinstance(workflow_id, str) or not workflow_id:
        raise InvalidWorkflowIdError("workflow_id must be a non-empty string.")
    if len(workflow_id) > 128:
        raise InvalidWorkflowIdError(
            f"workflow_id '{workflow_id[:32]}...' exceeds maximum length of 128 characters."
        )
    if ".." in workflow_id or "/" in workflow_id or "\\" in workflow_id or "\x00" in workflow_id:
        raise InvalidWorkflowIdError(
            f"workflow_id {workflow_id!r} contains forbidden path traversal or separator characters."
        )
    if not _WORKFLOW_ID_PATTERN.match(workflow_id):
        raise InvalidWorkflowIdError(
            f"workflow_id {workflow_id!r} must match '^[A-Za-z0-9][A-Za-z0-9._-]{{0,127}}$'."
        )
    stem = workflow_id.split(".")[0].upper()
    if stem in _WINDOWS_RESERVED_NAMES:
        raise InvalidWorkflowIdError(
            f"workflow_id {workflow_id!r} uses a reserved Windows device name ({stem})."
        )
    return workflow_id


def validate_dag(tasks: Sequence[TaskSpec]) -> dict[str, TaskSpec]:
    """
    Validate a sequence of `TaskSpec` definitions:
    1. Reject duplicate task IDs (`DuplicateTaskIdError`).
    2. Reject references to missing dependencies (`MissingDependencyError`).
    3. Reject directed cycles of any depth (`CycleDetectedError`) using an
       iterative 3-color DFS that reports the exact cycle path without hitting
       Python's recursion limit.

    Returns a dictionary mapping `task_id -> TaskSpec`.
    """
    task_map: dict[str, TaskSpec] = {}
    for spec in tasks:
        if not isinstance(spec, TaskSpec):
            raise WorkflowValidationError(
                f"Expected TaskSpec instance, got {type(spec).__name__}."
            )
        if spec.task_id in task_map:
            raise DuplicateTaskIdError(
                f"Duplicate task_id '{spec.task_id}' in workflow definition."
            )
        task_map[spec.task_id] = spec

    for task_id, spec in task_map.items():
        for dep_id in spec.dependencies:
            if dep_id not in task_map:
                raise MissingDependencyError(
                    f"Task '{task_id}' depends on non-existent task '{dep_id}'."
                )

    dependents: dict[str, list[str]] = {tid: [] for tid in task_map}
    for task_id, spec in task_map.items():
        for dep_id in spec.dependencies:
            dependents[dep_id].append(task_id)
    for tid in dependents:
        dependents[tid].sort()

    # 0 = unvisited, 1 = visiting (on current recursion stack), 2 = visited
    color: dict[str, int] = {tid: 0 for tid in task_map}
    parent_stack: list[str] = []
    pos_in_stack: dict[str, int] = {}

    for start_id in sorted(task_map.keys()):
        if color[start_id] != 0:
            continue

        dfs_stack: list[tuple[str, int]] = [(start_id, 0)]
        color[start_id] = 1
        pos_in_stack[start_id] = len(parent_stack)
        parent_stack.append(start_id)

        while dfs_stack:
            curr_id, idx = dfs_stack[-1]
            neighbors = dependents[curr_id]
            if idx < len(neighbors):
                dfs_stack[-1] = (curr_id, idx + 1)
                nxt_id = neighbors[idx]
                nxt_color = color[nxt_id]
                if nxt_color == 1:
                    cycle_start_idx = pos_in_stack[nxt_id]
                    cycle_path = parent_stack[cycle_start_idx:] + [nxt_id]
                    raise CycleDetectedError(cycle_path)
                elif nxt_color == 0:
                    color[nxt_id] = 1
                    pos_in_stack[nxt_id] = len(parent_stack)
                    parent_stack.append(nxt_id)
                    dfs_stack.append((nxt_id, 0))
            else:
                color[curr_id] = 2
                dfs_stack.pop()
                parent_stack.pop()
                pos_in_stack.pop(curr_id, None)

    return task_map
