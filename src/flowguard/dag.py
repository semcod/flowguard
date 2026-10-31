"""DAG Task representation and topological execution planner for Flowguard."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from graphlib import CycleError, TopologicalSorter
from typing import Any, Callable, Coroutine


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    QUARANTINED = "quarantined"


@dataclass
class TaskNode:
    id: str
    action: Callable[[], Coroutine[Any, Any, Any]]
    needs: list[str] = field(default_factory=list)
    resource_type: str = "cpu"
    resources: dict[str, int] = field(default_factory=dict)
    provider: str | None = None
    blocking: bool = False
    attempts: int = 0
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: str | None = None
    started_at: float | None = None
    completed_at: float | None = None


class TaskDAG:
    """Manages an acyclic directed graph of tasks, isolating failures to dependent paths."""

    def __init__(self) -> None:
        self._nodes: dict[str, TaskNode] = {}

    def add_task(
        self,
        task_id: str,
        action: Callable[[], Coroutine[Any, Any, Any]],
        needs: list[str] | None = None,
        resource_type: str = "cpu",
        resources: dict[str, int] | None = None,
        provider: str | None = None,
        blocking: bool = False,
    ) -> None:
        """Declare a task.

        ``resources`` names counted resources such as ``engine:willman`` or the
        exclusive ``writer:<repo>/<worktree>``. ``provider`` scopes rate-limit
        backoff. ``blocking`` runs a synchronous callable in a worker thread.
        """
        if task_id in self._nodes:
            raise ValueError(f"Task '{task_id}' already declared in DAG")
        self._nodes[task_id] = TaskNode(
            id=task_id,
            action=action,
            needs=list(needs or []),
            resource_type=resource_type,
            resources=dict(resources or {}),
            provider=provider,
            blocking=blocking,
        )

    @property
    def nodes(self) -> list[TaskNode]:
        return list(self._nodes.values())

    def requeue(self, task_id: str) -> None:
        """Return a deferred task to the ready pool without recording a failure."""
        node = self._nodes[task_id]
        node.status = TaskStatus.PENDING
        node.started_at = None

    def get_node(self, task_id: str) -> TaskNode | None:
        return self._nodes.get(task_id)

    def validate(self) -> None:
        """Reject incomplete or cyclic graphs without changing task state."""
        for node in self._nodes.values():
            for parent_id in node.needs:
                if parent_id not in self._nodes:
                    raise KeyError(f"Dependency '{parent_id}' not found for task '{node.id}'")
        try:
            TopologicalSorter({node.id: node.needs for node in self._nodes.values()}).prepare()
        except CycleError as error:
            cycle = " -> ".join(map(str, error.args[1]))
            raise ValueError(f"DependencyCycle: {cycle}") from error

    def is_finished(self) -> bool:
        return all(
            n.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED, TaskStatus.QUARANTINED)
            for n in self._nodes.values()
        )

    def get_ready_tasks(self) -> list[TaskNode]:
        """Return tasks whose dependencies are satisfied and are ready to run."""
        ready: list[TaskNode] = []
        for node in self._nodes.values():
            if node.status != TaskStatus.PENDING:
                continue

            # Check all parent dependencies
            deps_completed = True
            any_dep_failed = False
            for parent_id in node.needs:
                parent = self._nodes.get(parent_id)
                if parent is None:
                    raise KeyError(f"Dependency '{parent_id}' not found for task '{node.id}'")
                if parent.status in (TaskStatus.FAILED, TaskStatus.SKIPPED, TaskStatus.QUARANTINED):
                    any_dep_failed = True
                    break
                if parent.status != TaskStatus.COMPLETED:
                    deps_completed = False
                    break

            if any_dep_failed:
                node.status = TaskStatus.SKIPPED
                node.error = "Parent dependency failed/skipped"
            elif deps_completed:
                ready.append(node)

        return ready

    def mark_completed(self, task_id: str, result: Any) -> None:
        node = self._nodes[task_id]
        node.status = TaskStatus.COMPLETED
        node.result = result

    def mark_failed(self, task_id: str, error: str) -> None:
        node = self._nodes[task_id]
        node.status = TaskStatus.FAILED
        node.error = error

    def mark_quarantined(self, task_id: str, reason: str) -> None:
        node = self._nodes[task_id]
        node.status = TaskStatus.QUARANTINED
        node.error = reason
