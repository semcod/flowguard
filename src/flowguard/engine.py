"""Main FlowEngine orchestrating bounded parallel execution over TaskDAG."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from flowguard.governor import FlowGovernor, ResourceLimits
from flowguard.dag import TaskDAG, TaskNode, TaskStatus
from flowguard.quarantine import QuarantineManager, GateFailureClassification


class RateLimited(Exception):
    """Raised by a task when its provider refuses work for ``retry_after`` seconds."""

    def __init__(self, message: str = "rate limited", retry_after: float | None = None,
                 provider: str | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after
        self.provider = provider


@dataclass(frozen=True)
class TaskExecutionReceipt:
    task_id: str
    status: TaskStatus
    duration_seconds: float
    result: Any = None
    error: str | None = None
    quarantine_code: GateFailureClassification | None = None
    attempts: int = 1


class FlowEngine:
    """Executes a TaskDAG with bounded concurrency, adaptive backpressure, and fast quarantine."""

    def __init__(
        self,
        governor: FlowGovernor | None = None,
        quarantine: QuarantineManager | None = None,
    ) -> None:
        self.governor = governor or FlowGovernor()
        self.quarantine = quarantine or QuarantineManager()
        self._in_use: dict[str, int] = {}

    def _validate_demands(self, dag: TaskDAG) -> None:
        for node in dag.nodes:
            for resource, amount in node.resources.items():
                capacity = self.governor.limits.capacity_of(resource)
                if amount < 1 or (capacity is not None and amount > capacity):
                    raise ValueError(
                        f"Task '{node.id}' requests {amount} of '{resource}' (capacity {capacity})"
                    )

    def _provider(self, node: TaskNode) -> str | None:
        return node.provider or (node.resource_type if node.resource_type.startswith("llm") else None)

    def _deferred(self, node: TaskNode) -> bool:
        provider = self._provider(node)
        if provider is None:
            return False
        if node.resource_type.startswith("llm") and self.governor.is_rate_limited:
            return True
        return self.governor.is_provider_limited(provider)

    def _try_acquire(self, node: TaskNode) -> bool:
        limits = self.governor.limits
        for resource, amount in node.resources.items():
            capacity = limits.capacity_of(resource)
            if capacity is not None and self._in_use.get(resource, 0) + amount > capacity:
                return False
        for resource, amount in node.resources.items():
            self._in_use[resource] = self._in_use.get(resource, 0) + amount
        return True

    def _release(self, node: TaskNode) -> None:
        for resource, amount in node.resources.items():
            self._in_use[resource] = self._in_use.get(resource, 0) - amount

    async def execute_dag(self, dag: TaskDAG, poll_interval: float = 0.05) -> list[TaskExecutionReceipt]:
        receipts: list[TaskExecutionReceipt] = []
        running_tasks: dict[asyncio.Task, TaskNode] = {}
        self._validate_demands(dag)

        while not dag.is_finished() or running_tasks:
            # 1. Check current admissible capacity
            admissible_slots = self.governor.evaluate_capacity()
            available_slots = max(0, admissible_slots - len(running_tasks))

            # 2. Schedule ready tasks up to available slot capacity
            if available_slots > 0:
                for node in dag.get_ready_tasks():
                    if available_slots <= 0:
                        break
                    # A backed-off provider or a busy resource defers only this task.
                    if self._deferred(node) or not self._try_acquire(node):
                        continue
                    available_slots -= 1
                    node.attempts += 1
                    node.status = TaskStatus.RUNNING
                    node.started_at = time.monotonic()
                    task = asyncio.create_task(self._run_node(node))
                    running_tasks[task] = node

            # 3. If no tasks running and none ready, check if deadlocked or finished
            if not running_tasks:
                if dag.is_finished():
                    break
                # Wait briefly for rate limits or delayed state
                await asyncio.sleep(poll_interval)
                continue

            # 4. Wait for at least one running task to complete
            # The timeout re-admits tasks deferred by an expired backoff or a freed slot.
            done, _ = await asyncio.wait(
                running_tasks.keys(),
                timeout=poll_interval,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for finished_task in done:
                node = running_tasks.pop(finished_task)
                self._release(node)
                node.completed_at = time.monotonic()
                duration = node.completed_at - (node.started_at or node.completed_at)

                try:
                    result = await finished_task
                    dag.mark_completed(node.id, result)
                    receipts.append(
                        TaskExecutionReceipt(
                            task_id=node.id,
                            status=TaskStatus.COMPLETED,
                            duration_seconds=duration,
                            result=result,
                            attempts=node.attempts,
                        )
                    )
                except Exception as exc:
                    err_msg = str(exc)
                    if self._retry_rate_limited(dag, node, exc):
                        continue
                    classification, retryable = self.quarantine.classify_and_quarantine(
                        node.id, err_msg, time.time()
                    )


                    dag.mark_quarantined(node.id, f"{classification.value}: {err_msg}")
                    receipts.append(
                        TaskExecutionReceipt(
                            task_id=node.id,
                            status=TaskStatus.QUARANTINED,
                            duration_seconds=duration,
                            error=err_msg,
                            quarantine_code=classification,
                            attempts=node.attempts,
                        )
                    )

            await asyncio.sleep(0.01)

        # Collect receipts for skipped nodes
        for node in dag._nodes.values():
            if node.status == TaskStatus.SKIPPED and not any(r.task_id == node.id for r in receipts):
                receipts.append(
                    TaskExecutionReceipt(
                        task_id=node.id,
                        status=TaskStatus.SKIPPED,
                        duration_seconds=0.0,
                        error=node.error or "Skipped due to upstream dependency failure",
                    )
                )

        return receipts

    def _retry_rate_limited(self, dag: TaskDAG, node: TaskNode, exc: Exception) -> bool:
        """Back off the task's provider and requeue it while retries remain."""
        limits = self.governor.limits
        if isinstance(exc, RateLimited):
            retry_after, provider = exc.retry_after, exc.provider or self._provider(node)
        elif QuarantineManager.is_rate_limit(str(exc)):
            retry_after, provider = None, self._provider(node)
        else:
            return False
        self.governor.signal_rate_limit(
            retry_after_seconds=limits.default_retry_after_seconds if retry_after is None else retry_after,
            provider=provider,
        )
        if node.attempts > limits.max_rate_limit_retries:
            return False
        dag.requeue(node.id)
        return True

    async def _run_node(self, node: TaskNode) -> Any:
        if node.blocking:
            return await asyncio.to_thread(node.action)
        return await node.action()
