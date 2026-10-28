"""Main FlowEngine orchestrating bounded parallel execution over TaskDAG."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from flowguard.governor import FlowGovernor, ResourceLimits
from flowguard.dag import TaskDAG, TaskNode, TaskStatus
from flowguard.quarantine import QuarantineManager, GateFailureClassification


@dataclass(frozen=True)
class TaskExecutionReceipt:
    task_id: str
    status: TaskStatus
    duration_seconds: float
    result: Any = None
    error: str | None = None
    quarantine_code: GateFailureClassification | None = None


class FlowEngine:
    """Executes a TaskDAG with bounded concurrency, adaptive backpressure, and fast quarantine."""

    def __init__(
        self,
        governor: FlowGovernor | None = None,
        quarantine: QuarantineManager | None = None,
    ) -> None:
        self.governor = governor or FlowGovernor()
        self.quarantine = quarantine or QuarantineManager()

    async def execute_dag(self, dag: TaskDAG, poll_interval: float = 0.05) -> list[TaskExecutionReceipt]:
        receipts: list[TaskExecutionReceipt] = []
        running_tasks: dict[asyncio.Task, TaskNode] = {}

        while not dag.is_finished() or running_tasks:
            # 1. Check current admissible capacity
            admissible_slots = self.governor.evaluate_capacity()
            available_slots = max(0, admissible_slots - len(running_tasks))

            # 2. Schedule ready tasks up to available slot capacity
            if available_slots > 0:
                ready_nodes = dag.get_ready_tasks()
                for node in ready_nodes[:available_slots]:
                    # Check rate limiting for provider tasks
                    if node.resource_type.startswith("llm") and self.governor.is_rate_limited:
                        continue

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
            done, _ = await asyncio.wait(
                running_tasks.keys(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            for finished_task in done:
                node = running_tasks.pop(finished_task)
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
                        )
                    )
                except Exception as exc:
                    err_msg = str(exc)
                    classification, retryable = self.quarantine.classify_and_quarantine(
                        node.id, err_msg, time.time()
                    )

                    if classification == GateFailureClassification.PROVIDER_RATE_LIMIT:
                        self.governor.signal_rate_limit(retry_after_seconds=15.0)

                    dag.mark_quarantined(node.id, f"{classification.value}: {err_msg}")
                    receipts.append(
                        TaskExecutionReceipt(
                            task_id=node.id,
                            status=TaskStatus.QUARANTINED,
                            duration_seconds=duration,
                            error=err_msg,
                            quarantine_code=classification,
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

    async def _run_node(self, node: TaskNode) -> Any:
        return await node.action()
