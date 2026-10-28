"""Comprehensive tests for semcod/flowguard components."""

import asyncio
import pytest

from flowguard.governor import FlowGovernor, ResourceLimits, ResourceSnapshot
from flowguard.dag import TaskDAG, TaskStatus
from flowguard.quarantine import QuarantineManager, GateFailureClassification
from flowguard.engine import FlowEngine


def test_governor_aimd_capacity():
    limits = ResourceLimits(
        min_free_ram_gb=2.0,
        max_cpu_load_ratio=0.85,
        max_concurrent_slots=6,
        min_concurrent_slots=1,
    )
    governor = FlowGovernor(limits=limits)

    # Initial capacity should be halfway
    capacity = governor.evaluate_capacity()
    assert 1 <= capacity <= 6

    # Critical RAM triggers multiplicative decrease
    stressed_snapshot = ResourceSnapshot(
        cpu_load_1m=1.0,
        cpu_count=8,
        ram_total_mb=16384.0,
        ram_available_mb=1024.0,  # 1.0 GB < 2.0 GB min
        timestamp=100.0,
    )
    assert governor.evaluate_capacity(stressed_snapshot) == 1


def test_quarantine_classification():
    qm = QuarantineManager()

    code, retryable = qm.classify_and_quarantine(
        "task-1", "ValueError: GOAL_DEPENDENCY_PIN_MISMATCH: uv.lock", timestamp=100.0
    )
    assert code == GateFailureClassification.PIN_MISMATCH
    assert not retryable

    code, retryable = qm.classify_and_quarantine(
        "task-2", "HTTP 429 Too Many Requests: SUBLLM-PROVIDER-RATE-LIMIT", timestamp=101.0
    )
    assert code == GateFailureClassification.PROVIDER_RATE_LIMIT
    assert retryable

    assert len(qm.records) == 2


async def _test_flowguard_parallel_execution_and_hol_isolation():
    """Verify that independent tasks run concurrently and failure in branch A doesn't block branch B."""
    dag = TaskDAG()
    execution_order = []

    async def task_a1():
        await asyncio.sleep(0.05)
        execution_order.append("a1")
        return "a1_done"

    async def task_a2_fail():
        await asyncio.sleep(0.02)
        # Deterministic pin failure
        raise ValueError("LOCKED_MANIFEST_DEPENDENCY_PIN_MISMATCH")

    async def task_a3_dependent():
        execution_order.append("a3")
        return "a3_done"

    async def task_b1():
        await asyncio.sleep(0.04)
        execution_order.append("b1")
        return "b1_done"

    async def task_b2():
        await asyncio.sleep(0.04)
        execution_order.append("b2")
        return "b2_done"

    # Branch A: a1 -> a2_fail -> a3_dependent
    dag.add_task("task_a1", task_a1)
    dag.add_task("task_a2_fail", task_a2_fail, needs=["task_a1"])
    dag.add_task("task_a3_dependent", task_a3_dependent, needs=["task_a2_fail"])

    # Branch B (Independent): b1 -> b2
    dag.add_task("task_b1", task_b1)
    dag.add_task("task_b2", task_b2, needs=["task_b1"])

    limits = ResourceLimits(max_concurrent_slots=4)
    engine = FlowEngine(governor=FlowGovernor(limits=limits))

    receipts = await engine.execute_dag(dag)
    receipt_map = {r.task_id: r for r in receipts}

    # Branch A assertions: a1 passed, a2 quarantined, a3 skipped
    assert receipt_map["task_a1"].status == TaskStatus.COMPLETED
    assert receipt_map["task_a2_fail"].status == TaskStatus.QUARANTINED
    assert receipt_map["task_a2_fail"].quarantine_code == GateFailureClassification.PIN_MISMATCH
    assert receipt_map["task_a3_dependent"].status == TaskStatus.SKIPPED

    # Branch B assertions: independent branch completed fully despite A's failure
    assert receipt_map["task_b1"].status == TaskStatus.COMPLETED
    assert receipt_map["task_b2"].status == TaskStatus.COMPLETED
    assert "b1" in execution_order
    assert "b2" in execution_order
    assert "a3" not in execution_order


def test_flowguard_parallel_execution_and_hol_isolation():
    asyncio.run(_test_flowguard_parallel_execution_and_hol_isolation())
