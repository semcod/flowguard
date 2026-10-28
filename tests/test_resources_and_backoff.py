"""Resource classes, provider-scoped backoff and blocking tasks."""

import asyncio
import time

import pytest

from flowguard import FlowEngine, FlowGovernor, RateLimited, ResourceLimits, ResourceSnapshot, TaskDAG, TaskStatus


def calm_snapshot():
    return ResourceSnapshot(cpu_load_1m=0.1, cpu_count=8, ram_total_mb=16384.0,
                            ram_available_mb=12288.0, timestamp=0.0)


def engine(**limits):
    limits.setdefault("max_concurrent_slots", 8)
    return FlowEngine(governor=FlowGovernor(ResourceLimits(**limits), snapshot_source=calm_snapshot))


class Tracker:
    def __init__(self):
        self.active = {}
        self.peak = {}

    def task(self, key, delay=0.05):
        async def run():
            self.active[key] = self.active.get(key, 0) + 1
            self.peak[key] = max(self.peak.get(key, 0), self.active[key])
            await asyncio.sleep(delay)
            self.active[key] -= 1
            return key
        return run


async def test_writer_resource_is_exclusive_per_worktree():
    tracker = Tracker()
    dag = TaskDAG()
    for i in range(3):
        dag.add_task(f"a{i}", tracker.task("repo/wt-a"), resources={"writer:repo/wt-a": 1})
        dag.add_task(f"b{i}", tracker.task("repo/wt-b"), resources={"writer:repo/wt-b": 1})
    receipts = await engine().execute_dag(dag)
    assert all(r.status == TaskStatus.COMPLETED for r in receipts)
    assert tracker.peak == {"repo/wt-a": 1, "repo/wt-b": 1}


async def test_engine_capacity_bounds_concurrency():
    tracker = Tracker()
    dag = TaskDAG()
    for i in range(5):
        dag.add_task(f"w{i}", tracker.task("willman"), resources={"engine:willman": 1})
    await engine(resource_capacity={"engine:willman": 2}).execute_dag(dag)
    assert tracker.peak["willman"] == 2


async def test_rate_limit_defers_only_its_provider():
    calls = {"a": 0}
    finished = {}

    async def provider_a():
        calls["a"] += 1
        if calls["a"] == 1:
            raise RateLimited("429", retry_after=0.2)
        finished["a"] = time.monotonic()
        return "a"

    async def provider_b():
        await asyncio.sleep(0.01)
        finished["b"] = time.monotonic()
        return "b"

    dag = TaskDAG()
    dag.add_task("a", provider_a, provider="llm:a")
    dag.add_task("b", provider_b, provider="llm:b")
    start = time.monotonic()
    receipts = {r.task_id: r for r in await engine().execute_dag(dag)}
    assert receipts["a"].status == TaskStatus.COMPLETED and receipts["a"].attempts == 2
    assert finished["b"] - start < 0.15
    assert finished["a"] - start >= 0.2


async def test_rate_limit_retries_are_bounded():
    async def always_limited():
        raise RateLimited("429 Too Many Requests", retry_after=0.0)

    dag = TaskDAG()
    dag.add_task("x", always_limited, provider="llm:x")
    receipt, = await engine(max_rate_limit_retries=2).execute_dag(dag)
    assert receipt.status == TaskStatus.QUARANTINED
    assert receipt.attempts == 3


async def test_blocking_tasks_run_in_parallel_threads():
    dag = TaskDAG()
    for i in range(3):
        dag.add_task(f"s{i}", lambda: time.sleep(0.1) or "ok", blocking=True)
    start = time.monotonic()
    receipts = await engine().execute_dag(dag)
    assert [r.result for r in receipts] == ["ok"] * 3
    assert time.monotonic() - start < 0.25


async def test_unsatisfiable_demand_is_rejected_before_execution():
    dag = TaskDAG()
    dag.add_task("x", lambda: asyncio.sleep(0), resources={"writer:repo/wt": 2})
    with pytest.raises(ValueError, match="capacity 1"):
        await engine().execute_dag(dag)


def test_limits_from_policy_mapping_fail_closed():
    limits = ResourceLimits.from_mapping({"max_concurrent_slots": 4,
                                          "resource_capacity": {"engine:willman": 2}})
    assert limits.capacity_of("engine:willman") == 2
    assert limits.capacity_of("writer:r/w") == 1
    assert limits.capacity_of("cpu") is None
    with pytest.raises(ValueError, match="Unknown"):
        ResourceLimits.from_mapping({"max_slots": 4})


async def test_io_tasks_are_not_throttled_by_cpu_load():
    def loaded():
        return ResourceSnapshot(cpu_load_1m=24.0, cpu_count=16, ram_total_mb=65536.0,
                                ram_available_mb=40000.0, timestamp=0.0)
    tracker = Tracker()
    dag = TaskDAG()
    for i in range(4):
        dag.add_task(f"io{i}", tracker.task("io"), resource_type="io")
        dag.add_task(f"cpu{i}", tracker.task("cpu"))
    governor = FlowGovernor(ResourceLimits(max_io_slots=4), snapshot_source=loaded)
    await FlowEngine(governor=governor).execute_dag(dag)
    assert tracker.peak == {"io": 4, "cpu": 1}


async def test_io_tasks_back_off_when_ram_is_critical():
    def low_ram():
        return ResourceSnapshot(cpu_load_1m=0.1, cpu_count=16, ram_total_mb=65536.0,
                                ram_available_mb=512.0, timestamp=0.0)
    tracker = Tracker()
    dag = TaskDAG()
    for i in range(3):
        dag.add_task(f"io{i}", tracker.task("io"), resource_type="io")
    await FlowEngine(governor=FlowGovernor(ResourceLimits(), snapshot_source=low_ram)).execute_dag(dag)
    assert tracker.peak == {"io": 1}
