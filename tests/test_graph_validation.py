"""Malformed DAGs must be rejected before any task can produce effects."""
import asyncio
import sys

import pytest

from flowguard import FlowEngine, FlowGovernor, ResourceLimits, ResourceSnapshot, TaskDAG, TaskStatus


def engine():
    calm = ResourceSnapshot(cpu_load_1m=0.1, cpu_count=8, ram_total_mb=16384,
                            ram_available_mb=12288, timestamp=0)
    return FlowEngine(FlowGovernor(ResourceLimits(), snapshot_source=lambda: calm))


@pytest.mark.parametrize("self_cycle", [False, True])
def test_cycles_are_rejected_before_independent_actions(self_cycle):
    async def scenario():
        effects = []

        async def action():
            effects.append("effect")

        flow, dag = engine(), TaskDAG()
        dag.add_task("independent", action, resources={"writer:probe": 1})
        dag.add_task("a", action, needs=["a" if self_cycle else "b"])
        if not self_cycle:
            dag.add_task("b", action, needs=["a"])
        with pytest.raises(ValueError, match="DependencyCycle:"):
            await asyncio.wait_for(flow.execute_dag(dag), 0.2)
        assert effects == []
        assert flow._in_use == {}
        assert all(node.status == TaskStatus.PENDING for node in dag.nodes)

    asyncio.run(scenario())


@pytest.mark.parametrize("parent_failed", [False, True])
def test_missing_reference_cannot_hide_behind_another_parent(parent_failed):
    async def scenario():
        effects = []

        async def action():
            effects.append("effect")

        flow, dag = engine(), TaskDAG()
        dag.add_task("child", action, needs=["parent", "missing"])
        dag.add_task("parent", action, resources={"writer:probe": 1})
        if parent_failed:
            dag.mark_failed("parent", "previous failure")
        before = {node.id: node.status for node in dag.nodes}
        with pytest.raises(KeyError, match="Dependency 'missing' not found for task 'child'"):
            await asyncio.wait_for(flow.execute_dag(dag), 0.2)
        assert effects == []
        assert flow._in_use == {}
        assert {node.id: node.status for node in dag.nodes} == before

    asyncio.run(scenario())


def test_forward_shared_and_duplicate_dependencies_remain_valid():
    async def scenario():
        effects = []

        def action(name):
            async def run():
                effects.append(name)
            return run

        flow, dag = engine(), TaskDAG()
        dag.add_task("child", action("child"), needs=["left", "right", "right"])
        dag.add_task("left", action("left"), needs=["root"])
        dag.add_task("right", action("right"), needs=["root"])
        dag.add_task("root", action("root"))
        receipts = await asyncio.wait_for(flow.execute_dag(dag), 1)
        assert len(receipts) == 4
        assert all(r.status == TaskStatus.COMPLETED for r in receipts)
        assert effects[0] == "root" and effects[-1] == "child"
        assert sorted(effects) == ["child", "left", "right", "root"]

    asyncio.run(scenario())


def test_deep_completed_graph_validation_does_not_recurse_or_reexecute():
    async def scenario():
        dag = TaskDAG()
        count = sys.getrecursionlimit() + 100

        async def must_not_run():
            pytest.fail("completed action was re-executed")

        for i in range(count):
            dag.add_task(str(i), must_not_run, needs=[str(i + 1)] if i + 1 < count else [])
            dag.mark_completed(str(i), i)
        assert await engine().execute_dag(dag) == []
        assert all(node.status == TaskStatus.COMPLETED for node in dag.nodes)

    asyncio.run(scenario())
