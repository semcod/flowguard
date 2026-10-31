"""Cancellation retains writer ownership until the real action has stopped."""
import asyncio
import threading

import pytest

from flowguard import FlowEngine, FlowGovernor, ResourceLimits, ResourceSnapshot, TaskDAG, TaskStatus


def engine():
    snapshot = ResourceSnapshot(cpu_load_1m=0.1, cpu_count=8, ram_total_mb=16384,
                                ram_available_mb=12288, timestamp=0)
    return FlowEngine(FlowGovernor(ResourceLimits(), snapshot_source=lambda: snapshot))


def test_parent_cancellation_drains_async_cleanup_and_allows_engine_reuse():
    async def scenario():
        started, cleaning, finish, stopped = (asyncio.Event() for _ in range(4))

        async def action():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await finish.wait()
                stopped.set()

        flow, dag = engine(), TaskDAG()
        dag.add_task("writer", action, resources={"writer:probe": 1})
        run = asyncio.create_task(flow.execute_dag(dag))
        try:
            await asyncio.wait_for(started.wait(), 1)
            run.cancel()
            await asyncio.wait_for(cleaning.wait(), 1)
            run.cancel()  # repeated caller cancellation must not interrupt cleanup
            await asyncio.sleep(0)
            assert not run.done()
            assert flow._in_use["writer:probe"] == 1
            finish.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(run, 1)
            assert stopped.is_set()
            assert dag.get_node("writer").status == TaskStatus.FAILED
            assert dag.get_node("writer").completed_at is not None
            assert not any(flow._in_use.values())
            fresh = TaskDAG()
            fresh.add_task("next", lambda: asyncio.sleep(0, result="ok"),
                           resources={"writer:probe": 1})
            receipt, = await asyncio.wait_for(flow.execute_dag(fresh), 1)
            assert receipt.result == "ok"
        finally:
            finish.set()
            run.cancel()
            await asyncio.gather(run, return_exceptions=True)

    asyncio.run(scenario())


def test_blocking_writer_remains_exclusive_until_thread_finishes():
    async def scenario():
        started, finish = threading.Event(), threading.Event()
        second_started = asyncio.Event()

        def blocking():
            started.set()
            finish.wait()

        async def next_writer():
            second_started.set()

        flow, first, second = engine(), TaskDAG(), TaskDAG()
        first.add_task("thread", blocking, blocking=True, resources={"writer:probe": 1})
        second.add_task("next", next_writer, resources={"writer:probe": 1})
        run = asyncio.create_task(flow.execute_dag(first))
        competitor = None
        try:
            assert await asyncio.to_thread(started.wait, 1)
            run.cancel()
            await asyncio.sleep(0)
            run.cancel()
            competitor = asyncio.create_task(flow.execute_dag(second, poll_interval=0.001))
            await asyncio.sleep(0.02)
            assert not run.done()
            assert not second_started.is_set()
            assert flow._in_use["writer:probe"] == 1
            finish.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(run, 1)
            await asyncio.wait_for(competitor, 1)
            assert second_started.is_set()
            assert not any(flow._in_use.values())
        finally:
            finish.set()
            tasks = [t for t in (run, competitor) if t is not None]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())


def test_self_cancelled_child_fails_without_stalling_independent_branch():
    async def scenario():
        async def cancel():
            raise asyncio.CancelledError("action stopped")

        flow, dag = engine(), TaskDAG()
        dag.add_task("cancel", cancel, resources={"writer:probe": 1})
        dag.add_task("dependent", lambda: asyncio.sleep(0), needs=["cancel"])
        dag.add_task("independent", lambda: asyncio.sleep(0, result="ok"))
        receipts = {r.task_id: r for r in await asyncio.wait_for(flow.execute_dag(dag), 1)}
        assert receipts["cancel"].status == TaskStatus.FAILED
        assert "cancel" in receipts["cancel"].error.lower()
        assert receipts["dependent"].status == TaskStatus.SKIPPED
        assert receipts["independent"].result == "ok"
        assert dag.is_finished()
        assert not any(flow._in_use.values())

    asyncio.run(scenario())


def test_scheduler_failure_also_drains_its_running_actions():
    async def scenario():
        started, finish, stopped = (asyncio.Event() for _ in range(3))

        async def action():
            started.set()
            try:
                await finish.wait()
            finally:
                stopped.set()

        flow, dag = engine(), TaskDAG()
        original = flow.governor.snapshot

        def snapshot():
            if started.is_set():
                raise RuntimeError("snapshot unavailable")
            return original()

        flow.governor.snapshot = snapshot
        dag.add_task("writer", action, resources={"writer:probe": 1})
        try:
            with pytest.raises(RuntimeError, match="snapshot unavailable"):
                await flow.execute_dag(dag, poll_interval=0.001)
            assert stopped.is_set()
            assert dag.get_node("writer").status == TaskStatus.FAILED
            assert not any(flow._in_use.values())
        finally:
            finish.set()

    asyncio.run(scenario())
