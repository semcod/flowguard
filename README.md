# semcod/flowguard

Autonomous bounded concurrent dispatch and flow governor for Heyasa and Planfile.

## Overview

Just as **Hostguard** protects hardware from resource exhaustion (CPU, RAM, storage, runaway daemons), **Flowguard** protects the task pipeline from:
- **Head-of-Line (HoL) Blocking**: isolating deterministic failures (`*_DEPENDENCY_PIN_MISMATCH`, rate limits) to allow independent branches to progress.
- **Serial Bottlenecks**: executing independent task DAGs concurrently within dynamic host and API limits rather than sequentially.
- **Resource Saturation**: dynamically scaling worker slots using an Additive Increase / Multiplicative Decrease (AIMD) algorithm backed by host telemetry.

## Architecture

- **`FlowGovernor`**: Monitors `/proc/loadavg` and `/proc/meminfo` and throttles slot allocation when CPU or memory pressure rises.
- **`TaskDAG`**: Direct Acyclic Graph scheduler where upstream task failure only skips downstream dependents, leaving orthogonal branches unblocked.
- **`QuarantineManager`**: Fast-fails and segregates known failure signatures into quarantine receipts without stalling the run loop.
- **`FlowEngine`**: Asynchronous orchestration engine dispatching ready nodes across available slots.

## Install

```bash
pip install semcod-flowguard   # imports as `flowguard`
```

## Quickstart

```python
import asyncio
from flowguard import FlowEngine, TaskDAG, FlowGovernor

async def main():
    dag = TaskDAG()
    dag.add_task("task_a", lambda: asyncio.sleep(0.1))
    dag.add_task("task_b", lambda: asyncio.sleep(0.1))

    engine = FlowEngine()
    receipts = await engine.execute_dag(dag)
    for r in receipts:
        print(f"{r.task_id}: {r.status}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Resources, providers and blocking tasks

```python
dag.add_task("edit", run_edit, resources={"writer:paxlet-com/heyasa/ticket-144": 1})
dag.add_task("ask", ask_llm, provider="llm:openrouter")
dag.add_task("willman", submit, resources={"engine:willman": 1}, blocking=True)

limits = ResourceLimits.from_mapping({
    "max_concurrent_slots": 8,
    "resource_capacity": {"engine:willman": 2},
})
```

- `writer:*` resources are exclusive (capacity 1): one writer per worktree.
- Other named resources are bounded by `resource_capacity`; unnamed ones are unbounded.
- A task raising `RateLimited(retry_after=..)` (or a 429 message) backs off only its
  provider and is requeued up to `max_rate_limit_retries`; other providers keep running.
- `blocking=True` runs a synchronous callable in a worker thread.
- `resource_type="io"` marks remote/I/O-bound work: it uses `max_io_slots`, is not
  throttled by host CPU load, and still drops to the minimum when RAM is critical.
- Flowguard schedules admitted work only; it grants no execution authority.

## Cancellation and shutdown

Cancelling `execute_dag` cancels its running async actions and waits for their
cleanup before propagating `CancelledError`. Repeated cancellation does not
interrupt that cleanup. Interrupted nodes become `FAILED` with a note that
effects may have occurred; cancellation does not roll back work or authorize
automatic retries. Unstarted nodes are left unexecuted.

A blocking callable runs in an OS thread, which cannot be stopped by cancelling
its asyncio wrapper. Flowguard waits for the actual callable to finish before
releasing its resources, including exclusive `writer:*` reservations. Blocking
actions should provide their own bounded I/O or cooperative stop mechanism;
shutdown can otherwise wait indefinitely for them.

An action that cancels itself becomes a failed node: dependent tasks are skipped,
while independent branches continue. Scheduler failures also drain running
actions before releasing ownership, allowing the engine to be reused safely.

## Testing

```bash
pytest -v tests/
```
