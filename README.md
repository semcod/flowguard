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

## Testing

```bash
pytest -v tests/
```
