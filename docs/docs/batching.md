# Batching

Batching lets you enqueue many independent agent tasks concurrently and track their progress as a group.

<br/>

## Spawning concurrent subagents

When you spawn multiple concurrent subagents with the `spawn_child_tasks` method, a batch is automatically created.

```python
from factorial import AgentContext, ExecutionContext, forking_tool


@forking_tool(timeout=600)
async def fanout_research(
    queries: list[str],
    agent_ctx: AgentContext,
    execution_ctx: ExecutionContext,
) -> list[str]:
    """Spawn one subagent per query, concurrently, as a single batch."""
    payloads = [AgentContext(query=q) for q in queries]
    batch = await execution_ctx.spawn_child_tasks(search_agent, payloads)

    return batch.task_ids
```

## Enqueuing many top‑level tasks

```python
import asyncio
from factorial import Orchestrator, Agent, AgentContext

agent = Agent(...)
orchestrator = Orchestrator(...)
orchestrator.register_runner(agent=agent)

async def enqueue_many(queries: list[str], user_id: str) -> str:
    payloads = [AgentContext(query=q) for q in queries]
    batch = await orchestrator.enqueue_batch(agent=agent, owner_id=user_id, payloads=payloads)
    return batch.id
```

<br/>

## Progress and events

When you create a batch via `ExecutionContext.spawn_child_tasks` or via `orchestrator.enqueue_batch(...)`, Factorial tracks the group under a `batch_id` and emits structured events you can subscribe to using the orchestrator’s updates channel.

- **batch_progress**: Emitted periodically while the batch is active.
  - Fields: `batch_id`, `owner_id`, `progress` (0–100), `completed_tasks`, `total_tasks`, `status` (e.g., `active`).
- **batch_completed**: Emitted once when all tasks in the batch reach a terminal state.
- **batch_cancelled**: Emitted when a batch is cancelled as a group. See cancellation section.

Subscribe to updates and filter for batch events:

```python
import asyncio
from factorial import Orchestrator

orchestrator = Orchestrator(...)

async def stream_batch(batch_id: str, owner_id: str):
    async for evt in orchestrator.subscribe_to_updates(
        owner_id=owner_id,
        event_types=["batch_progress", "batch_completed", "batch_cancelled"],
    ):
        if evt.get("batch_id") == batch_id:
            print(evt)

asyncio.run(stream_batch(batch_id="...", owner_id="user123"))
```

### Progress granularity and `max_turns`

Batch progress adapts automatically:

- If your agent sets `max_turns`, the batch’s theoretical maximum progress is `max_turns * num_tasks`. Progress updates are emitted on each turn, making the progress bar smooth.
- If `max_turns` is not set, progress advances per completed task.

<br/>

## Cancellation

- For subagent batches (spawned via a forking tool), cancelling the parent task will cause the orchestrator to unlink any remaining children and clean up the parent. This is the simplest way to abort a batch that was created inside an agent run.
- For top‑level batches created via `orchestrator.enqueue_batch(...)`, cancel the entire group with `orchestrator.cancel_batch(batch_id)`. This emits a `batch_cancelled` event and attempts to cancel each task in the batch concurrently.
- You can still cancel individual tasks at any time using `orchestrator.cancel_task(task_id)`.

Example:

```python
updated = await orchestrator.cancel_batch(batch_id)
print(updated.metadata.status)  # "cancelled"
```

<br/>

## Error handling

Individual task failures do not stop the batch. Terminal states (completed/failed/cancelled) count towards the whole task completion progress.
