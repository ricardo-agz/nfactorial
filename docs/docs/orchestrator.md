# Orchestrator

The `Orchestrator` powers Factorial's queued and distributed runtime. Use it when you need:

- Redis-backed task execution
- waits, hooks, signals, or subagents
- human control-plane actions
- retries and recovery
- task handles, batches, and streaming updates

## Basic Setup

```python
from factorial import AgentWorkerConfig, MaintenanceWorkerConfig, Orchestrator


orchestrator = Orchestrator(
    redis_host="localhost",
    redis_port=6379,
)

orchestrator.register(
    agent=my_agent,
    agent_worker_config=AgentWorkerConfig(workers=10),
    maintenance_worker_config=MaintenanceWorkerConfig(),
)


if __name__ == "__main__":
    orchestrator.run()
```

## Common Configuration

### Redis

```python
orchestrator = Orchestrator(
    redis_host="localhost",
    redis_port=6379,
    redis_db=0,
    redis_max_connections=50,
)
```

### Agent workers

```python
from factorial import AgentWorkerConfig


config = AgentWorkerConfig(
    workers=10,
    batch_size=25,
    max_retries=5,
    heartbeat_interval=5,
    turn_timeout=120,
)
```

### Maintenance workers

Maintenance workers recover dropped tasks and clean up expired data.

```python
from factorial import MaintenanceWorkerConfig, TaskTTLConfig


config = MaintenanceWorkerConfig(
    interval=10,
    workers=1,
    task_ttl=TaskTTLConfig(
        completed_ttl=3600,
        failed_ttl=86400,
        cancelled_ttl=1800,
    ),
)
```

## Enqueueing Work

### Single task

```python
task = await orchestrator.enqueue(
    my_agent,
    input="Analyze this data.",
    owner_id="user123",
    state={"priority": 1},
    metadata={"source": "api"},
    idempotency_key="request-123",
)
```

`enqueue(...)` returns a `TaskHandle`.

If you retry the same enqueue call with the same `idempotency_key` and payload, the orchestrator returns the same task instead of creating a duplicate. Reusing the same key with a different payload raises a conflict.

### Batch enqueue

Use `enqueue_many(...)` when you want one agent run per item:

```python
from factorial import with_context


batch = await orchestrator.enqueue_many(
    my_agent,
    [
        with_context("First item.", state={"priority": 1}),
        with_context("Second item.", metadata={"source": "batch-override"}),
        "Third item.",
    ],
    owner_id="user123",
    state={"priority": 0},
    metadata={"source": "default"},
)

results = await batch.wait()
```

`enqueue_many(...)` returns a `BatchHandle`.

## `TaskHandle`

The `TaskHandle` API is the main way to inspect and control queued runs.

### Inspect state

```python
snapshot = await task.snapshot()
print(snapshot.status)
print(snapshot.wait)
print(snapshot.pending_hooks)
```

Snapshots include the task status, current state/metadata, last turn summary, any active wait, pending hook snapshots, and pending child task IDs.

### Wait for completion

```python
result = await task.wait()
print(result.output)
```

### Stream typed updates

```python
async for event in task.updates():
    print(type(event).__name__)
```

### Steer a task

Use steering to inject additional user input into an active run:

```python
await task.steer("Focus on the financial risks.")
```

You can also pass an explicit message list instead of a plain string.

### Cancel a task

```python
await task.cancel()
```

### Wake a waiting task

If a task is parked on a wakeable wait, you can resume it manually:

```python
woke = await task.wake("Approval granted. Continue.")
print(woke)
```

`wake()` only applies to tasks that are currently waiting. It does not replace hook resolution and it does not wake tasks blocked on pending child tasks.

### Branch from a terminal task

Create a new queued task from a completed, failed, or cancelled source task while reusing its context:

```python
branched = await task.branch(
    "Revise with stronger evidence.",
    state={"priority": 8},
    metadata={"source": "branch"},
)
```

### Work with pending hooks

```python
pending_hooks = await task.hooks()
first_hook = pending_hooks[0]

result = await first_hook.complete({"approved": True})
print(result.status, result.task_resumed)
```

You can also look up a specific hook by ID with `await task.hook(hook_id)`.

## `BatchHandle`

`BatchHandle` exposes the batch-level control surface:

```python
snapshot = await batch.snapshot()
print(snapshot.total_tasks, snapshot.remaining_tasks)

async for event in batch.updates():
    print(type(event).__name__)

results = await batch.wait()
await batch.cancel()
```

Each batch also exposes `batch.tasks` if you want the individual `TaskHandle` objects.

## Resuming terminal tasks

Resume a terminal task as a brand-new queued task:

```python
resumed = await orchestrator.resume_task(
    task_id=task.id,
    messages=[{"role": "user", "content": "Please revise with stricter evidence."}],
    idempotency_key="resume:task-123:revision-1",
)
```

`resume_task(...)` creates a new task ID, carries forward prior context, appends your new messages, preserves ancestry, and resets run-scoped fields such as output and verification counters.

## Human control-plane messaging

The orchestrator can also deliver human-originated messages into active tasks and groups.

### Message a specific task

```python
report = await orchestrator.message_task(
    task_id=task.id,
    owner_id="user123",
    content="Please respond to the latest comment.",
    data={"kind": "human_followup"},
)
```

### Message a group

```python
report = await orchestrator.message_group(
    owner_id="user123",
    content="Discussion is now open.",
    data={"kind": "moderator_broadcast"},
    group_name="research",
    task_id=task.id,
)
```

Both methods return delivery metadata such as `delivered_task_ids`, `skipped_inactive_task_ids`, and `failed_task_ids`.

## Hook resolution APIs

External servers and UIs can resolve pending hooks directly:

```python
resolution = await orchestrator.resolve_hook(
    hook_id=hook_id,
    payload={"approved": True},
    token=token,
    idempotency_key="approve:123",
)
```

If you need a fresh token for an already-pending hook, rotate it first:

```python
token = await orchestrator.rotate_hook_token(
    hook_id=hook_id,
    revoke_previous=False,
)
```

## Subscribing to updates

Use `owner_id` to subscribe to the raw update stream for a user or session:

```python
async for update in orchestrator.subscribe_to_updates(owner_id="user123"):
    print(update["event_type"], update["task_id"])
```

See [Events](events.md) for the streaming model and filters.

## Observability

### Dashboard

The orchestrator includes a built-in dashboard:

```python
from factorial import ObservabilityConfig


orchestrator = Orchestrator(
    observability_config=ObservabilityConfig(
        enabled=True,
        host="0.0.0.0",
        port=8080,
        dashboard_name="My AI System",
    ),
)
```

Open [http://localhost:8080/observability](http://localhost:8080/observability).

### Metrics timeline

```python
from factorial import MetricsTimelineConfig


config = MetricsTimelineConfig(
    timeline_duration=3600,
    bucket_size="minutes",
    retention_multiplier=2.0,
)
```
