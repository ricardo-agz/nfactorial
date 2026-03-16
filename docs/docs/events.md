# Events

Factorial exposes events in two places:

- direct runs via `agent.stream(...)`
- queued runs via `TaskHandle.updates()` or `orchestrator.subscribe_to_updates(...)`

## Direct Run Events

Use `agent.stream(...)` when you want typed lifecycle events for an in-process run:

```python
from factorial import FinishEvent


async for event in agent.stream("Summarize the repo."):
    print(type(event).__name__)

    if isinstance(event, FinishEvent):
        print(event.output)
```

`agent.stream(...)` yields typed event objects such as:

- `StartEvent`
- `TurnStartEvent`
- `ModelStartEvent`
- `ModelFinishEvent`
- `ToolStartEvent`
- `ToolFinishEvent`
- `TurnFinishEvent`
- `FinishEvent`

## Queued Run Events

When you enqueue work through the orchestrator, you have two main choices.

### Typed events from a task handle

```python
task = await orchestrator.enqueue(
    agent,
    input="Summarize the repo.",
    owner_id="user123",
)

async for event in task.updates():
    print(type(event).__name__)
```

You can filter typed handle events by class:

```python
from factorial import FinishEvent


async for event in task.updates(types=(FinishEvent,)):
    print(event.output)
```

### Raw event payloads from the orchestrator

Use `subscribe_to_updates(...)` when you want raw dictionaries for a user/session fan-out channel:

```python
async for update in orchestrator.subscribe_to_updates(owner_id="user123"):
    print(update["event_type"], update["task_id"])
```

This is what example API servers usually forward over WebSocket or SSE:

```python
@app.get("/api/events/{user_id}")
async def stream_updates(user_id: str):
    async def event_stream():
        async for update in orchestrator.subscribe_to_updates(owner_id=user_id):
            yield f"data: {json.dumps(update)}\n\n"
```

## Filtering queued updates

`subscribe_to_updates(...)` supports three useful filters:

- `task_ids=[...]`
- `event_types=[...]`
- `event_pattern=r"..."`

```python
async for update in orchestrator.subscribe_to_updates(
    owner_id="user123",
    event_types=["run_completed", "run_failed", "run_cancelled"],
):
    print(update)
```

```python
async for update in orchestrator.subscribe_to_updates(
    owner_id="user123",
    task_ids=[task.id],
):
    print(update)
```

```python
async for update in orchestrator.subscribe_to_updates(
    owner_id="user123",
    event_pattern=r"progress_update_.*",
):
    print(update)
```

## Common queued event fields

Queued updates are published as dictionaries with fields like:

```python
{
    "event_type": "run_started",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2026-03-15T12:00:00Z",
    "data": {...},
}
```

When you want typed event objects from these payloads, use `parse_event(...)`:

```python
from factorial import parse_event


event = parse_event(update_payload)
print(type(event).__name__)
```

## Event families

The exact set of event names grows as the runtime surface grows, but the main families are:

- run lifecycle: `run_started`, `run_completed`, `run_failed`, `run_cancelled`
- progress: `progress_update_completion_*`, `progress_update_tool_action_*`, `progress_update_run_turn_*`
- queue/control-plane: `task_failed`, `task_retried`, `task_resumed`, `run_steering_applied`
- verification: `verification_passed`, `verification_rejected`
- waits, hooks, and messaging: emitted when tasks park, resume, exchange messages, or resolve approvals

In practice, most UIs only need a handful of these:

- run start / finish
- tool start / finish / failure
- wait enter / wake
- hook pending / resolved
- messaging sent / received

## Custom Events

Custom agent implementations can publish additional events through `ExecutionContext.events`:

```python
from factorial import Agent, AgentEvent, ExecutionContext


class CustomAgent(Agent):
    async def run_turn(self, agent_ctx):
        execution_ctx = ExecutionContext.current()

        await execution_ctx.events.publish_event(
            AgentEvent(
                event_type="analysis_started",
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                data={"phase": "analysis"},
            )
        )

        return await super().run_turn(agent_ctx)
```
