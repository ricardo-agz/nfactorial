# Context

Factorial exposes two complementary context objects:

- `AgentContext`: persisted conversation and typed agent state
- `ExecutionContext`: runtime-only task metadata and runtime namespaces

## `AgentContext`

`AgentContext` is the state your agent carries across turns. It is available in tools, `prepare_turn`, verifiers, and custom agent implementations.

Common fields:

```python
agent_ctx.messages      # normalized conversation history
agent_ctx.state         # typed state payload
agent_ctx.metadata      # typed metadata payload
agent_ctx.turn_number   # current turn number, 1-based
```

Use `Agent[StateT, MetadataT]` when you want typed state or metadata. When you enqueue a queued task or call `agent.run(...)`, you can pass `state=` and `metadata=` to seed that context.

## `ExecutionContext`

`ExecutionContext` is runtime-owned metadata for the currently executing task or direct run.

Common fields:

```python
execution_ctx.task_id
execution_ctx.owner_id
execution_ctx.retry_count
execution_ctx.usage
execution_ctx.last_turn
```

It also exposes the distributed runtime namespaces used by higher-level APIs:

```python
execution_ctx.subagents
execution_ctx.hooks
execution_ctx.messaging
execution_ctx.inbox
execution_ctx.signals
```

Unlike `AgentContext`, `ExecutionContext` is not persisted as part of the conversation state.

## Injection Rules

You do not need to construct either context manually. Factorial injects them when your callable declares the relevant parameters.

```python
from factorial import ExecutionContext, tool


@tool
def choose_strategy(query: str, agent_ctx, execution_ctx: ExecutionContext) -> str:
    if execution_ctx.retry_count > 0:
        return "fallback"
    if len(agent_ctx.messages) > 10:
        return "compress_context"
    return "normal"
```

The same injection pattern works in:

- tools
- `prepare_turn`
- verifiers
- custom `run_turn(...)` implementations

## `ExecutionContext.current()`

If you are already inside an active run and want the runtime context imperatively, use:

```python
from factorial import ExecutionContext


execution_ctx = ExecutionContext.current()
print(execution_ctx.task_id)
```

The top-level runtime namespaces such as `messaging`, `inbox`, and `signals` use `ExecutionContext.current()` internally, which is why they work without you passing `execution_ctx` around manually.

## Example: state + runtime APIs together

```python
from dataclasses import dataclass

from factorial import Agent, inbox, messaging, tool


@dataclass
class ReviewState:
    reviewer: str
    review_count: int = 0


@tool
async def acknowledge_latest(agent_ctx, execution_ctx) -> str:
    page = await inbox.direct.peek(unread_only=True, limit=1)
    if not page.messages:
        return "No unread messages."

    latest = page.messages[0]
    await latest.mark_read(notify_sender=True, data={"ack": True})
    agent_ctx.state.review_count += 1

    await messaging.send(
        latest.from_task_id,
        f"Acknowledged by {agent_ctx.state.reviewer}.",
    )
    return f"Processed message in task {execution_ctx.task_id}"


agent = Agent[ReviewState](
    instructions="Review incoming requests and acknowledge them.",
    tools=[acknowledge_latest],
)
```
