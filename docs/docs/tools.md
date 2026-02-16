# Tools

Tools allow agents to take actions in the world. Each tool is a Python function that the agent can call during execution.

## Defining Tools

The simplest way to create a tool is by just writing a python function with typed args and a docstring

```python
from factorial import tool


@tool
def get_weather(location: str) -> str:
    """Get the current weather for a location."""
    return f"The weather in {location} is sunny and 72F"


@tool(name="web_search", description="Search the web for information")
def search_web(query: str, max_results: int = 5) -> str:
    return f"Found {max_results} results for: {query}"
```

## Using Tools in Agents

```python
from factorial import Agent


agent = Agent(
    description="Weather assistant",
    instructions="You help users get weather information.",
    tools=[get_weather, search_web],
)
```

## Tool Return Values

### Plain values

Any serializable value works (`str`, `dict`, etc.).

```python
@tool
def get_time() -> str:
    return "2026-02-15T12:00:00Z"
```

### Structured status with `ToolResult`

Use `tool.ok/fail/error` when you want explicit status semantics.

```python
from factorial import tool, ToolResult


@tool
def fetch_user(user_id: str) -> ToolResult:
    user = database.get_user(user_id)
    if user is None:
        return tool.fail("User not found", {"user_id": user_id})
        
    return tool.ok(
        f"Loaded user {user['name']}",
        {"user": user},
    )
```

## Conditional Tools

```python
def is_premium_user(agent_ctx: CustomAgentContext) -> bool:
    return agent_ctx.user_tier == "premium"


@tool(is_enabled=is_premium_user)
def premium_feature(query: str) -> str:
    return "Premium feature result"
```

## Blocking Waits Inside Tools

Use `wait` for time-based and job-based blocking behavior.

```python
from factorial import WaitInstruction, subagents, tool, wait


@tool
def retry_later() -> WaitInstruction:
    return wait.sleep(30, message="Retrying shortly")


@tool
def refresh_hourly() -> WaitInstruction:
    return wait.cron(
        "0 * * * *",
        timezone="UTC",
        message="Waiting for the next hourly refresh",
    )


@tool
async def run_research(queries: list[str]) -> WaitInstruction:
    payloads = [{"query": q} for q in queries]
    jobs = await subagents.spawn(agent=research_agent, inputs=payloads, key="research")
    return wait.jobs(jobs, message="Waiting for research jobs")
```

## External Approval and Callbacks (Hooks)

Use hooks instead of deferred tool decorators.

```python
from typing import Annotated

from factorial import Hook, HookRequestContext, PendingHook, hook, tool


class ExecutionApproval(Hook):
    approved: bool


def request_execution_approval(
    ctx: HookRequestContext,
) -> PendingHook[ExecutionApproval]:
    return ExecutionApproval.pending(
        ctx=ctx,
        title="Approve execution",
        timeout_s=300,
    )


@tool
async def execute_after_approval(
    approval: Annotated[
        ExecutionApproval,
        hook.requires(request_execution_approval),
    ],
) -> str:
    if not approval.approved:
        return "Execution rejected."
    return "Execution approved."
```

Resolve pending hooks from your API/server:

```python
resolution = await orchestrator.resolve_hook(
    hook_id=hook_id,
    payload={"approved": True},
    token=token,
    idempotency_key="approve:123",  # optional but recommended
)
```

## Error Handling

You can either:

- return `tool.error(...)` / `tool.fail(...)`, or
- raise an exception (which is captured and surfaced in run events).
