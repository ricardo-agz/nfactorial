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

Any serializable value works (`str`, `dict`, `list`, etc.). Both the model and the client see the full value.

```python
@tool
def get_time() -> str:
    return "2026-02-15T12:00:00Z"


@tool
def execute_code(code: str) -> dict:
    return {"exit_code": 0, "stdout": "Hello, world!"}
```

### Typed returns with `Hidden` (model/client split)

Use a Pydantic `BaseModel` with `Annotated[T, Hidden]` when you want some fields to be excluded from the model's context while still being sent to the client/events.

```python
from typing import Annotated
from pydantic import BaseModel
from factorial import Hidden, tool


class EditResult(BaseModel):
    summary: str                          # model and client see this
    new_code: Annotated[str, Hidden]      # only client sees this
    lines_changed: Annotated[int, Hidden] # only client sees this


@tool
def edit_code(file_path: str, changes: str) -> EditResult:
    """Edit code in a file."""
    # ... perform edit ...
    return EditResult(
        summary=f"Edited {file_path}: applied changes",
        new_code=updated_code,
        lines_changed=42,
    )
```

**What the model sees:**
```json
{"summary": "Edited main.py: applied changes"}
```

**What the client/events see:**
```json
{"summary": "Edited main.py: applied changes", "new_code": "...", "lines_changed": 42}
```

### Failures

Raise exceptions for errors. The framework captures them and surfaces the error message to the model and the client.

```python
@tool
def fetch_user(user_id: str) -> dict:
    user = database.get_user(user_id)
    if user is None:
        raise ValueError(f"User {user_id} not found")
    return {"name": user["name"], "email": user["email"]}
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

Use `wait` for time-based and job-based blocking behavior. The optional `data=` parameter follows the same serialization rules as tool returns.

```python
from factorial import WaitInstruction, subagents, tool, wait


@tool
def retry_later() -> WaitInstruction:
    return wait.sleep(30, data="Retrying shortly")


@tool
def refresh_hourly() -> WaitInstruction:
    return wait.cron(
        "0 * * * *",
        timezone="UTC",
        data="Waiting for the next hourly refresh",
    )


@tool
async def run_research(queries: list[str]) -> WaitInstruction:
    payloads = [{"query": q} for q in queries]
    jobs = await subagents.spawn(agent=research_agent, inputs=payloads, key="research")
    return wait.jobs(jobs, data="Waiting for research jobs")
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

Raise exceptions to signal tool failures. The framework catches them and surfaces the error to the model and in run events.

```python
@tool
def dangerous_operation(path: str) -> str:
    if not is_safe(path):
        raise ValueError(f"Unsafe path: {path}")
    return "Operation completed"
```

For unrecoverable errors that should immediately fail the task, use `FatalAgentError`:

```python
from factorial.exceptions import FatalAgentError

@tool
def critical_operation() -> str:
    if not has_credentials():
        raise FatalAgentError("Missing required credentials")
    return "Done"
```
