# Tools

Tools allow agents to take actions in the world. Each tool is a Python function that the agent can call during execution.

## Defining Tools

The simplest way to create a tool is by just writing a python function with typed args and a docstring

```python
from factorial import function_tool, AgentContext

def get_weather(location: str) -> str:              # Note: the function args and their type hints define the schema for the tool sent to the LLM
    """Get the current weather for a location"""    # Note: the docstring is the tool description sent to the LLM
    # Your implementation here...
    return f"The weather in {location} is sunny and 72°F"

def search_web(query: str, max_results: int = 5) -> str:
    """Search the web for information"""
    # Your implementation here...
    return f"Found {max_results} results..."
```

## Using Tools in Agents

Pass tools to your agent during initialization:

```python
from factorial import Agent, AgentContext

agent = Agent(
    description="Weather assistant",
    instructions="You help users get weather information",
    tools=[get_weather, search_web],
)
```

## Return Values

Tools can return different types of values:

### Single Output
The output will be shown as-is to the LLM

```python
def get_time() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
```

### Tuple Output (Display, Data)
The first value will be shown as-is to the LLM, the second value will be what is published
with the tool comletion progress. 
This is useful when you want to render the results of the tool call in a UI

```python
def fetch_user(user_id: str) -> tuple[str, dict[str, Any]]:
    """Fetches the data for a user from an ID"""
    user = database.get_user(user_id)
    display = f"User: {user['name']} ({user['email']})"
    return display, user
```

## Custom Tool Configuration

You can customize tool names and descriptions:

```python
from factorial import tool

@tool(
    name="weather_lookup",
    description="Get detailed weather information for any city"
)
def get_weather(location: str) -> str:
    return f"Weather in {location}: sunny, 72°F"
```

If no description is provided, the function's docstring is used.

## Conditional Tools

Tools can be enabled or disabled based on context:

```python
def is_premium_user(agent_ctx: CustomAgentContext) -> bool:
    return agent_ctx.user_tier == "premium"

@tool(
    is_enabled=is_premium_user,
    description="..."
)
def premium_feature(query: str) -> str:
    return "Premium feature result"
```

## Error Handling

You may choose to either return the error as a string, which will be shown to the LLM, or simply raise an exception, which will also be formatted
and shown to the LLM. 


## Deferred Tools

Deferred tools are designed for results that are completed externally, leaving the agent in a pending state until a tool result is provided. For example, this is useful with operations that complete via a webhook, or tools that require a user input or approval before executing. 


### How Deferred Tools Work

When a deferred tool is called:

1. **Tool Execution**: The tool function is executed, this could be anything from enqueing a task externally or firing a request that is waiting for a response via a webhook.
2. **Task State**: The agent task enters a `PENDING_TOOL_RESULTS` state and is removed from active processing and into an idle queue.
3. **External Completion**: An external system must call `complete_deferred_tool()` to provide the result.
4. **Task Resumption**: Once all pending tool calls are completed, the task is put back in the main queue and the agent resumes execution.

### Defining Deferred Tools

Use the `@deferred_tool` decorator with a timeout value:

```python
from factorial import deferred_tool, ExecutionContext

@deferred_tool(timeout=60 * 60)  # 60 minute timeout
async def request_fund_transfer(
    amount_cents: int,
    destination_account: str,
    execution_ctx: ExecutionContext,
) -> None:
    """Request user approval to transfer funds."""

    approve_link = (
        "https://api.myapp.com/approve-transfer"
        f"?task_id={execution_ctx.task_id}&tool_call_id={execution_ctx.current_tool_call_id}"
    )
    reject_link = (
        "https://api.myapp.com/reject-transfer"
        f"?task_id={execution_ctx.task_id}&tool_call_id={execution_ctx.current_tool_call_id}"
    )

    subject = "Action required: Approve funds transfer"
    body = (
        f"Approve transfer of ${amount_cents/100:.2f} to {destination_account}.\n"
        f"Approve: {approve_link}\n"
        f"Reject: {reject_link}"
    )

    await send_email_to_user_for_confirmation(
        user_id=execution_ctx.owner_id,
        subject=subject,
        body=body,
    )
```

### Completing Deferred Tools

External systems complete deferred tools using the `complete_deferred_tool()` function: 

```python
@app.get("/approve-transfer")
async def approve_transfer(task_id: str, tool_call_id: str):
    await orchestrator.complete_deferred_tool(
        task_id=task_id,
        tool_call_id=tool_call_id,
        result={"status": "approved"},
    )

@app.get("/reject-transfer")
async def reject_transfer(task_id: str, tool_call_id: str):
    await orchestrator.complete_deferred_tool(
        task_id=task_id,
        tool_call_id=tool_call_id,
        result={"status": "rejected"},
    )
```

### Task Lifecycle with Deferred Tools

1. **Normal Execution**: Agent processes the task normally
2. **Deferred Tool Call**: When a deferred tool is called:
   - Tool function executes and returns immediately
   - Task status changes to `PENDING_TOOL_RESULTS`
   - Tool call IDs are stored with a `<|PENDING|>` sentinel value in Redis
3. **External Processing**: Your external system processes the long-running operation
4. **Result Completion**: Call `complete_deferred_tool()` with the final result
5. **Task Resumption**: Once all pending tools are completed:
   - Results are added to the conversation as tool responses
   - Task status changes back to `ACTIVE`
   - Task is returned to the processing queue
