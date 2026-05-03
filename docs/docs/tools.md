# Tools

Tools allow agents to take actions in the world. Each tool is a Python function that the agent can call during execution.

## Defining Tools

The simplest way to create a tool is by just writing a python function with typed args and a docstring

```python
from factorial import function_tool, AgentContext

def get_weather(location: str) -> str:
    """Get the current weather for a location"""
    # Your implementation here
    return f"The weather in {location} is sunny and 72°F"

def search_web(query: str, max_results: int = 5) -> str:
    """Search the web for information"""
    # Your implementation here
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
@function_tool
def get_time() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
```

### Tuple Output (Display, Data)
The first value will be shown as-is to the LLM, the second value will be what is published
with the tool comletion progress. 
This is useful when you want to render the results of the tool call in a UI

```python
@function_tool
def fetch_user(user_id: str) -> tuple[str, dict[str, Any]]:
    user = database.get_user(user_id)
    display = f"User: {user['name']} ({user['email']})"
    return display, user
```

## Custom Tool Configuration

You can customize tool names and descriptions:

```python
@function_tool(
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

@function_tool(is_enabled=is_premium_user)
def premium_feature(query: str) -> str:
    return "Premium feature result"
```

## Error Handling

You may choose to either return the error as a string, which will be shown to the LLM, or simply raise an exception, which will also be formatted
and shown to the LLM. 


## Deferred Tools

Deferred tools are designed for long-running operations that complete externally, leaving the agent in a pending state until the tool result is provided. This is useful for operations that take significant time to complete (like external API calls, user input, or webhook-based workflows).

### How Deferred Tools Work

When a deferred tool is called:

1. **Tool Execution**: The tool function executes normally but is marked as having a pending result
2. **Task State**: The task enters a `PENDING_TOOL_RESULTS` state and is removed from active processing and into an idle queue
3. **External Completion**: An external system must call `complete_deferred_tool()` to provide the result
4. **Task Resumption**: Once all pending tool calls are completed, the task resumes processing

### Defining Deferred Tools

Use the `@deferred_result` decorator with a timeout value:

```python
from factorial import deferred_result, ExecutionContext

@deferred_result(timeout=300.0)  # 5 minute timeout
async def send_email_for_confirmation(
    recipient: str, 
    subject: str, 
    body: str,
    execution_ctx: ExecutionContext,
) -> str:
    """Send an email and wait for user confirmation before proceeding"""
    user = await get_user(id=execution_ctx.owner_id)

    # notify the user that an email needs confirmation to be sent
    await send_email_to_user_for_confirmation(user, recipient, subject, body)
```

### Completing Deferred Tools

External systems complete deferred tools using the `complete_deferred_tool()` function:

```python
success = await orchestrator.complete_deferred_tool(
    task_id="task_123",
    tool_call_id="call_abc123",
    result="User confirmed the email was received successfully"
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
