# Tools

Tools allow agents to take actions in the world. Each tool consists of a schema definition and a Python function implementation.

## Defining Tools

Tools are defined using OpenAI's function calling schema:

```python
tools = [
    {
        "type": "function",
        "function": {
            "name": "search_web",
            "description": "Search the web for information",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "max_results": {"type": "integer", "default": 5},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        },
        "strict": True,
    }
]
```

## Tool Actions

Each tool needs a corresponding Python function:

```python
def search_web(query: str, max_results: int = 5) -> str:
    """Search the web and return results."""
    # Your implementation here
    results = perform_search(query, max_results)
    return f"Found {len(results)} results for '{query}'"

# Map tool names to functions
tool_actions = {
    "search_web": search_web,
}
```

## Return Values

Tool functions can return different types of values:

### Simple String

```python
def get_time() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
```

### Tuple (Display, Data)

Return both a display string and structured data:

```python
def fetch_user(user_id: str) -> tuple[str, dict]:
    user = database.get_user(user_id)
    display = f"User: {user['name']} ({user['email']})"
    return display, user
```

### ToolActionResult

For advanced control over tool execution:

```python
from factorial import ToolActionResult

def complex_tool(param: str) -> ToolActionResult:
    result = process_data(param)
    return ToolActionResult(
        output_str=f"Processed: {param}",
        output_data=result,
        pending_result=False,
    )
```

## Accessing Agent Context

Tool functions can access the agent's context:

```python
from factorial import AgentContext

def context_aware_tool(query: str, agent_ctx: AgentContext) -> str:
    # Access conversation history
    previous_messages = agent_ctx.messages
    current_turn = agent_ctx.turn
    
    # Use context in your logic
    return f"Processing query '{query}' on turn {current_turn}"
```

## Async Tools

Tools can be asynchronous:

```python
import asyncio
import httpx

async def fetch_url(url: str) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.text

# Async tools work automatically
tool_actions = {
    "fetch_url": fetch_url,
}
```

## Error Handling

Tools should handle errors gracefully:

```python
def safe_divide(a: float, b: float) -> str:
    try:
        if b == 0:
            return "Error: Cannot divide by zero"
        result = a / b
        return f"{a} ÷ {b} = {result}"
    except Exception as e:
        return f"Error: {str(e)}"
```

## Long-Running Tools

For tools that take a long time to complete:

```python
from factorial import deferred_result

@deferred_result(timeout=300.0)  # 5 minute timeout
async def long_running_task(data: str) -> str:
    # This will run in the background
    await asyncio.sleep(60)  # Simulate long work
    return f"Processed: {data}"
```

## File Operations

Tools for file handling:

```python
import os
from pathlib import Path

def read_file(filepath: str) -> str:
    try:
        path = Path(filepath)
        if not path.exists():
            return f"Error: File {filepath} not found"
        
        content = path.read_text()
        return f"File content ({len(content)} chars):\n{content}"
    except Exception as e:
        return f"Error reading file: {str(e)}"

def write_file(filepath: str, content: str) -> str:
    try:
        path = Path(filepath)
        path.write_text(content)
        return f"Successfully wrote {len(content)} characters to {filepath}"
    except Exception as e:
        return f"Error writing file: {str(e)}"
```

## API Integration

Tools for external API calls:

```python
import httpx
import json

async def call_api(endpoint: str, method: str = "GET", data: dict = None) -> str:
    try:
        async with httpx.AsyncClient() as client:
            if method.upper() == "GET":
                response = await client.get(endpoint)
            elif method.upper() == "POST":
                response = await client.post(endpoint, json=data)
            else:
                return f"Error: Unsupported method {method}"
            
            response.raise_for_status()
            return f"API Response ({response.status_code}):\n{response.text}"
    except Exception as e:
        return f"API Error: {str(e)}"
```

## Database Tools

Tools for database operations:

```python
import sqlite3

def query_database(sql: str) -> str:
    try:
        conn = sqlite3.connect("database.db")
        cursor = conn.cursor()
        cursor.execute(sql)
        
        if sql.strip().upper().startswith("SELECT"):
            results = cursor.fetchall()
            return f"Query returned {len(results)} rows:\n{results}"
        else:
            conn.commit()
            return f"Query executed successfully, {cursor.rowcount} rows affected"
    except Exception as e:
        return f"Database error: {str(e)}"
    finally:
        conn.close()
```

## Best Practices

1. **Clear descriptions**: Write detailed descriptions for better LLM understanding
2. **Input validation**: Validate parameters before processing
3. **Error handling**: Return meaningful error messages
4. **Type hints**: Use proper type annotations
5. **Documentation**: Document complex tools with examples
6. **Security**: Validate and sanitize inputs, especially for file/database operations
7. **Timeouts**: Set appropriate timeouts for long-running operations 