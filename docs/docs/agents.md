# Agents

Agents are LLMs equipped with instructions and tools. They form the core building blocks of Factorial applications.

## Creating an Agent

```python
from factorial import Agent, AgentContext

agent = Agent(
    description="Research Assistant",
    instructions="You are a helpful research assistant that can search and analyze information.",
    tools=tools,
    tool_actions=tool_actions,
)
```

## Agent Parameters

- **description**: A brief description of what the agent does
- **instructions**: System prompt that guides the agent's behavior
- **tools**: List of tool schemas the agent can use
- **tool_actions**: Dictionary mapping tool names to Python functions
- **model**: LLM model to use (defaults to GPT-4)
- **model_settings**: Configuration for model parameters
- **max_turns**: Maximum conversation turns before completion
- **output_type**: Pydantic model for structured output

## Tools and Actions

Tools define what actions your agent can take. Each tool has a schema and a corresponding Python function:

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
                    "query": {"type": "string"},
                    "max_results": {"type": "integer", "default": 5},
                },
                "required": ["query"],
            },
        },
    }
]

def search_web(query: str, max_results: int = 5) -> str:
    # Your search implementation
    return f"Search results for: {query}"

tool_actions = {
    "search_web": search_web,
}
```

## Model Settings

Customize model behavior with `ModelSettings`:

```python
from factorial import ModelSettings, gpt_4o

agent = Agent(
    # ... other parameters
    model=gpt_4o,
    model_settings=ModelSettings(
        temperature=0.7,
        max_completion_tokens=1000,
        tool_choice="auto",
    ),
)
```

## Structured Output

Define structured output using Pydantic models:

```python
from factorial.utils import BaseModel

class ResearchReport(BaseModel):
    title: str
    summary: str
    key_findings: list[str]
    sources: list[str]

agent = Agent(
    # ... other parameters
    output_type=ResearchReport,
)
```

## Context and Memory

Agents maintain conversation state through `AgentContext`:

```python
from factorial import AgentContext

# Create initial context
context = AgentContext(
    query="Research the latest developments in AI",
    messages=[],  # Previous conversation history
    turn=0,       # Current turn number
)

# Create and submit task
task = agent.create_task(owner_id="user123", payload=context)
```

## Custom Agent Classes

For advanced use cases, extend `BaseAgent`:

```python
from factorial import BaseAgent, AgentContext

class CustomAgent(BaseAgent[AgentContext]):
    async def completion(self, agent_ctx, messages, **kwargs):
        # Custom completion logic
        return await super().completion(agent_ctx, messages, **kwargs)
    
    async def tool_action(self, tool_call, agent_ctx):
        # Custom tool execution logic
        return await super().tool_action(tool_call, agent_ctx)
```

## Error Handling

Agents include built-in retry logic and error handling:

```python
from factorial import retry

class MyAgent(BaseAgent[AgentContext]):
    @retry(max_attempts=3, delay=1.0)
    async def custom_method(self):
        # This method will retry up to 3 times on failure
        pass
``` 