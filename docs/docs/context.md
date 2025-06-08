# Context

Context manages conversation state and agent memory throughout task execution. It tracks messages, turn count, and custom data.

## AgentContext

The base context class for simple agents:

```python
from factorial import AgentContext

context = AgentContext(
    query="What's the weather like?",
    messages=[],  # Conversation history
    turn=0,       # Current turn number
)
```

## Custom Context

Create custom context classes for specialized agents:

```python
from factorial import AgentContext
from typing import List, Dict, Any

class ResearchContext(AgentContext):
    sources: List[str] = []
    findings: Dict[str, Any] = {}
    research_depth: int = 1
    
    def add_source(self, url: str, title: str):
        self.sources.append({"url": url, "title": title})
    
    def add_finding(self, topic: str, data: Any):
        self.findings[topic] = data
```

## Using Custom Context

```python
from factorial import BaseAgent

class ResearchAgent(BaseAgent[ResearchContext]):
    def __init__(self):
        super().__init__(
            description="Research Agent",
            instructions="You are a research assistant.",
            tools=research_tools,
            tool_actions=research_actions,
            context_class=ResearchContext,  # Specify custom context
        )

# Create tasks with custom context
context = ResearchContext(
    query="Research AI developments",
    research_depth=3,
)
task = agent.create_task(owner_id="user123", payload=context)
```

## Message Management

Context automatically manages conversation messages:

```python
# Initial state
context = AgentContext(query="Hello")
print(context.messages)  # []

# After agent processes
# Messages are automatically added:
# [
#   {"role": "user", "content": "Hello"},
#   {"role": "assistant", "content": "Hi there!"},
# ]
```

## Turn Tracking

Track conversation progress:

```python
def my_tool(query: str, agent_ctx: AgentContext) -> str:
    turn = agent_ctx.turn
    if turn == 0:
        return "This is the first turn"
    else:
        return f"This is turn {turn}"
```

## Persistence

Context is automatically serialized and stored:

```python
# Context is saved to Redis during task execution
# and restored when tasks resume

# Manual serialization
context_json = context.to_json()
restored_context = AgentContext.from_json(context_json)
```

## Context in Tools

Access context in tool functions:

```python
def stateful_tool(input_data: str, agent_ctx: AgentContext) -> str:
    # Access conversation history
    previous_messages = agent_ctx.messages
    
    # Check turn number
    if agent_ctx.turn > 5:
        return "We've been talking for a while!"
    
    # Use query for context
    original_query = agent_ctx.query
    return f"Processing '{input_data}' for query: {original_query}"
```

## Memory Patterns

### Simple Memory

Store key-value data:

```python
class MemoryContext(AgentContext):
    memory: Dict[str, Any] = {}
    
    def remember(self, key: str, value: Any):
        self.memory[key] = value
    
    def recall(self, key: str) -> Any:
        return self.memory.get(key)
```

### Conversation Summary

Maintain conversation summaries:

```python
class SummaryContext(AgentContext):
    summary: str = ""
    
    def update_summary(self, new_info: str):
        if self.summary:
            self.summary += f" {new_info}"
        else:
            self.summary = new_info
```

### Task Progress

Track multi-step tasks:

```python
class TaskContext(AgentContext):
    steps_completed: List[str] = []
    current_step: str = ""
    total_steps: int = 0
    
    def complete_step(self, step: str):
        if step not in self.steps_completed:
            self.steps_completed.append(step)
    
    def progress_percentage(self) -> float:
        if self.total_steps == 0:
            return 0.0
        return len(self.steps_completed) / self.total_steps * 100
```

## Context Validation

Add validation to custom contexts:

```python
from pydantic import validator

class ValidatedContext(AgentContext):
    email: str = ""
    age: int = 0
    
    @validator('email')
    def validate_email(cls, v):
        if '@' not in v:
            raise ValueError('Invalid email format')
        return v
    
    @validator('age')
    def validate_age(cls, v):
        if v < 0 or v > 150:
            raise ValueError('Age must be between 0 and 150')
        return v
```

## Context Inheritance

Build hierarchical context structures:

```python
class BaseWorkflowContext(AgentContext):
    workflow_id: str = ""
    status: str = "pending"

class DataProcessingContext(BaseWorkflowContext):
    input_file: str = ""
    output_file: str = ""
    processing_steps: List[str] = []

class MLTrainingContext(BaseWorkflowContext):
    model_type: str = ""
    training_data: str = ""
    hyperparameters: Dict[str, Any] = {}
```

## Best Practices

1. **Keep it simple**: Start with `AgentContext` and add complexity as needed
2. **Type hints**: Use proper type annotations for better IDE support
3. **Validation**: Add validators for critical fields
4. **Serialization**: Ensure custom fields are JSON-serializable
5. **Documentation**: Document custom context fields and methods
6. **Immutability**: Consider making context fields immutable where appropriate
7. **Memory management**: Be mindful of context size for long conversations 