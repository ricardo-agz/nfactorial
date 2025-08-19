# Context

Agents are stateless, Context is used to track the state of execution across turns.

## AgentContext

The default base context class for agents:

```python
from factorial import AgentContext

context = AgentContext(
    query="Tell me a joke",
    messages=[],  # Conversation history
    turn=0,       # Current turn number
)
```

## Custom Agent Context

Create a custom agent context classes for specialized agents:

```python
from typing import Any
from factorial import AgentContext

class DualAgentContext(AgentContext):
    agent_a_messages: list[dict[str, Any]] = []
    agent_b_messages: list[dict[str, Any]] = []
```

## Using Custom Agent Context

```python
from factorial import BaseAgent

class ABTestableAgent(BaseAgent[DualAgentContext]): # For the type checker
    def __init__(self):
        super().__init__(
            description="Executes two agents side by side",
            instructions="You are a helpful assistant",
            tools=tools,
            context_class=DualAgentContext,  # For task serialization
        )

agent = ABTestableAgent()

# Create tasks with custom context
context = DualAgentContext(
    query="Research AI developments",
)
task = await orchestrator.enqueue_task(agent=agent, owner_id="user123", payload=context)
```

## Execution Context

The `ExecutionContext` is a per-request context that tracks task-level information such as task ID, owner ID, retries, and pickups during agent execution. 
Unlike `AgentContext`, it is not stored with the agent and is automatically managed by the framework.

```python
from factorial import AgentContext, ExecutionContext

class MyAgent(Agent):
    def run_turn(self, agent_ctx: AgentContext)
        execution_ctx = self.get_execution_context()
        # or
        execution_ctx = ExecutionContext.current()

        print(f"Task ID: {execution_ctx.task_id}")
        print(f"Owner ID: {execution_ctx.owner_id}")
        print(f"Retries: {execution_ctx.retries}")
        print(f"Iterations: {execution_ctx.iterations}")
```


## Using Context in Tools

The agent automatically injects the agent and execution context to tools that require them
as arguments. 

```python
def stateless_tool(input_args: str) -> str:
    ...

def stateful_tool(input_args: str, agent_ctx: AgentContext) -> str:
    if len(agent_ctx.messages) > 10:
        return run_tool_b(input_args)
    return run_tool_a(input_args)

def tool_with_fallbacks(input_args: str, agent_ctx: AgentContext, execution_ctx: ExecutionContext) -> str:
    if execution_context.retries > 0:
        return run_tool_b(input_args)

    return run_tool_a(input_args)
```
