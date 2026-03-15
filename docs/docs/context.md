# Context

Agents are stateless. `AgentContext` tracks execution state for each turn.

## AgentContext

`AgentContext` exposes `messages`, `state`, and `metadata`:

```python
# In tools or prepare_turn, agent_ctx provides:
agent_ctx.messages   # Conversation history (list of message dicts)
agent_ctx.state     # Typed state (from Agent[StateT])
agent_ctx.metadata  # Typed metadata
agent_ctx.turn_number  # Current turn (1-based)
```

For typed state, use `Agent[StateT]` with a dataclass and pass `state=...` to `orchestrator.enqueue()`.

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

Tools can receive `agent_ctx` and `execution_ctx` as arguments:

```python
def stateless_tool(input_args: str) -> str:
    ...

def stateful_tool(input_args: str, agent_ctx) -> str:
    if len(agent_ctx.messages) > 10:
        return run_tool_b(input_args)
    return run_tool_a(input_args)

def tool_with_fallbacks(input_args: str, agent_ctx, execution_ctx) -> str:
    if execution_ctx.retries > 0:
        return run_tool_b(input_args)
    return run_tool_a(input_args)
```

Access typed state via `agent_ctx.state.*` when using `Agent[StateT]`.
