# Agents

Agents are the core building blocks of Factorial applications. They are LLMs equipped with instructions, tools, and configuration that can execute tasks autonomously. Each agent runs in a stateless manner, with context managed separately for each task execution.

## Quick Start

The simplest way to create an agent:

```python
from factorial import Agent

def get_weather(location: str) -> str:
    """Get the current weather for a location"""
    return f"The weather in {location} is sunny and 72°F"

agent = Agent(
    instructions="You are a helpful weather assistant. Always be friendly and informative.",
    tools=[get_weather],
)
```

## Agent Parameters

### Required Parameters

**`description`** (str): A brief description of what the agent does. Used for logging and observability.

**`instructions`** (str): The system prompt that guides the agent's behavior. This is sent to the LLM as the system message.

### Optional Parameters

**`tools`** (list): List of Python functions that the agent can call. See [Tools documentation](tools.md) for details.

```python
def search_web(query: str) -> str:
    """Search the web for information"""
    return f"Search results for: {query}"

agent = Agent(
    description="Research Assistant",
    instructions="You help users research topics by searching the web.",
    tools=[search_web],
)
```

**`model`** (Model | Callable): The LLM model to use. Can be a static model or a function that returns a model based on context.

```python
from factorial import gpt_41, gpt_41_mini

# Static model
agent = Agent(
    description="Assistant",
    instructions="You are helpful",
    model=gpt_41,
)

# Dynamic model based on context
def choose_model(agent_ctx) -> Model:
    return gpt_41 if agent_ctx.turn_number == 1 else gpt_41_mini

agent = Agent(
    description="Assistant", 
    instructions="You are helpful",
    model=choose_model,
)
```

**`temperature`** (float): Model temperature (0.0–2.0).

**`tool_choice`** (str | dict): Tool choice for the model (`"auto"`, `"required"`, `"none"`, or a specific tool).

**`prepare_turn`** (Callable): Optional hook that runs before each model call. It always receives `turn`, and `agent_ctx` / `execution_ctx` are injected only if your function declares them.

**`stop_when`** (Callable | StopCondition): When to stop the loop. Default: `any_of(no_tool_calls(), turn_count_is(10))`. Use `turn_count_is(n)` or `tool_called("done")` for custom behavior.

**`verifier`** (Callable): Optional sync/async callable that validates output before completion. Returns `verify.accept()`, `verify.retry(message=...)`, or `verify.fail(message=...)`.
If you want a retry cap, enforce it inside the verifier with `agent_ctx.verification.attempts_used`.

**`request_timeout`** (float): HTTP timeout for LLM requests in seconds (default: 120.0).

**`parse_tool_args`** (bool): Whether to parse tool arguments as JSON (default: True).

## Dynamic Turn Configuration

Use `prepare_turn` to set per-turn `tool_choice`, `temperature`, or other model settings:

```python
from factorial import Agent, any_of, no_tool_calls, turn_count_is

def my_prepare_turn(turn, agent_ctx):
    if agent_ctx.turn_number == 1:
        turn.tool_choice = {"type": "function", "function": {"name": "plan"}}
    else:
        turn.tool_choice = "required"
    turn.temperature = 0.2

agent = Agent(
    instructions="You plan then execute.",
    model=gpt_41,
    tools=[plan, search],
    prepare_turn=my_prepare_turn,
    stop_when=any_of(no_tool_calls(), turn_count_is(12)),
)
```

## Typed Output

For structured output, define a finish tool and use `stop_when=tool_called("done")`:

```python
from pydantic import BaseModel
from factorial import Agent, stop, tool

class Joke(BaseModel):
    setup: str
    punchline: str

@tool
def done(result: Joke) -> Joke:
    return result

agent = Agent(
    instructions="Tell a joke, then call done with the result.",
    tools=[search_web, done],
    stop_when=stop.tool_called("done"),
)
```

## Output Verification

Use `verify.accept()`, `verify.retry()`, or `verify.fail()` in your verifier:

```python
from factorial import Agent, verify

def verify_output(output: AgentOutput, *, agent_ctx):
    if output.score < 80:
        return verify.fail(
            message="Score below acceptance threshold",
            code="score_low",
            metadata={"score": output.score, "minimum": 80},
        )
    return verify.accept()
```

## Enqueueing Tasks

Enqueue tasks through the orchestrator:

```python
task = await orchestrator.enqueue(
    agent,
    input="What's the weather in San Francisco?",
    owner_id="user123",
)
# task is a TaskHandle; use task.snapshot(), task.wait(), task.updates()
```

## Typed State and AgentContext

Use `Agent[StateT, MetadataT]` with a state dataclass for typed state. `AgentContext` exposes `messages`, `state`, and `metadata`:

```python
from dataclasses import dataclass
from factorial import Agent

@dataclass
class ResearchState:
    research_topic: str = ""
    sources_found: list[str] = []
    confidence_level: float = 0.0

research_agent = Agent[ResearchState](
    description="Research specialist",
    instructions="You conduct thorough research",
    tools=[search_web, analyze_source],
)

# Enqueue with initial state
task = await orchestrator.enqueue(
    research_agent,
    input="Research the impact of AI on education",
    owner_id="researcher123",
    state=ResearchState(research_topic="AI in Education"),
)
```

In tools, access state via `agent_ctx.state.*`.

## Agent Lifecycle

Understanding the agent execution lifecycle:

1. **Enqueue**: Call `orchestrator.enqueue(agent, input=..., owner_id=..., state=...)` to create a task
2. **Task Processing**: Task is added to the processing queue
3. **Turn Execution**: Agent processes one turn at a time
   - Prepare messages (system prompt + conversation history)
   - Make LLM completion request
   - Execute any tool calls
   - Update context with results
4. **Completion Check**: Determine if agent should finish
5. **Context Update**: Save updated context
6. **Next Turn**: Repeat until completion or max turns reached
