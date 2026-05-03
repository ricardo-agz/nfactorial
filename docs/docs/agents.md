# Agents

Agents are the core unit of execution in Factorial. An `Agent` combines:

- instructions
- a model
- optional tools
- optional per-turn hooks like `prepare_turn`
- optional completion control like `stop_when` and `verifier`

## Smallest Useful Agent

```python
from factorial import Agent, gpt_41


def get_weather(location: str) -> str:
    return f"The weather in {location} is sunny and 72°F"


agent = Agent(
    name="weather_agent",
    description="Weather assistant",
    instructions="You answer weather questions clearly and briefly.",
    model=gpt_41,
    tools=[get_weather],
)
```

## Execution Modes

### Direct execution

Use direct execution for synchronous, in-process runs:

```python
result = await agent.run("What's the weather in San Francisco?")
print(result.output)
print(result.turn_count)
```

`agent.run(...)` returns a `RunResult` with:

- `output`
- `messages`
- `state`
- `metadata`
- `usage`
- `turn_count`
- `verification`

You can also stream typed lifecycle events:

```python
from factorial import FinishEvent


async for event in agent.stream("Summarize nfactorial in one sentence."):
    print(type(event).__name__)
    if isinstance(event, FinishEvent):
        print(event.output)
```

### Queued execution

Use the orchestrator when your agent needs any of the distributed runtime features:

- waits such as `wait.sleep(...)`, `wait.activity(...)`, `wait.until_signal(...)`, or `wait.jobs(...)`
- hook-based approvals
- subagents
- runtime messaging, inboxes, or signals
- retries, backoff, steering, wake, or cancellation

```python
task = await orchestrator.enqueue(
    agent,
    input="What's the weather in San Francisco?",
    owner_id="user123",
)

result = await task.wait()
print(result.output)
```

Direct `agent.run(...)` and `agent.stream(...)` do **not** support pending tool results or child-task continuations, so anything that parks and resumes should use the orchestrator.

## Common Agent Parameters

- `name`: Optional stable identifier. Useful for observability, control-plane APIs, and examples that register multiple agents.
- `description`: Optional human-readable summary used in logs and dashboards.
- `instructions`: System prompt for the agent.
- `model`: A concrete model or a callable that selects a model per turn.
- `tools`: Python callables the model can invoke. See [Tools](tools.md).
- `temperature`: Sampling temperature.
- `tool_choice`: `"auto"`, `"required"`, `"none"`, or an explicit tool selection object.
- `prepare_turn`: Optional hook that can rewrite the next turn before the model call.
- `stop_when`: Stop policy for the loop. Common choices are `tool_called("done")`, `no_tool_calls()`, and `turn_count_is(...)`.
- `verifier`: Optional sync or async validator for final output.
- `request_timeout`: Provider request timeout in seconds.
- `parse_tool_args`: Whether JSON tool arguments should be parsed automatically.

You can also choose models dynamically:

```python
from factorial import Agent, gpt_41, gpt_41_mini


def choose_model(agent_ctx):
    return gpt_41 if agent_ctx.turn_number == 1 else gpt_41_mini


agent = Agent(
    instructions="Think hard on the first turn, then be cheap.",
    model=choose_model,
)
```

## `prepare_turn`

Use `prepare_turn` to tune each turn without mutating the stored transcript:

```python
from factorial import Agent, any_of, no_tool_calls, turn_count_is


def my_prepare_turn(turn, agent_ctx):
    if agent_ctx.turn_number == 1:
        turn.tool_choice = {"type": "function", "function": {"name": "plan"}}
    else:
        turn.tool_choice = "required"

    turn.parallel_tool_calls = False
    turn.temperature = 0.1


agent = Agent(
    instructions="Plan first, then execute.",
    model=gpt_41,
    tools=[plan, search],
    prepare_turn=my_prepare_turn,
    stop_when=any_of(no_tool_calls(), turn_count_is(12)),
)
```

`prepare_turn` always receives `turn`. `agent_ctx` and `execution_ctx` are injected only if your function declares them.

## Typed Final Output

For structured final output, define an explicit finish tool and stop when it is called:

```python
from pydantic import BaseModel

from factorial import Agent, tool, tool_called


class Joke(BaseModel):
    setup: str
    punchline: str


@tool
def done(result: Joke) -> Joke:
    return result


agent = Agent(
    instructions="Tell a joke, then call done with the final result.",
    tools=[done],
    stop_when=tool_called("done"),
)
```

## Output Verification

Use `verify.accept()`, `verify.retry()`, or `verify.fail()` in a verifier:

```python
from factorial import verify


def verify_output(output, *, agent_ctx, execution_ctx):
    if output["score"] < 80:
        return verify.retry(
            message="Need stronger evidence.",
            code="score_low",
            metadata={"score": output["score"]},
        )
    return verify.accept(
        metadata={
            "owner_id": execution_ctx.owner_id,
            "turn_number": agent_ctx.turn_number,
        }
    )
```

If you want a retry cap, enforce it in the verifier with `agent_ctx.verification.attempts_used`.

## Typed State and Metadata

Use `Agent[StateT, MetadataT]` when you want structured state or metadata:

```python
from dataclasses import dataclass

from factorial import Agent


@dataclass
class ResearchState:
    topic: str
    sources_found: list[str]
    confidence: float = 0.0


research_agent = Agent[ResearchState](
    name="research_agent",
    instructions="Research carefully and track your findings in state.",
)

task = await orchestrator.enqueue(
    research_agent,
    input="Research the impact of AI on education.",
    owner_id="researcher123",
    state=ResearchState(topic="AI in education", sources_found=[]),
)
```

Inside tools, verifiers, and `prepare_turn`, access state through `agent_ctx.state` and metadata through `agent_ctx.metadata`.

## Multimodal Input

Direct runs and queued runs both accept either:

- a plain string
- a normalized message list

Use the helper functions when working with files and images:

```python
from factorial import Agent, file, gpt_41, image, user


agent = Agent(
    instructions="Compare the screenshots and the attached document.",
    model=gpt_41,
)

result = await agent.run(
    [
        user(
            "Compare these inputs.",
            image(path="before.png", detail="high"),
            image(path="after.png", detail="high"),
            file(path="requirements.pdf"),
        )
    ]
)
```

Raw OpenAI-style typed content parts are also accepted, so you can pass `input_text`, `input_image`, and `input_file` payloads directly if that is already what your application produces.

## Transcript Helpers

When you need to seed or test multi-turn transcripts manually, use the transcript helpers:

```python
from factorial import tool_call, tool_calls, tool_result, user


messages = [
    user("Search for release notes."),
    tool_calls(
        tool_call(
            "web_search",
            {"query": "nfactorial release notes"},
            call_id="call_1",
        )
    ),
    tool_result(
        "call_1",
        {"hits": 3},
        tool_name="web_search",
        model_output="Found 3 relevant results.",
    ),
]
```
