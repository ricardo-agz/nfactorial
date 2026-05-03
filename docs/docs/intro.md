---
slug: /
---

# Factorial

**Build agents that run directly or on a distributed runtime**

Factorial is an agent framework with a Redis-backed orchestrator for reliable, high-concurrency workflows. It supports:

- direct `agent.run(...)` and `agent.stream(...)` for local synchronous flows
- queued execution with retries, backoff, recovery, and observability
- waits, hooks, subagents, messaging, inboxes, and signals
- in-flight task management such as steer, cancel, wake, and branch
- built-in dashboards and streaming runtime events

## Installation

```bash
pip install nfactorial
```

---



## Why use nFactorial

nFactorial has two core components:

- **Agent (and tools)**: the model loop, state, prompts, tools, verifiers, and direct execution APIs
- **Orchestrator**: the queued runtime for distributed execution, waits, subagents, hooks, messaging, and control-plane operations

In practice, this gives you a small local agent API when you want one, and a more powerful distributed runtime when the workflow needs to park, resume, coordinate, or scale.

**Here are the main features:**

- **Direct runs**: execute an agent locally with `await agent.run(...)` or stream typed events with `agent.stream(...)`.
- **Distributed execution**: run agents across workers with Redis as the source of truth.
- **Fault tolerance**: automatic retries, backoff, and dropped-task recovery.
- **Real-time events**: stream progress and lifecycle updates for direct and queued runs.
- **Task control**: steer, cancel, wake, branch, and resume tasks in flight or after completion.
- **Runtime coordination**: use hooks, waits, signals, subagents, messaging, and inboxes.
- **Observability**: inspect dashboards, metrics, traces, and runtime state.

## Quick Example

```python
import asyncio

from factorial import Agent, gpt_41


def get_weather(city: str) -> str:
    """Get weather for a city"""
    return f"The weather in {city} is sunny and 72°F"

weather_agent = Agent(
    description="Weather Assistant",
    instructions="You help users get weather information.",
    model=gpt_41,
    tools=[get_weather],
)


async def main() -> None:
    result = await weather_agent.run("What's the weather in San Francisco?")
    print(result.output)


asyncio.run(main())
```

When the workflow needs waits, hooks, subagents, or human control-plane actions, register the agent with an `Orchestrator` and enqueue tasks instead.

## Next Steps

- [**Quickstart**](./quickstart): Get up and running in 5 minutes
- [**Agents**](./agents): Learn how to create and configure agents
- [**Orchestrator**](./orchestrator): Set up distributed processing
- [**Messaging**](./messaging): Coordinate tasks with groups, inboxes, and receipts
- [**Signals**](./signals): Park tasks until named events arrive
- [**Tools**](./tools): Build custom tools for your agents
- [**Events**](./events): Real-time monitoring and progress tracking
- [**Examples**](./examples/multi_agent.md): Check out example agents
