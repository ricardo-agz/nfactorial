# nFactorial

**Build agents that run directly or on a distributed runtime.**

nFactorial is an agent framework with a Redis-backed orchestrator for reliable multi-agent systems. It makes the following straightforward to implement:

* **Direct and queued execution**: Run agents locally with `agent.run(...)` or on a distributed Redis-backed runtime.
* **Agent reliability**: Automatic retries, backoff strategies, and recovery of dropped tasks from crashed workers.
* **In-flight task management**: Cancel, steer, wake, branch, and monitor running tasks.
* **Subagents and waits**: Spawn child tasks and pause on sleeps, activity, signals, jobs, or approval hooks.
* **Runtime coordination**: Exchange direct or group messages, read inboxes, and process receipts.
* **Observability**: Built-in metrics dashboard and comprehensive logging.

![Dashboard](https://raw.githubusercontent.com/ricardo-agz/nfactorial/main/docs/static/img/dashboard.png)

## Installation

```bash
pip install nfactorial
```

## Quick Start

```python
import asyncio

from factorial import Agent, gpt_41


def get_weather(location: str) -> str:
    return f"The weather in {location} is sunny and 72°F"


agent = Agent(
    instructions="You help users get weather information.",
    model=gpt_41,
    tools=[get_weather],
)


async def main() -> None:
    result = await agent.run("What's the weather in San Francisco?")
    print(result.output)


asyncio.run(main())

```

When you need waits, hooks, subagents, messaging, retries, or distributed workers, register the agent with an `Orchestrator` and enqueue tasks instead.

## Usage Examples

### IDE Coding Agent

`docs/docs/examples/code_agent.md`

![Dashboard](https://raw.githubusercontent.com/ricardo-agz/nfactorial/main/docs/static/img/code-agent.png)

### Multi-Agent

`docs/docs/examples/multi_agent.md`

![Dashboard](https://raw.githubusercontent.com/ricardo-agz/nfactorial/main/docs/static/img/multi-agent-progress.png)

### Group Chat Agent

`docs/docs/examples/group_chat_agent.md`

### Mafia Game Agent

`examples/mafia_game_agent`

### Deliberations.ai

[Check it out](https://www.deliberations.ai/)

![Dashboard](https://raw.githubusercontent.com/ricardo-agz/nfactorial/main/docs/static/img/deliberations-demo-small.gif)
