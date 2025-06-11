---
slug: /
---

# Factorial

Factorial is a Python framework for reliably running high-concurrency agents asynchronously. It's designed for production workloads where you need to process thousands of agent tasks concurrently with built-in retries, monitoring, and distributed execution.

Factorial has a small set of core primitives:

* **Agents**, which are simply a system prompt + tools in an LLM -> tool execution loop
* **Orchestrator**, which coordinates task distribution and manages worker processes  
* **Tools**, which are Python functions that agents can call to interact with external systems

In combination with Redis for coordination, these primitives let you build scalable agent systems that handle real-world production loads without complex infrastructure setup.

## Why use Factorial

The framework has a few driving design principles:

1. **Works great out of the box** with sensible defaults and minimal configuration
2. **Highly robust and scalable** with built-in fault tolerance and distributed execution
3. **Highly configurable and overridable** with no hidden under-the-hood prompt bloat

Here are the main features:

* **Distributed execution**: Run agents across multiple workers and machines with Redis-based coordination  
* **Fault tolerance**: Automatic retries, backoff strategies, and recovery of dropped tasks from crashed workers
* **Real-time events**: Stream progress updates and results via WebSocket or Redis pub/sub 
* **In-flight agent task management**: Cancel, steer, and monitor running tasks
* **Observability**: Built-in metrics dashboard and comprehensive logging  
* **Deferred tools**: Support for long-running operations that complete outside the agent execution

## Quick Example

```python
from factorial import Agent, AgentContext, Orchestrator, AgentWorkerConfig


def get_weather(city: str) -> str:
    """Get weather for a city"""
    return f"The weather in {city} is sunny and 72°F"

# Create an agent
weather_agent = Agent(
    description="Weather Assistant",
    instructions="You help users get weather information.",
    tools=[get_weather],
)

# Set up orchestrator
orchestrator = Orchestrator()
orchestrator.register_runner(
    agent=weather_agent,
    agent_worker_config=AgentWorkerConfig(workers=2, batch_size=10),
)

# Enqueue a task
task = weather_agent.create_task(
    owner_id="user123",
    payload=AgentContext(query="What's the weather in San Francisco?"),
)
await orchestrator.enqueue_task(agent=weather_agent, task=task)

# Subscribe to results
async for update in orchestrator.subscribe_to_updates(owner_id="user123"):
    if update['event_type'] == 'agent_output':
        print(f"Result: {update['data']}")
        break
```

## Installation

```bash
pip install nfactorial
```

## Next Steps

- [**Quickstart**](./quickstart): Get up and running in 5 minutes
- [**Agents**](./agents): Learn how to create and configure agents
- [**Orchestrator**](./orchestrator): Set up distributed processing
- [**Tools**](./tools): Build custom tools for your agents
- [**Events**](./events): Real-time monitoring and progress tracking
