---
slug: /
---

# Factorial

**Build distributed agents that spawn other agents**

nFactorial is a distributed task queue for building reliable, high-concurrency multi-agent systems. It makes the following trivial to implement:

* **Agent Reliability**: Automatic retries, backoff strategies, and recovery of dropped tasks from crashed workers.
* **In-flight Task Management**: Cancel, steer, and monitor running tasks. 
* **Spawning Sub Agents**: Having an agent spawn multiple sub agents and wait for their completion before continuing.
* **Deferred Tools**: Pause the agent while it waits for long running tools to complete externally or wait for user approval before continuing.
* **Observability**: Built-in dashboard to visualize agent states, completions, and failures.

## Installation

```bash
pip install nfactorial
```

---



## Why use nFactorial

nFactorial has a 2 core components:

* **Agent (and Tools)**, which is simply a system prompt + python functions that interact with external systems in an LLM -> tool execution loop
* **Orchestrator**, which spins up workers for each agent and orchestrates the task execution and agent states.

In practice, this lets you easily build agents much like you would with another frameworks such as the OpenAI agents SDK, but with the reliability of using something like Celery or Temporal.

**Here are the main features:**

* **Distributed execution**: Run agents across multiple workers with Redis powering the underlying queue.
* **Fault tolerance**: Automatic retries, backoff strategies, and recovery of dropped tasks from crashed workers.
* **Real-time events**: Stream progress updates and results via WebSocket or Redis pub/sub 
* **Agent-lifecycle hooks**: Easily inject logic to run before/after each turn or run, on completion, failure, or cancellation. 
* **In-flight agent task management**: Cancel or steer (inject in-flight messages) ongoing agent runs.
* **Observability**: Built-in metrics dashboard to visualize active agents and workers and track succesful completions, errors, and other agent states.
* **Hooks and wait orchestration**: Pause execution for external approvals via hooks, schedule time-based waits, and block on spawned subagent jobs.

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


## Next Steps

- [**Quickstart**](./quickstart): Get up and running in 5 minutes
- [**Agents**](./agents): Learn how to create and configure agents
- [**Orchestrator**](./orchestrator): Set up distributed processing
- [**Tools**](./tools): Build custom tools for your agents
- [**Events**](./events): Real-time monitoring and progress tracking
- [**Examples**](./examples/multi_agent.md): Check out example agents
