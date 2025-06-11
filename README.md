# nFactorial

Factorial is a Python framework for reliably running high-concurrency agents asynchronously. 
It's designed for production workloads where you need to process thousands of agent tasks concurrently with built-in retries, monitoring, and distributed execution.

![Dashboard](https://raw.githubusercontent.com/ricardo-agz/nfactorial/main/docs/static/img/dashboard.png)

## Features

* **Distributed execution**: Run agents across multiple workers and machines with Redis-based coordination
* **Fault tolerance**: Automatic retries, backoff strategies, and recovery of dropped tasks from crashed workers
* **Real-time events**: Stream progress updates and results via WebSocket or Redis pub/sub
* **In-flight agent task management**: Cancel, steer, and monitor running tasks
* **Observability**: Built-in metrics dashboard and comprehensive logging
* **Deferred tools**: Support for long-running operations that complete outside the agent execution

## Installation

```bash
pip install nfactorial
```

## Quick Start

```python
from factorial import Agent, Orchestrator, AgentWorkerConfig, gpt_41


def get_weather(location: str) -> str:
    return f"The weather in {location} is sunny and 72°F"


agent = Agent(
    instructions="You help users get weather information.",
    model=gpt_41,
    tools=[get_weather],
)

# Create orchestrator
orchestrator = Orchestrator(
    redis_host="localhost",
    redis_port=6379,
    redis_db=0,
    redis_max_connections=50,
)
orchestrator.register_runner(
    agent=agent, agent_worker_config=AgentWorkerConfig(workers=1)
)

# Run the system
if __name__ == "__main__":
    orchestrator.run()

```
