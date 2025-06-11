# nFactorial

Factorial is a Python framework for reliably running high-concurrency agents asynchronously. 
It's designed for production workloads where you need to process thousands of agent tasks concurrently with built-in retries, monitoring, and distributed execution.

![Dashboard](docs/static/img/dashboard.png)

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
from factorial import Agent, Orchestrator, gpt_41

# Define tools
tools = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get the current weather",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {"type": "string"},
                },
                "required": ["location"],
            },
        },
    }
]

# Define tool actions
def get_weather(location: str) -> str:
    return f"The weather in {location} is sunny and 72°F"

# Create agent
agent = Agent(
    instructions="You help users get weather information.",
    model=gpt_41,
    tools=tools,
    tool_actions={"get_weather": get_weather},
)

# Create orchestrator
orchestrator = Orchestrator()
orchestrator.register_runner(
    agent=agent,
    agent_worker_config=AgentWorkerConfig(workers=30),
)

# Run the system
if __name__ == "__main__":
    orchestrator.run()
```
