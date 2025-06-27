# nFactorial

**Build distributed agents that spawn more distributed sub-agents.**

nFactorial is a distributed task queue for building reliable multi-agent systems. It makes the following trivial to implement:

* **Agent Reliability**: Automatic retries, backoff strategies, and recovery of dropped tasks from crashed workers.
* **In-flight Task Management**: Cancel, steer, and monitor running tasks. 
* **Spawning Sub Agents**: Having an agent spawn multiple sub agents and wait for their completion before continuing.
* **Deferred Tools**: Pause the agent while it waits for long running tools to complete externally or wait for user approval before continuing.
* **Observability**: Built-in metrics dashboard and comprehensive logging

![Dashboard](https://raw.githubusercontent.com/ricardo-agz/nfactorial/main/docs/static/img/dashboard.png)


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


## Usage Examples:

### IDE Coding Agent

`/docs/examples/code_agent`

![Dashboard](https://raw.githubusercontent.com/ricardo-agz/nfactorial/main/docs/static/img/code-agent.png)

### Multi-Agent

`/docs/examples/multi_agent`

![Dashboard](https://raw.githubusercontent.com/ricardo-agz/nfactorial/main/docs/static/img/multi-agent.png)

### Deliberations.ai
[Check it out](https://www.deliberations.ai/)

![Dashboard](https://raw.githubusercontent.com/ricardo-agz/nfactorial/main/docs/static/img/deliberations-demo-small.gif)