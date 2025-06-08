# Quickstart

Get started with Factorial in minutes.

## Installation

```bash
pip install nfactorial
```

## Set up Redis

Factorial uses Redis for distributed task management. You can run Redis locally:

```bash
# Using Docker
docker run -d -p 6379:6379 redis:latest

# Or install locally (macOS)
brew install redis
redis-server
```

## Set an API Key

Set your OpenAI API key:

```bash
export OPENAI_API_KEY=sk-...
```

## Create your first agent

```python
from factorial import Agent, AgentContext, Orchestrator

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
    description="Weather Assistant",
    instructions="You help users get weather information.",
    tools=tools,
    tool_actions={"get_weather": get_weather},
)

# Create orchestrator
orchestrator = Orchestrator()
orchestrator.register_runner(agent)

# Run the system
if __name__ == "__main__":
    orchestrator.run()
```

## Submit a task

In another terminal or script:

```python
import asyncio
from factorial import AgentContext

async def main():
    # Create task
    context = AgentContext(query="What's the weather in San Francisco?")
    task = agent.create_task(owner_id="user123", payload=context)
    
    # Submit to orchestrator
    await orchestrator.enqueue_task(agent, task)
    
    # Check status
    status = await orchestrator.get_task_status(task.id)
    print(f"Task status: {status}")

asyncio.run(main())
```

## View the dashboard

Open http://localhost:8080/observability to see the real-time dashboard with task queues, metrics, and agent performance.

## Next steps

- Learn about [Agents](agents) and how to customize them
- Explore the [Orchestrator](orchestrator) for scaling and deployment
- Check out [Examples](examples/basic-agent) for more complex use cases 