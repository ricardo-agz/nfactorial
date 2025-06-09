# Quickstart

Get started with Factorial in minutes.

## Installation

```bash
pip install nfactorial
```

## Set up Redis

Factorial uses Redis for distributed task management. Run Redis locally:

```bash
# Using Docker
docker run -d -p 6379:6379 redis:latest

# Or install locally (macOS)
brew install redis
redis-server
```

## Set your LLM API Keys

Set your OpenAI/xAI/Anthropic API key(s):

```bash
export OPENAI_API_KEY=...
export XAI_API_KEY=...
export ANTHROPIC_API_KEY=...
```

## Create your first agent

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
