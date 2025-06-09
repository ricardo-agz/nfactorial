---
slug: /
---

# Factorial

Factorial is a Python framework for building AI agents with advanced orchestration capabilities. It provides a lightweight, production-ready system for creating intelligent agents that can work together to solve complex tasks.

## Key Features

- **Simple Agent Creation**: Build agents with just a few lines of code
- **Advanced Orchestration**: Coordinate multiple agents seamlessly  
- **Tool Integration**: Extensible tool system for external integrations
- **Distributed Execution**: Built-in support for scalable, distributed processing
- **Real-time Monitoring**: Comprehensive observability and debugging tools

## Core Concepts

Factorial is built around three main primitives:

### 🤖 Agents
LLMs equipped with instructions and tools for specific tasks. Agents handle the core logic of processing requests and making decisions.

### 🎯 Orchestrator  
Coordinates multiple agents to work together on complex workflows. The orchestrator manages task distribution and agent communication.

### 🛠️ Tools
Extensible functions that agents can call to interact with external systems, APIs, databases, and more.

## Quick Example

```python
from factorial import BaseAgent, AgentContext

# Define a simple tool
def get_weather(city: str, agent_ctx: AgentContext) -> str:
    return f"The weather in {city} is sunny and 72°F"

# Create an agent
weather_agent = BaseAgent(
    description="Weather Assistant",
    instructions="You help users get weather information.",
    tools=[{
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get weather for a city",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {"type": "string"}
                },
                "required": ["city"]
            }
        }
    }],
    tool_actions={"get_weather": get_weather}
)

# Use the agent
context = AgentContext(query="What's the weather in San Francisco?")
task = weather_agent.create_task(owner_id="user123", payload=context)
result = weather_agent.run_task(task.id)
```

## Why Factorial?

The framework follows two key design principles:

1. **Enough features to be worth using**, but few enough primitives to make it quick to learn
2. **Works great out of the box**, but you can customize exactly what happens

Whether you're building a simple chatbot or a complex multi-agent system, Factorial provides the tools you need without unnecessary complexity.

## Next Steps

- [**Quickstart**](./quickstart): Get up and running in 5 minutes
- [**Agents**](./agents): Learn how to create and configure agents
- [**Tools**](./tools): Build custom tools for your agents
- [**Examples**](./examples): See practical examples and patterns
