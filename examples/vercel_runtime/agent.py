from __future__ import annotations

from factorial import Agent


def get_weather(city: str) -> str:
    return f"The weather in {city} is sunny and 72F."


assistant_agent = Agent(
    name="assistant_agent",
    instructions=(
        "You are a concise assistant. Use tools when helpful and provide a short final "
        "answer."
    ),
    tools=[get_weather],
)
