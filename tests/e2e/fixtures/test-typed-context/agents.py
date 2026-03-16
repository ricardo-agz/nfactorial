from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from factorial import Agent, AgentContext, tool
from factorial.testing import ScriptedMockLLMClient, mock_model, tool_call


@dataclass
class FixtureState:
    priority: int = 0
    topic: str = ""
    processed: bool = False


@dataclass
class FixtureMetadata:
    source: str = "default"
    tags: list[str] = field(default_factory=list)


@tool
async def inspect_typed_context(
    agent_ctx: AgentContext[FixtureState, FixtureMetadata],
) -> dict[str, Any]:
    state = agent_ctx.state
    metadata = agent_ctx.metadata

    state.priority += 1
    state.processed = True

    return {
        "state_is_typed": isinstance(state, FixtureState),
        "metadata_is_typed": isinstance(metadata, FixtureMetadata),
        "priority": state.priority,
        "topic": state.topic,
        "processed": state.processed,
        "source": metadata.source,
        "tags": list(metadata.tags),
    }


typed_context_agent = Agent[FixtureState, FixtureMetadata](
    name="typed_context_agent",
    instructions="Inspect and mutate typed state and metadata.",
    tools=[inspect_typed_context],
    client=ScriptedMockLLMClient(
        responses=[
            tool_call("inspect_typed_context"),
            "typed context processed",
        ]
    ),
    model=mock_model,
)
