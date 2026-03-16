from __future__ import annotations

from tests.e2e import (
    ProbeContext,
    field_equals,
    output_contains,
    probe,
    status_is,
    tool_output,
)


@probe(timeout_s=12.0)
async def typed_state_and_metadata_round_trip(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "typed_context_agent",
        input="Process this typed context.",
        state={
            "priority": 3,
            "topic": "launch",
            "processed": False,
        },
        metadata={
            "source": "api",
            "tags": ["urgent", "customer"],
        },
    )
    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("typed context processed"),
        field_equals("state.priority", 4),
        field_equals("state.topic", "launch"),
        field_equals("state.processed", True),
        field_equals("metadata.source", "api"),
        field_equals("metadata.tags.0", "urgent"),
        timeout_s=6.0,
    )

    assert result.output == "typed context processed"
    output = tool_output(result.messages, "inspect_typed_context")
    assert output == {
        "state_is_typed": True,
        "metadata_is_typed": True,
        "priority": 4,
        "topic": "launch",
        "processed": True,
        "source": "api",
        "tags": ["urgent", "customer"],
    }
