from __future__ import annotations

import json

from tests.e2e import ProbeContext, output_contains, probe, status_is


@probe(timeout_s=12.0)
async def prepare_turn_shapes_next_request_without_rewriting_transcript(
    ctx: ProbeContext,
) -> None:
    run = await ctx.run(
        "prepare_turn_agent",
        input=[
            {"role": "system", "content": "Original system input."},
            {"role": "user", "content": "Original input."},
        ],
    )
    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("override-model"),
        timeout_s=4.0,
    )

    assert isinstance(result.output, str)
    payload = json.loads(result.output)
    assert payload == {
        "max_completion_tokens": 32,
        "messages": [
            {"role": "system", "content": "Runtime-compacted prompt."},
            {"role": "user", "content": "Only send this input."},
        ],
        "model": "override-model",
        "parallel_tool_calls": False,
        "temperature": 0.1,
        "tool_choice": "required",
        "tool_count": 0,
    }

    assert result.messages[0] == {
        "role": "system",
        "content": "Original system input.",
    }
    assert result.messages[1] == {
        "role": "user",
        "content": "Original input.",
    }
    assert result.messages[2] == {
        "role": "assistant",
        "content": result.output,
    }
