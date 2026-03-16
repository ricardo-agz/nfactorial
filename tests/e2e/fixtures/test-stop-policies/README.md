# `test-stop-policies`

This fixture covers the stop-condition layer that decides when an agent run is
actually finalized.

It proves three important behaviors:

- `tool_called("done")` can finalize a run directly from tool output,
- `turn_count_is(...)` can stop a run early and fail it if no finalized output exists,
- composite stop logic can intentionally override the default "no tool calls means
  stop" behavior and require additional turns.
