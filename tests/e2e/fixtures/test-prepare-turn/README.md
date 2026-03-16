# `test-prepare-turn`

This fixture covers the `prepare_turn` hook in the queued runtime by using a custom
LLM client that echoes the final request it received.

That lets the probe verify both halves of the contract:

- `prepare_turn` can reshape the outbound model request
- the live task transcript still preserves the original input messages
