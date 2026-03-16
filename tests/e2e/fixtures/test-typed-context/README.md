# `test-typed-context`

This fixture verifies that typed `state` and `metadata` survive the HTTP enqueue
boundary, get coerced back into the agent's declared types, and are serialized out
again correctly in task snapshots and final results.
