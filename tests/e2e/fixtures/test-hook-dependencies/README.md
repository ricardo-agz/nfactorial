# `test-hook-dependencies`

This fixture covers staged hook execution instead of a single blocking approval.

It verifies that:

- independent first-stage hooks are requested together
- resolving only one first-stage hook does not resume the task
- a dependent second-stage hook is requested only after its prerequisites resolve
- resolved hook payloads flow into later request builders and the final tool result
