# `test-hook-idempotency`

This fixture verifies that resolving the same pending hook twice with the same
`idempotency_key` is safe:

- the first resolution is applied normally
- the second resolution is reported as idempotent replay
- the task still completes exactly once with the expected tool output
