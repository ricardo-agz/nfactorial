# `test-retry-backoff`

This fixture verifies the queue-level retry behavior for retryable failures:

- a retryable exception should park the task in `backoff`
- maintenance should recover the task and let a later attempt succeed
- repeated retryable failures should eventually terminate once retries are exhausted
