# `test-control-plane-waits`

This fixture covers the control-plane paths that are hard to trust from unit tests
alone, while still keeping CI fast and deterministic:

- direct human `message()` delivery during a real `wait.sleep(...)`
- `wait.activity(...)` resumed by `run.steer(...)`
- `wait.cron(...)` parking into a scheduled wait state
- hook approval resumed through the probe helper

## Design Notes

Each behavior gets its own tiny `MockAgent` plus one or two small tools.

That keeps the probes readable:

- the probe only drives the system through public HTTP APIs,
- the agent code stays canonical (`MockAgent` + tools), and
- failures point at one runtime concept at a time.

The cron probe intentionally verifies that the task *parks correctly* and then
cancels it, rather than waiting for a real minute boundary in CI.
