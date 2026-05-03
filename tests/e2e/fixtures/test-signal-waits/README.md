# `test-signal-waits`

This fixture covers `wait.until_signal(...)` in the queued runtime:

- one probe manually wakes a waiting task through the probe control plane
- one probe lets a bounded signal wait timeout and resume automatically

It complements the existing activity-wait fixture by exercising the signal-specific
parking and wake path instead of generic steering.
