# `test-agent-messaging`

This fixture exercises real agent-to-agent messaging flows inside one team:

- direct parent-to-child delivery plus read receipts
- team-scoped group broadcast plus per-member receipts

The probes intentionally validate both sides of each exchange by checking the child
inbox transcript and the parent receipt transcript.
