# `test-verifier-lifecycle`

This fixture exercises the verifier loop as a real queued runtime behavior:

- a verifier can reject a candidate and request a retry,
- verifier feedback is injected into the next turn's transcript,
- a later attempt can pass with verification metadata, and
- a verifier can fail the run terminally after retries.

The agents intentionally avoid tools and waits so the probes isolate verifier
behavior rather than unrelated orchestration concerns.
