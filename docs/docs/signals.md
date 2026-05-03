# Signals

Signals let queued tasks pause until a named event arrives. They are useful when you want explicit turn-gating instead of loosely coupling everything through plain messages.

Signals are a **queued-runtime feature**. Use them with the `Orchestrator`, not direct `agent.run(...)`.

## Waiting for a signal

Use `wait.until_signal(...)` inside a tool to park the task:

```python
from factorial import WaitInstruction, tool, wait


@tool
def wait_for_launch() -> WaitInstruction:
    return wait.until_signal(
        "fixture.launch",
        data={"reason": "awaiting_fixture_signal"},
    )
```

When the task is waiting, its snapshot includes:

- `status == "waiting"`
- `wait.kind == "signal"`
- `wait.signal_id`
- any `wait.data` payload you attached

## Waiting with a timeout

Signal waits can include a timeout. The timeout must be another wait instruction created with `wait.sleep(...)` or `wait.cron(...)`.

```python
@tool
def wait_for_approval() -> WaitInstruction:
    return wait.until_signal(
        "approval",
        timeout=wait.sleep(300),
        data={"reason": "awaiting_approval"},
    )
```

## Reading the signal payload

Once the task resumes, use `signals.current()` to inspect the delivered signal:

```python
from factorial import signals


signal = signals.current()
if signal is not None:
    print(signal.signal_id)
    print(signal.payload)
    print(signal.sender_task_id)
```

`signals.current()` returns a `SignalEnvelope` with:

- `signal_id`
- `payload`
- `sender_task_id`
- `sent_at`
- `seq`
- `wake_reason`

If you need just the reason the wait resumed, use:

```python
reason = signals.wake_reason()
```

Typical wake reasons include a delivered signal and a timeout firing.

## Signaling child tasks

The most common producer is a parent task waking its direct children:

```python
from factorial import subagents


await subagents.signal(
    child_job,
    signal_id="fixture.launch",
    payload={"approved": True},
)
```

You can also signal multiple children at once:

```python
await subagents.signal(
    jobs,
    signal_id="day_vote_open:2",
    payload={"round_no": 2},
)
```

## Manual wake from the control plane

If a task is waiting, the control plane can resume it manually:

```python
woke = await task.wake("Launch approved.")
print(woke)
```

This is especially useful for UIs or probe/test helpers. Manual wake injects a runtime note plus your optional input back into the task transcript.

## A complete pattern

```python
from factorial import WaitInstruction, signals, tool, wait


@tool
def poll_for_phase() -> WaitInstruction:
    return wait.until_signal(
        "day_vote_open:2",
        timeout=wait.sleep(30),
        data={"phase": "day_discussion"},
    )


@tool
def handle_phase_transition() -> str:
    signal = signals.current()
    if signal is None:
        return "No signal was available."

    if signal.wake_reason == "timeout":
        return "Signal wait timed out."

    return f"Received {signal.signal_id} with payload {signal.payload!r}"
```

## When to use signals vs messaging

Use **signals** when:

- a task should block until a named event occurs
- wake/resume behavior matters
- you want explicit structured phase transitions

Use **messaging** when:

- the task should receive conversational or threaded communication
- you want inbox history and receipts
- the content should be part of the runtime conversation flow
