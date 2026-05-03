# Messaging

Factorial includes first-class runtime messaging for queued runs. Use it when agents need to coordinate with each other or when humans need to inject messages into active tasks and groups.

Messaging is a **queued-runtime feature**. It is available when your agent is running under the `Orchestrator`, not in direct `agent.run(...)` mode.

## Direct task-to-task messaging

Use `messaging.send(...)` to send a message to another task:

```python
from factorial import messaging


report = await messaging.send(
    to_task_id,
    "Please own the synthesis.",
    data={"kind": "assignment"},
    metadata={"phase": "planning"},
)

print(report.delivered_task_ids)
```

The returned delivery report includes:

- `thread_message_id`
- `global_message_id`
- `delivered_task_ids`
- `skipped_inactive_task_ids`
- `failed_task_ids`

`messaging.direct.send(...)` is available as an alias, but `messaging.send(...)` is the preferred API.

## Group messaging

Create or fetch a team-scoped messaging group:

```python
from factorial import messaging


group = await messaging.groups.create("research", members=jobs)
await group.send("Kickoff: each agent should investigate one source.")
```

You can also use the namespace methods directly:

```python
await messaging.groups.send(
    "research",
    "Please post your latest findings.",
    data={"kind": "group_prompt"},
)

groups = await messaging.groups.list()
existing = await messaging.groups.get("research")
matches = await messaging.groups.find("research")
```

Manage membership explicitly:

```python
await messaging.groups.add_members("research", more_jobs)
await messaging.groups.remove_members("research", [task_id])
await messaging.groups.leave("research")
```

`messaging.group.send(...)` is available as an alias for `messaging.groups.send(...)`.

## Structured payloads

Both direct and group messaging support:

- `content`: user-visible message text
- `data`: structured payload for automation
- `metadata`: supplemental observability metadata

```python
await messaging.send(
    to_task_id,
    "Vote cast.",
    data={"kind": "day_vote", "target_player_id": "player-7"},
    metadata={"round_no": 2},
)
```

Use `data` when the receiver should parse the message programmatically.

## Reading the direct inbox

Use the `inbox` namespace from inside a running task:

```python
from factorial import inbox


page = await inbox.direct.peek(unread_only=True, limit=20)

for message in page.messages:
    print(message.content, message.from_task_id, message.data)
    await message.mark_read(
        notify_sender=True,
        data={"ack": True},
    )
```

If you want to acknowledge the whole page in one call:

```python
await page.mark_read(notify_sender=True, data={"ack": "batch"})
```

## Reading a group inbox

```python
page = await inbox.group.peek("research", unread_only=True, limit=20)

for message in page.messages:
    print(message.group_name, message.content)
```

Group inbox messages support the same `mark_read(...)` flow as direct messages.

## Reading receipts

When you mark a message as read with `notify_sender=True`, the sender receives a receipt in `inbox.receipts`:

```python
receipts = await inbox.receipts.peek(unread_only=True, limit=50)

for receipt in receipts.messages:
    print(receipt.reader_task_id, receipt.data)

await receipts.mark_read()
```

## Typed payloads

Inbox messages expose `data_as(...)` when you want to validate a structured payload:

```python
from pydantic import BaseModel


class DayVote(BaseModel):
    kind: str
    target_player_id: str


page = await inbox.direct.peek(unread_only=True, limit=10)
vote = page.messages[0].data_as(DayVote)
```

## Human control-plane messaging

External servers and UIs can inject human-originated messages through the orchestrator.

### Message one task

```python
report = await orchestrator.message_task(
    task_id=task.id,
    owner_id="user123",
    content="Please respond to the latest question.",
    data={"kind": "human_followup"},
)
```

### Message a group

```python
report = await orchestrator.message_group(
    owner_id="user123",
    content="Discussion is now open.",
    data={"kind": "moderator_broadcast"},
    group_name="research",
    task_id=task.id,
)
```

## Practical notes

- Messaging is team-scoped and designed for intra-workflow coordination.
- Inbox APIs are available only during active task execution.
- Use `data` for machine-readable payloads and `content` for the model-visible text.
- If you need agents to wake up on a structured event rather than a message, see [Signals](signals.md).
