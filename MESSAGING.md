# Messaging V1 Implementation Plan

Status: draft for iteration

## Goals

- Add first-class task-to-task messaging for multi-agent coordination.
- Keep DX simple and explicit:
  - `from factorial import messaging`
  - `messaging.groups.create(...)`, `messaging.groups.get(...)`, `messaging.groups.list()`
  - `group.send(...)`
  - `messaging.send(to_task_id, ...)` for direct messages
- Introduce a first-class team scope so naming and permissions are deterministic.
- Persist group state in Redis.
- Persist message history in Redis for observability.
- Use existing steering delivery path so running agents receive messages naturally.

## Non-Goals (V1)

- Full chat application features (edit/delete messages, reactions, typing, unread counts).
- Cross-team messaging semantics.
- Guaranteed exactly-once delivery (at-least-once is sufficient for V1).
- Reading history from agent runtime APIs (history is for observability first).

## Team Scope (New Core Primitive)

### Why

Global group names are ambiguous (`"research"` can exist many times). We need a scope boundary for:

- uniqueness
- access control
- predictable lookup (`groups.get("research")`)

### Model

- Every task has a `team_id` in `TaskMetadata`.
- Root task:
  - default `team_id = task.id` (simple, deterministic)
- Child task:
  - inherits parent `team_id`
- Sub-subagent task:
  - continues inheriting same `team_id`
- `resume_task`:
  - inherits source task `team_id` by default

### Team-scoped rules

- Group name uniqueness is enforced within team only:
  - unique key is `(team_id, group_name)`
- A task can message only tasks in the same team.
- Group membership can include only tasks in the same team.
- `groups.list()` and `groups.find(...)` are team/task scoped (not global search).

## Final DX (V1)

### Public import

```python
from factorial import messaging
```

### Group APIs

```python
group = await messaging.groups.create("research", members=jobs)
group = await messaging.groups.get("research")
groups = await messaging.groups.list()
matches = await messaging.groups.find("research")

await messaging.groups.send("research", "kickoff")  # one-off convenience
await group.send("share findings")
await group.add_members(more_jobs)
```

### Direct message API

```python
await messaging.send(to_task_id, "you own synthesis")
```

## Human Messaging (Control Plane)

Human-originated messaging now has explicit control-plane endpoints:

- `POST /api/tasks/{task_id}/message`
  - body: `{ "owner_id": "...", "content": "...", "metadata": {...} }`
- `POST /api/groups/message`
  - body supports one of:
    - `{ "owner_id": "...", "content": "...", "group_id": "..." }`
    - `{ "owner_id": "...", "content": "...", "task_id": "...", "group_name": "..." }`
    - `{ "owner_id": "...", "content": "...", "team_id": "...", "group_name": "..." }`

Delivery uses the same steering and activity-wake path as agent messaging, and
returns the same delivery receipt shape (`delivered_task_ids`,
`skipped_inactive_task_ids`, `failed_task_ids`) plus thread metadata.

`group_id` is a deterministic encoded identifier:

- format: `grp1.<base64url({"team_id":"...","group_name":"..."})>`
- canonicalized from `(team_id, group_name)` to avoid ambiguous global names

## Design Decisions

- No mode strings (`mode="create"` / `mode="require"`). Use explicit methods.
- `groups.get(...)` is strict and raises if missing in current team scope.
- `groups.create(...)` is strict and raises if name already exists in current team scope.
- Group membership management is explicit (`add_members`, optional `remove_members`).
- Creator task is auto-added as member in `groups.create(...)`.
- Group names are labels/keys inside team scope; not globally unique.

## Delivery Model

Hybrid model:

1. Persist metadata and history in Redis.
2. Deliver messages via task steering inboxes.

This keeps runtime behavior aligned with existing worker steering logic while giving full observability.

## Redis Data Model

Proposed key patterns:

- `MESSAGING_GROUP_META = "{namespace}:messaging:groups:{team_id}:meta"`
  - HASH `group_name -> group_meta_json`
- `MESSAGING_GROUP_MEMBERS = "{namespace}:messaging:groups:{team_id}:{group_name}:members"`
  - SET of `task_id`
- `MESSAGING_GROUPS_BY_TASK = "{namespace}:messaging:groups:by_task:{task_id}"`
  - SET of scoped refs (e.g. `"{team_id}:{group_name}"`) for fast list/find
- `MESSAGING_TEAM_TASKS = "{namespace}:messaging:teams:{team_id}:tasks"`
  - SET of `task_id` in team (optional optimization for validation/discovery)
- `MESSAGING_THREAD_HISTORY = "{namespace}:messaging:thread:{thread_id}:history"`
  - STREAM for per-thread message history
- `MESSAGING_HISTORY_GLOBAL = "{namespace}:messaging:history"`
  - STREAM for global observability timeline (optional but recommended)

Thread ID format:

- Group: `group:{team_id}:{group_name}`
- Direct: `dm:{team_id}:{min_task_id}:{max_task_id}` (deterministic pair within team)

Retention:

- Streams should be capped (`MAXLEN ~ N`) and/or TTL-controlled.
- Group metadata can be durable; optional cleanup in maintenance worker.

## Message Envelope

Canonical persisted message payload:

- `message_id` (stream ID)
- `thread_id`
- `kind` (`group` | `direct`)
- `team_id`
- `group_name` (nullable for direct)
- `from_task_id`
- `from_owner_id`
- `to_task_ids` (list)
- `content` (text)
- `metadata` (optional dict)
- `created_at` (unix seconds or ISO)

Steering payload injected to recipients should remain valid model message schema:

```python
{
  "role": "user",
  "content": "<peer_message kind='group' team_id='...' group='research' from_task_id='...'>...</peer_message>"
}
```

This avoids sending non-standard keys into model provider message arrays.

## Runtime Semantics

### `messaging.send(to_task_id, content)`

- Validate sender from `ExecutionContext.current()`.
- Validate target task exists and belongs to same `team_id`.
- Append history record to direct thread stream.
- Enqueue one steering message to target task.
- Return delivery receipt.

### `groups.send(group_name, content)` / `group.send(content)`

- Resolve group by `(current_team_id, group_name)`.
- Validate sender is a member of the group.
- Resolve current members.
- Append one history record.
- Fan out steering to members except sender.
- Skip inactive/terminal targets and return structured report:
  - `delivered_task_ids`
  - `skipped_inactive_task_ids`
  - `failed_task_ids`

Recommendation: skip inactive and report (do not fail whole operation).

### `groups.list()` / `groups.find(name)`

- List only groups the current task belongs to.
- `find(name)` filters within that same task-scoped set.
- No global find in runtime agent API.

## Error Model

Add explicit exceptions (in `factorial.exceptions`):

- `MessagingError`
- `MessagingGroupNotFoundError`
- `MessagingGroupAlreadyExistsError`
- `MessagingPermissionError`
- `MessagingScopeError` (cross-team violation)
- `MessagingInvalidRecipientError`

## Events / Observability

Publish owner-channel events (via existing `EventPublisher`) with `AgentEvent`:

- `messaging_group_created`
- `messaging_group_members_added`
- `messaging_group_message_sent`
- `messaging_direct_message_sent`
- `messaging_delivery_partial` (when some recipients skipped/failed)

Each event should include `team_id`, and include `group_name` when relevant.

Observability UI can render history from Redis streams and correlate by task ID.

## Code Changes (Concrete)

### 0) Task metadata scope support

- Update `src/factorial/queue/task.py`
  - add `team_id` to `TaskMetadata` serialization model
- Ensure defaults for roots and inheritance for children/resumes:
  - enqueue root -> team_id initialized
  - spawn child -> parent team_id copied
  - resume task -> source team_id copied

### 1) New public namespace

- Add `src/factorial/messaging.py`
  - `MessagingNamespace`
  - `MessagingGroupsNamespace`
  - `MessagingGroupHandle`
  - singleton `messaging`

### 2) Exports

- Update `src/factorial/__init__.py`
  - export `messaging` and messaging namespace types

### 3) ExecutionContext callbacks

- Update `src/factorial/context.py`
  - add callback fields used by messaging namespace:
    - create/get/list/find group
    - add members
    - group send
    - direct send

### 4) Worker wiring

- Update `src/factorial/queue/worker.py`
  - implement callback closures bound to current `redis_client`, `namespace`, and current task
  - pass callbacks when constructing `ExecutionContext`

### 5) Queue operations

- Update `src/factorial/queue/operations.py`
  - add messaging operations for:
    - create/get/list/find groups (team-scoped)
    - add members (same-team enforcement)
    - send group
    - send direct
  - include team and membership validation
  - write history streams
  - enqueue steering messages

### 6) Redis keys

- Update `src/factorial/queue/keys.py`
  - add messaging key constants and `RedisKeys` helpers
  - include team-scoped key builders

### 7) Queue exports

- Update `src/factorial/queue/__init__.py` to export new operations where needed

### 8) Docs

- Update `docs/docs/events.md` with messaging event types
- Add `docs/docs/messaging.md` with team scope semantics
- Update `docs/docs/examples/multi_agent.md` with messaging examples

## Testing Plan

### Unit tests

- `tests/unit/test_messaging_contracts.py`
  - namespace API shape
  - `groups.create/get/list/find`
  - strict errors (`already exists in team`, `not found in team`)
  - team-scope permission checks

### Integration tests

- `tests/integration/test_messaging.py`
  - create group with spawned child tasks in same team
  - same group name allowed in different teams
  - group fanout delivery via steering
  - direct message delivery (same team)
  - cross-team direct message rejection
  - inactive recipient skip behavior and receipts
  - `groups.list()` from current task context
  - history persistence checks (stream entries)

### Regression checks

- Ensure existing steering tests still pass.
- Ensure subagent/wait flows remain unchanged without messaging usage.

## Rollout Plan

### Phase 1 (core)

- Team scope in task metadata.
- Redis model, namespace API, group CRUD/list/find, direct send, group send, steering fanout, history streams.

### Phase 2 (observability)

- Dashboard/API views over history and delivery outcomes.

### Phase 3 (optional)

- remove members / archive groups / cleanup policies / richer analytics.

## Activity Wait V2 (Deadlock-Safe)

### Problem

`wait.activity()` is useful for event-driven agents, but naive semantics can deadlock:

- parent sleeps for activity
- children sleep for activity
- no actor emits new activity

Team-wide idle detection alone is too coarse for multi-tier trees. In a hierarchy
(`root -> middle -> bottom`), we must wake the **closest waiting parent** as soon
as its local subtree is quiescent.

### Decision (Locked)

- Add `wait.activity()` as a first-class wait primitive.
- Wake granularity is **hierarchical and local**:
  - if all direct children of parent `P` are quiescent and `P` is waiting on
    activity, wake `P` immediately.
- Do not wait for entire team idle before waking local parent tiers.
- Team-wide idle can remain an optional fallback for root tasks only.

This gives deterministic progression in tiered trees and avoids full-team stalls.

### API (V2)

Add to `factorial.wait` namespace:

```python
from factorial import wait

return TurnCompletion(
    is_done=False,
    context=ctx,
    tool_call_results=[(tool_call, wait.activity(data={"reason": "await_messages"}))],
)
```

`WaitInstruction` update:

- `kind`: include `"activity"`
- `data`: optional payload, preserved in wait metadata/events

### Wake Sources for `wait.activity()`

A task waiting on activity wakes when any of the following occurs:

1. top-level steering (`orchestrator.steer_task`)
2. direct message (`messaging.send(...)`)
3. group message (`messaging.groups.send(...)` / `group.send(...)`)
4. direct child terminal transition (`completed|failed|cancelled`)
5. subtree quiescence signal (`subtree_idle`) for a waiting parent

### Quiescence Semantics (Hierarchical)

For parent `P`:

- `P` is eligible only if it is currently in activity wait.
- Evaluate direct children set `children(P)`:
  - `busy`: child in `queued|active|processing|backoff|pending_tool_results|pending_child_tasks|paused(sleep/cron)`
  - `quiescent`: child in `paused(activity)` or terminal
- Wake condition:
  - no direct child is `busy`, and
  - at least one direct child is `paused(activity)` (prevents false positives on empty subtrees)

When condition is met, atomically wake `P` and inject synthetic steering:

```python
{
  "role": "user",
  "content": "<system_activity kind='subtree_idle' parent_task_id='P'/>"
}
```

This wake can propagate upward naturally: if middle tier wakes/re-evaluates and
then sleeps again, the same local rule can wake its parent later.

## Atomic Queue Design (Lua-First)

All multi-step state transitions remain Lua-backed.

### New/Updated Redis Keys

- `TASK_CHILDREN = "{namespace}:tasks:children:{parent_task_id}"`
  - SET of direct child task IDs
- `ACTIVITY_WAIT_META = "{namespace}:wait:activity:meta"`
  - HASH `task_id -> json` (`wait_kind`, `entered_at`, optional `data`, `epoch`)
- `ACTIVITY_WAITERS_BY_TEAM = "{namespace}:wait:activity:team:{team_id}"`
  - SET of waiting task IDs (optional optimization for team fallback)

### New Lua Scripts

1. `wait_activity.lua`
   - Transition: `processing|active -> paused(activity)`
   - Atomically:
     - remove heartbeat
     - persist payload + `ACTIVITY_WAIT_META`
     - add task to `queue_pending`
     - add waiter markers (team/global sets)
     - optionally evaluate parent-local quiescence and wake parent if needed

2. `steering_enqueue.lua` (replace direct hash write in `steer_task`)
   - Atomically:
     - validate task existence/non-terminal
     - append steering messages
     - if target is `paused(activity)`, wake immediately:
       - status `paused -> active`
       - remove activity wait markers
       - `LPUSH queue_main`

3. `activity_parent_probe.lua` (or shared helper in `shared.lua`)
   - Given parent task ID:
     - read direct children from `TASK_CHILDREN`
     - classify statuses
     - if wake condition true and parent is `paused(activity)`, wake + inject synthetic activity message

### Existing Scripts to Update

- `enqueue.lua`, `enqueue_batch.lua`, `resume_enqueue.lua`
  - if task has `parent_id`, `SADD TASK_CHILDREN[parent_id] child_id`
- `completion.lua`, `cancellation.lua`, `schedule_wait.lua`, `wait_activity.lua`
  - call parent probe helper after child state transition (where relevant)
- messaging send scripts can keep using steering payload writes if they route through
  `steering_enqueue.lua`; otherwise they should inline identical wake semantics.

## Runtime Wiring Changes

### Wait namespace

- `src/factorial/waits.py`
  - add `WaitInstruction.kind = "activity"`
  - add `wait.activity(data=None)`

### Worker

- `src/factorial/queue/worker.py`
  - in wait handling branch, support `wait_kind == "activity"`
  - call `wait_activity.lua` wrapper instead of `schedule_wait.lua`
  - publish `task_paused` with `wait_kind="activity"`

### Steering path

- `src/factorial/queue/operations.py`
  - migrate `steer_task(...)` from `HSET` to Lua wrapper (`steering_enqueue.lua`)
  - keep message envelope shape unchanged

## Event Model Additions

Add owner-channel events:

- `task_activity_waiting`
- `task_activity_woken` with `reason`:
  - `steering`, `direct_message`, `group_message`, `child_terminal`, `subtree_idle`, `team_idle`
- `task_subtree_idle_detected`

## Safety and Idempotency

- Wakes are idempotent:
  - if task is already active/queued, no duplicate queue push
- Synthetic subtree wake is one-shot per wait epoch:
  - increment `epoch` when entering activity wait
  - wake logic checks/stamps epoch to avoid duplicate wake storms
- Terminal tasks are never reactivated by activity wake paths.

## Testing Plan (V2)

### Unit

- `tests/unit/test_wait_activity_contracts.py`
  - API shape (`wait.activity`)
  - validation and serialization
- Lua wrapper tests:
  - `wait_activity` transition
  - `steering_enqueue` wake behavior
  - parent probe wake condition matrix

### Integration

- `tests/integration/test_wait_activity.py`
  - task parked on activity wakes on top-level steer
  - task parked on activity wakes on direct message
  - task parked on activity wakes on group message
  - hierarchical scenario:
    - bottom + middle waiting on activity
    - all bottom quiescent => middle wakes (without full-team idle)
  - concurrent message fan-in does not duplicate queue entries
  - cancellation of activity-waiting task still works

### Regression

- Existing `sleep`, `cron`, `jobs`, `pending_tool_results`, `pending_child_tasks`
  behavior unchanged.
- Existing steering tests still pass.

## Rollout Plan (Activity Wait)

### Phase A - Core wake-on-message

- Add `wait.activity`
- Add Lua steering enqueue wake semantics
- Add tests for direct/group/top-level wake

### Phase B - Hierarchical quiescence

- Add `TASK_CHILDREN`
- Add parent probe helper + subtree synthetic wake
- Add multi-tier integration tests

### Phase C - Optional team fallback

- Add root-level `team_idle` synthetic wake only if needed after Phase B.

## Outstanding Decisions (Narrow)

- Whether to ship team-wide fallback (`team_idle`) in same release as subtree wake,
  or keep it Phase C.
- Final XML tag name for synthetic activity payload (`system_activity` vs `activity_event`).
