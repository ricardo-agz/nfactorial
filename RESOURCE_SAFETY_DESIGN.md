# Resource Safety Design

## Goals

This document describes the production-safe resource coordination model now used by `nfactorial` resources, especially sandboxes.

The goals are:

- Make resource lifecycle state safe under distributed workers, retries, and recovery.
- Preserve the simple agent-facing API: `sandbox: Sandbox`, `await sandboxes.get()`, `await resources.get(Type)`.
- Keep `Sandbox` first-class while still supporting generic managed resources.
- Make cancellation and terminal cleanup actually destroy external resources instead of only deleting Redis metadata.
- Define a concrete follow-up plan for the remaining non-resource Redis atomicity gaps.

## Design Summary

The resource system now uses four layers:

1. A framework-owned `ResourceBindingRecord` stored in Redis per task/resource name.
2. A task-processing lease model based on the current task `status == processing` and `pickups` generation.
3. Atomic Lua-backed reservation/finalization steps for resource lifecycle transitions.
4. A guarded runtime handle for built-in sandboxes so stale workers cannot keep using a sandbox after they lose ownership.

That combination is what makes the design safe. The old model only stored resource JSON in Redis with plain `HGET` / `HSET` / `HDEL`, which meant concurrent workers could overwrite each other and cancellation could leak external sandboxes or snapshots.

## Core Concepts

### 1. Resource Bindings Are State Machines

Each persisted resource binding now tracks more than just `live_ref` and `checkpoint`.

It also stores:

- `phase`
- `owner_pickups`
- `operation_id`
- `updated_at`

The important phases are:

- `live`
- `checkpointed`
- `creating`
- `restoring`
- `attaching`
- `checkpointing`
- `destroying`

The non-terminal phases are reservations. They represent "a worker has atomically claimed the right to perform an external lifecycle step, but Redis is waiting for the worker to finalize that step."

### 2. Ownership Is Tied To The Task Processing Attempt

For Redis-backed task execution, a worker-owned resource manager is created with:

- `ResourceLease.worker(task.pickups)`

That means every resource mutation is tied to the task's current processing generation.

If Redis says:

- the task is no longer `processing`, or
- the task `pickups` count has changed,

then that worker has lost ownership and resource operations are rejected with `ResourceLeaseLostError`.

This gives resources a real distributed ownership boundary instead of assuming "the current Python process is still the owner."

### 3. External Side Effects Use Reserve/Finalize

Redis cannot atomically perform external operations like:

- create a Vercel sandbox
- restore from a snapshot
- snapshot a resource
- stop a sandbox
- delete a checkpoint

So the safe pattern is:

1. Atomically reserve the operation in Redis with a unique `operation_id`.
2. Perform the external lifecycle call.
3. Atomically finalize or abort the reservation.

This is now implemented by:

- `resource_begin.lua`
- `resource_commit_live.lua`
- `resource_finish.lua`

These scripts do three important jobs:

- Fence stale workers using the task's current processing lease.
- Prevent two workers from mutating the same binding at once.
- Recover abandoned in-flight reservations after a timeout instead of leaving the binding permanently wedged.

### 4. Built-In Sandboxes Are Lease-Guarded At Runtime

The biggest practical risk was not only Redis corruption. It was a stale worker continuing to use a sandbox object it had already injected.

To fix that, worker-owned sandboxes are wrapped in `GuardedSandbox` / `GuardedSandboxProcess`.

Every sandbox operation now re-validates the current worker lease before delegating to the provider handle. Once a worker loses ownership:

- `sandbox.exec(...)`
- `sandbox.spawn(...)`
- `sandbox.read_file(...)`
- `sandbox.write_file(...)`
- `sandbox.mkdir(...)`
- `sandbox.url(...)`
- `sandbox.checkpoint(...)`

all fail deterministically.

This is a stronger guarantee than only fencing lifecycle writes in Redis.

## Resource Lifecycle Semantics

### Acquire

Acquire now follows:

1. Check local in-process cache.
2. Atomically reserve the binding in Redis.
3. If there is a live ref, try attach.
4. Else if there is a checkpoint, restore.
5. Else create new.
6. Atomically commit the resulting live binding.

If attach fails because the stored live ref is dead, the reservation is aborted and acquire retries. That lets the system fall back to checkpoint restore or fresh create without leaving corrupted Redis state behind.

### Checkpoint

Checkpoint now follows:

1. Atomically reserve the binding for checkpointing.
2. Call the lifecycle's `checkpoint(...)`.
3. Best-effort destroy the live resource after checkpoint.
4. Atomically commit the checkpointed state.

If checkpoint creation fails, the reservation is aborted.

If checkpoint succeeds but finalization fails, the new checkpoint is best-effort deleted so we do not silently leak snapshots while also losing Redis ownership.

### Destroy

Destroy now follows:

1. Atomically reserve the binding for destruction.
2. Destroy the local live resource if present.
3. Otherwise attach to a persisted live ref and destroy that.
4. Otherwise delete the persisted checkpoint if the lifecycle supports checkpoint cleanup.
5. Atomically remove the binding.

This matters for:

- normal terminal completion
- worker failure recovery
- immediate cancellation when no worker is currently processing the task

## Cleanup Guarantees

### Terminal Cleanup

`destroy_all()` no longer only destroys local live resources.

It now also walks any remaining persisted bindings and cleans them up, including:

- checkpointed sandboxes from prior turns that were never re-acquired
- persisted live refs that belong to the current task and still need teardown

That closes the previous leak where terminal cleanup could drop the binding hash while leaving an external snapshot alive.

### Immediate Cancellation

`run_agent_cancellation()` no longer deletes `resource_bindings` directly.

It now creates a system-scoped resource manager and runs proper resource destruction. That means a paused or pending task cancellation now:

- destroys persisted live resources
- deletes persisted checkpoints when supported
- only then clears the binding metadata

For Vercel sandboxes, checkpoint cleanup is implemented through snapshot deletion.

## Why This Is Safe

The design is safe because it makes each failure mode explicit:

- Two workers cannot both reserve the same binding transition at once because the reservation happens inside Lua.
- A stale worker cannot successfully finalize a live binding after the task moved to a different processing attempt.
- Abandoned in-flight reservations no longer wedge the binding forever; they time out back into a stable state.
- Cancellation no longer leaks resources by deleting only Redis metadata.
- Terminal cleanup no longer leaks checkpoints from prior turns.
- Built-in sandbox operations are runtime-fenced, not just lifecycle-fenced.

## Extension Contract For Custom Resources

The framework now supports three levels of safety for custom resources:

1. `create / restore / checkpoint / destroy`
   This gives deterministic lifecycle management and safe Redis coordination.

2. Add `attach_live / capture_live_ref`
   This enables cross-worker live reattachment.

3. Add `delete_checkpoint`
   This enables full external cleanup on cancellation and terminal teardown.

For resource types that perform external side effects outside lifecycle hooks, extension authors should expose a handle that can tolerate lease invalidation or perform its own guard checks. The framework provides this automatically for built-in sandboxes.

## Files Added Or Changed

The main implementation lives in:

- `src/factorial/resources/core.py`
- `src/factorial/resources/manager.py`
- `src/factorial/resources/scripts.py`
- `src/factorial/resources/store.py`
- `src/factorial/resources/sandbox/guarded.py`
- `src/factorial/resources/sandbox/vercel.py`
- `src/factorial/queue/scripts/resources/resource_begin.lua`
- `src/factorial/queue/scripts/resources/resource_commit_live.lua`
- `src/factorial/queue/scripts/resources/resource_finish.lua`
- `src/factorial/queue/operations/control.py`
- `src/factorial/queue/worker/processor.py`

## Tests Added

The resource safety model is covered by tests for:

- live attach across managers
- checkpoint restore across attempts
- stale sandbox lease rejection
- worker-side cleanup of persisted checkpoints
- system-side cleanup of persisted live sandboxes
- paused-task cancellation cleaning persisted sandbox checkpoints

## Remaining Non-Resource Redis Gaps

The queue still has some non-resource atomicity gaps. They are outside the resource runtime, but they should be fixed next.

### Priority 0: Parent child-result resume TOCTOU

Problem:

- Python reads child wait/result state.
- Python synthesizes new parent context.
- Lua later re-checks and clears wait/result state.

Risk:

- stale context synthesis
- duplicate or lost parent resume transitions

Recommended fix:

- move child-result collection plus parent context update behind a single Lua/CAS-style ownership boundary
- or add an explicit wait-set version and only commit if the version still matches

### Priority 1: Hook registration check-then-act

Problem:

- hook/session persistence does a Python `HGET` idempotency check before a transactional pipeline write

Risk:

- duplicate hook/session registration races

Recommended fix:

- adopt the same claim/finalize Lua pattern already used by `hook_resolve.lua`
- make registration a single atomic "claim this tool call/session id" operation

### Priority 2: Global task execution fencing beyond resources

Problem:

- resource safety now uses the task `pickups` generation as a lease
- the wider task runtime still does not consistently fence all worker-side effects using an explicit execution token

Risk:

- stale workers may still do non-resource work after heartbeat loss until they naturally hit another checked boundary

Recommended fix:

- introduce a first-class execution lease token in pickup/recovery/completion scripts
- thread that token through task runtime operations, not just resources

### Priority 3: Recovery / cleanup ownership handoff

Problem:

- stale processing recovery requeues the task safely, but broader control-plane cleanup is still mixed between Lua and Python

Risk:

- hard-to-reason-about edge cases during overlapping recovery, cancellation, and completion

Recommended fix:

- move more terminal state handoff into atomic scripts
- keep Python responsible only for the unavoidable external side effects, with Redis reserve/finalize around them

### Priority 4: Concurrency Test Coverage

Problem:

- many of the hard failures only appear with overlapping workers or delayed completion

Recommended fix:

- add targeted concurrency tests for:
  - overlapping recover + checkpoint
  - overlapping cancel + resource cleanup
  - duplicate hook registration
  - parent child-result fan-in races
  - stale worker attempting post-recovery runtime operations

## Suggested Next Follow-Up

If we continue the hardening pass immediately, the next best target is the parent child-result resume path. It has the same shape as the old resource problem: Python observes state, computes a mutation, and then asks Lua to commit after the fact.
