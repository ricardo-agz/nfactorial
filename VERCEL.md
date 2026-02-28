# Vercel Runtime Implementation Plan for nfactorial

Status: Draft v2 (reviewed and revised)

Audience: maintainers implementing dual runtime support (process + Vercel serverless)

Scope: Keep Redis + Lua as the execution state machine. Replace long-running worker hosting with Vercel worker/cron services.

---

## 0) Revision summary (why v2)

This v2 revision was produced after a self-review pass focused on:

- architecture safety (what can break parity)
- explicit runtime contracts (worker and cron invocation behavior)
- app-author DX (what users actually write)
- operational reliability (timeouts, duplicate delivery, backpressure)

Key upgrades from v1:

- Added concrete callback algorithms (worker and cron).
- Added an explicit data contract for wake messages.
- Added stronger migration safety and rollback strategy.
- Added reliability invariants and acceptance checks.
- Added app-author quickstart guidance and release gating criteria.

---

## 1) Assumptions and Decisions

### Assumptions

- Vercel cron services are available and reliable soon.
- Vercel queue worker services are available and support the current queue trigger semantics.
- Redis remains available as a managed service reachable by Vercel functions.

### Hard decisions

- Redis + Lua remains the source of truth for task state transitions.
- Vercel queue messages are dispatch/wake triggers, not workflow state.
- All correctness-sensitive transitions remain in existing Lua scripts.
- The existing process runtime remains first-class and continues to work.

### Architecture Decision Record (ADR) snapshot

| ADR | Decision | Why |
| --- | --- | --- |
| ADR-001 | Redis is authoritative state; Vercel Queue is wake transport | Preserves tested Lua atomicity and minimizes rewrite risk |
| ADR-002 | Convert loops into bounded ticks | Enables process + serverless hosts to share one engine |
| ADR-003 | Runtime adapters live under `runtimes/*` | Keeps platform glue out of core engine/backend code |
| ADR-004 | Keep process runtime default until parity proven | Safe rollout and easy rollback |
| ADR-005 | Add `WakeDispatch` abstraction now | Avoids `if vercel` branching in control-plane paths |

### Non-goals

- Replacing Redis data model with Vercel queues alone.
- Rewriting Lua transition logic in SQL as part of this work.
- Breaking existing orchestrator API used by users today.

---

## 2) Goals

1. Run nfactorial on Vercel serverless without requiring long-running processes.
2. Preserve current semantics (retry, backoff, scheduled waits, activity waits, parent-child joins, hook wakes, cancellation, idempotency).
3. Keep a clean architecture where runtime hosting and queue transport are replaceable.
4. Keep process runtime stable and low-risk during migration.
5. Provide a good app-author DX with minimal Vercel wrapper code.

---

## 3) Current State (brief)

- Long-running `worker_loop` and `maintenance_loop` own orchestration pacing.
- Redis Lua scripts enforce atomic transitions across:
  - task status hashes
  - queue lists/zsets
  - pending child/tool sets
  - heartbeat zsets
  - messaging steering and wake mechanics
- Control-plane operations (`enqueue`, `resume`, `cancel`, `steer`, hook resolve) currently mutate Redis but do not target a serverless dispatch transport.

Implication: We do not need to redesign state transitions, only how work is scheduled and executed in bounded serverless invocations.

---

## 4) Target Runtime Architecture

## 4.1 Runtime split

- **Engine layer (shared):**
  - one-step worker execution (`worker_tick`)
  - one-step maintenance execution (`maintenance_tick`)
  - task processor, state machine, runtime ops
- **Backend layer (shared, existing):**
  - Redis key model
  - Lua scripts + operations
- **Host runtimes (replaceable):**
  - `process`: infinite loops calling shared ticks
  - `vercel`: queue callback + cron callback invoking shared ticks

## 4.1.1 Control plane vs data plane

- **Control plane**
  - User/API actions: enqueue, resume, cancel, steer, hook resolve, metadata reads.
  - Trigger source: HTTP endpoints (web service or in-process app server).
  - Side effect: Redis mutation + optional dispatch wake.
- **Data plane**
  - Actual task execution and maintenance/recovery.
  - Trigger source:
    - Process mode: long-running loops
    - Vercel mode: queue callback + cron callback

## 4.2 Core idea

Convert "while True loops" into bounded ticks:

- `worker_tick(...)`: processes up to N batches or T seconds.
- `maintenance_tick(...)`: runs one maintenance pass (or bounded pass) and returns recovery summary.

This lets:

- process runtime call ticks in loops
- Vercel runtime call ticks per event/invocation

## 4.3 Dispatch model

- Keep Redis agent queues (`queue_main`) as canonical ordering.
- Introduce dispatch wakes via Vercel queues as hints:
  - enqueue/resume/steer/messaging/hook wake/scheduled recovery trigger wake events
  - wake events tell serverless runtime "run a worker tick for agent X"
- Duplicate wake messages are acceptable (ticks are idempotent when no work).

## 4.4 Runtime selection and flags

Use platform-aware host selection:

- If `VERCEL=1` (set automatically by Vercel), runtime host is `vercel`.
- Otherwise, runtime host is `process`.

Optional flag:

- `NFACTORIAL_WAKE_TRANSPORT=none|vercel_queue`
  - default is `vercel_queue` when `VERCEL=1`
  - default is `none` otherwise

Recommended rollout:

1. Baseline on existing non-Vercel process deployment.
2. Canary on Vercel deployment (`VERCEL=1` auto-selects `vercel`) for selected agents/namespaces.
3. Gradually expand coverage after parity and SLO checks pass.

---

## 5) Proposed Code Directory Structure

Target structure (library side):

```text
src/factorial/
  control_plane/
    orchestrator.py
    commands.py

  engine/
    worker_tick.py
    maintenance_tick.py
    processor.py
    state_machine.py
    runtime_ops.py

  backend/
    redis/
      keys.py
      lua/
        _core.py
        _runtime.py
        interface.py
        scripts/
      operations/
        enqueue.py
        control.py
        messaging.py
        hooks.py

  contracts/
    wake_dispatch.py
    runtime_host.py

  runtimes/
    process/
      worker_loop.py
      maintenance_loop.py
      supervisor.py
    vercel/
      bootstrap.py
      worker_service.py
      cron_service.py
      # web app surface now lives on Orchestrator.create_app()
      wake_dispatcher.py
      settings.py
```

Notes:

- Existing imports should keep compatibility shims during migration.
- `src/factorial/queue/...` can be retained and internally delegate to the new paths until final cleanup.

---

## 6) Detailed Migration Map

## 6.1 File moves/refactors

- `src/factorial/queue/worker/processor.py` -> `src/factorial/engine/processor.py`
- `src/factorial/queue/worker/state_machine.py` -> `src/factorial/engine/state_machine.py`
- `src/factorial/queue/worker/runtime_ops.py` -> `src/factorial/engine/runtime_ops.py`
- `src/factorial/queue/worker/loop.py` becomes thin host wrapper in `runtimes/process/worker_loop.py`
- `src/factorial/queue/maintenance.py` becomes:
  - `engine/maintenance_tick.py` (core logic)
  - `runtimes/process/maintenance_loop.py` (loop wrapper)
- `src/factorial/queue/keys.py` -> `backend/redis/keys.py`
- `src/factorial/queue/lua/*` -> `backend/redis/lua/*`
- `src/factorial/queue/operations/*` -> `backend/redis/operations/*`

## 6.2 Compatibility strategy

- Keep old import paths as forwarding modules for one release cycle.
- Add deprecation warnings only after serverless runtime is stable.

---

## 7) New Abstractions (minimal set)

## 7.1 `WakeDispatch` contract

Create `src/factorial/contracts/wake_dispatch.py`:

- `async def wake_agent(agent_name: str, reason: str, task_id: str | None = None) -> None`
- `async def wake_agents(agent_names: list[str], reason: str) -> None`
- `async def flush() -> None` (optional for buffered/coalesced implementations)

Implementations:

- `NoopWakeDispatch` (process runtime)
- `VercelQueueWakeDispatch` (sends queue messages to configured topic)

## 7.2 Tick contracts

`engine/worker_tick.py`:

- `async def worker_tick(..., max_batches: int, max_tasks: int, max_runtime_s: float) -> WorkerTickResult`

`engine/maintenance_tick.py`:

- `async def maintenance_tick(..., max_cleanup_batch: int) -> MaintenanceTickResult`

Result includes counts:

- picked tasks
- processed tasks
- recovered stale/backoff/scheduled/pending-child
- expired hooks
- cleanup counts
- touched agents to wake

`WorkerTickResult` fields (minimum):

- `processed_tasks: int`
- `picked_tasks: int`
- `cancelled_tasks_processed: int`
- `remaining_backlog_estimate: int | None`
- `touched_agents: list[str]`
- `duration_ms: int`

`MaintenanceTickResult` fields (minimum):

- `stale_recovered: int`
- `backoff_recovered: int`
- `scheduled_recovered: int`
- `pending_child_resumed: int`
- `expired_hooks: int`
- `expired_tasks_removed: int`
- `expired_batches_removed: int`
- `touched_agents: list[str]`
- `duration_ms: int`

## 7.3 Orchestrator host selection

Host selection should be environment-driven:

- If `VERCEL=1`, force host mode to `vercel`.
- Otherwise default host mode is `process`.

`runtime_mode="process" | "vercel"` can remain as a compatibility/testing override, but Vercel deployments should rely on `VERCEL=1` auto-selection.

Behavior:

- process mode: existing `run()`
- vercel mode: no local loops; expose reusable app builders under `factorial.runtimes.vercel.*`

Public helper signatures (proposed):

- `orchestrator.create_app(*, enable_ws: bool = False, cors_origins: list[str] | None = None) -> FastAPI`
- `create_worker(orchestrator: Orchestrator) -> ASGI | WSGI`
- `orchestrator.run_maintenance_cron_tick() -> dict[str, Any]` (cron script entrypoint)

---

## 8) Vercel Service Topology

## 8.1 Services

- `web` service
  - control-plane HTTP endpoints
  - event streaming endpoints with transport policy:
    - on Vercel (`VERCEL=1`): SSE-first
    - on non-Vercel process deployments: SSE + optional WebSocket
- `worker` service
  - queue callback endpoint
  - runs bounded worker tick
- `cron` service
  - scheduled maintenance callback
  - runs bounded maintenance tick

## 8.1.1 Event transport policy (SSE and WebSocket)

- Keep one event payload schema regardless of transport so clients can switch without parsing differences.
- Default transport behavior:
  - Vercel web service: `GET /events/{owner_id}` SSE endpoint enabled by default.
  - Process runtime web service: SSE endpoint plus optional `WS /ws/{owner_id}` endpoint.
- Client recommendation:
  - Use SSE as default.
  - Use WebSocket only when running in environments where persistent connections are expected and desired.

## 8.2 Topics and consumers

- dispatch topic: `nfactorial-dispatch` (configurable)
- consumer group: `default` (configurable)

Queue payload example:

```json
{
  "schema_version": 1,
  "kind": "wake_agent",
  "namespace": "factorial",
  "agent_name": "parent_coordinator",
  "reason": "enqueue",
  "task_id": "...",
  "wake_id": "uuid",
  "emitted_at": "2026-02-26T10:00:00Z"
}
```

Maintenance trigger payload example:

```json
{
  "schema_version": 1,
  "kind": "maintenance_tick",
  "namespace": "factorial",
  "reason": "cron_schedule",
  "wake_id": "uuid",
  "emitted_at": "2026-02-26T10:00:00Z"
}
```

Wake payload rules:

- `schema_version` is mandatory for forward compatibility.
- `wake_id` is unique per emission for observability/correlation (not correctness).
- Unknown fields must be ignored by consumers.
- Unknown `kind` should be logged and ACKed (to avoid poison-loop retries).

## 8.3 Dispatch triggers

Call `WakeDispatch` after operations that make work runnable:

- enqueue task / enqueue batch / resume task
- steer task that wakes activity wait
- messaging direct/group send that wakes paused recipients
- hook resolve and hook expiry wake
- maintenance recovery that moves tasks to `queue_main`

## 8.4 Worker callback algorithm (concrete)

Per queue callback invocation:

1. Parse CloudEvent and message payload.
2. Validate payload schema and required fields.
3. Build `WorkerInvocationContext` (budget, redis client, orchestrator ref).
4. Execute `worker_tick(...)` with bounded limits:
   - stop on `max_runtime_s`, `max_batches`, or `max_tasks`.
5. Emit structured telemetry:
   - invocation id
   - wake id
   - agent name
   - processed count
   - duration
6. Return success so message is ACKed.
7. On transient infra failure (Redis/network):
   - return failure to trigger retry.
8. On permanent payload failure:
   - ACK (do not retry malformed payload forever).
   - emit `invalid_wake_payload` metric + structured log.
   - mirror payload/error to dead-letter diagnostics stream for debugging.

## 8.5 Cron callback algorithm (concrete)

Per cron invocation:

1. Enqueue one `maintenance_tick` message to dispatch topic and return quickly.
2. Do not run long maintenance loops inside cron handler.
3. If dispatch enqueue fails, return error (so cron failure is visible).

## 8.6 Maintenance execution algorithm (queue-driven)

Maintenance runs in worker callbacks when `kind=maintenance_tick`:

1. Acquire per-namespace maintenance lock (best-effort distributed mutex).
2. Run bounded maintenance sweep (`maintenance_tick`) across agents within budget.
3. Wake touched agents (`wake_agents`) when recovery makes tasks runnable.
4. If budget was exhausted or sweep appears saturated, enqueue `maintenance_tick` continuation.
5. Release lock and return success.

Why this is preferred over long cron-held execution:

- better fits serverless invocation model
- better backpressure control and continuation semantics
- avoids coupling recovery progress to one long cron invocation
- easier to tune with queue concurrency and budget limits

---

## 9) Reliability Model

## 9.1 Delivery and idempotency

- Vercel queue is at-least-once.
- Wake messages may duplicate.
- Safety comes from Redis/Lua atomic scripts:
  - duplicate ticks with empty queue are harmless
  - duplicate processing claims are prevented by pickup + status transitions

## 9.2 Worker timeouts

- Bounded `worker_tick` ensures invocation exits before Vercel timeout.
- Use conservative runtime budget:
  - `budget_s = max_duration_s - safety_margin_s`
  - default safety margin: 5s

## 9.3 Heartbeats in serverless

- Keep heartbeat writes during task execution as currently implemented.
- If invocation ends mid-turn, stale recovery path remains authoritative.

## 9.4 Maintenance correctness

Maintenance must enforce:

- stale heartbeat recovery
- scheduled wake recovery
- backoff recovery
- hook expiry wake
- TTL cleanup

Recommended strategy:

- cron acts as a trigger (enqueue `maintenance_tick`) rather than the long-running executor
- maintenance executes in queue-driven worker callbacks with bounded budgets and continuation messages
- keep cron at 1-minute cadence for freshness and as a safety heartbeat

Why not "cron every 5 minutes, run for ~10 minutes":

- it ties progress to one long-lived invocation and increases timeout/cold-start risk concentration
- it reduces fairness when many namespaces/agents need recovery
- retry behavior is coarser (you wait longer after failures)
- queue-driven continuation gives smoother, safer recovery throughput

## 9.5 Failure handling

- Worker callback failure:
  - return error to queue callback path so message retries
- Redis unavailable:
  - fail callback (retry later)
- Partial completion:
  - rely on existing script atomicity + stale recovery

## 9.6 Reliability invariants (must always hold)

1. A task reaches a terminal state at most once.
2. Duplicate wake delivery never creates duplicate task execution.
3. A crashed invocation eventually returns tasks to runnable state via maintenance recovery.
4. Scheduled and backoff waits eventually become runnable with bounded cron lag.
5. Parent tasks blocked on child jobs eventually resume when children complete.

Each invariant gets:

- at least one deterministic integration test
- at least one failure-injection test

## 9.7 Backpressure and overload behavior

- If Redis latency increases, worker ticks should process fewer tasks per invocation and exit within budget.
- If queue backlog spikes, scale via worker concurrency rather than unbounded per-invocation loops.
- If wake publish fails in control-plane operations:
  - operation still succeeds in Redis
  - metric/alarm raised
  - queue-driven maintenance continuations + cron heartbeat guarantee eventual progress

---

## 10) Developer Experience Plan

## 10.1 Library APIs for app authors

Provide stable helpers:

- `orchestrator.create_app(...)`
- `factorial.runtimes.vercel.create_worker(orchestrator, ...)`
- `orchestrator.run_maintenance_cron_tick()`
- `factorial.runtimes.vercel.VercelRuntimeSettings.from_env()`

App authors should only need:

- agent definitions
- one shared `orchestrator` module
- three tiny wrapper entrypoints

## 10.2 Wrapper pattern for app repos

```text
my-agent-app/
  agent.py
  orchestrator.py
  server.py
  chat.html
  vercel.json
```

## 10.3 Local development

Support three workflows:

1. process runtime (existing): local Redis + `orchestrator.run()`
2. hybrid local serverless simulation:
   - local web app
   - local queue callback invocation path
3. deployed Vercel integration testing in preview env

## 10.4 Minimal wrapper example

`orchestrator.py`:

```python
from agent import my_agent
from factorial import Orchestrator

orchestrator = Orchestrator()  # auto-selects vercel host when VERCEL=1
orchestrator.register_runner(agent=my_agent)
```

`orchestrator.py`:

```python
orchestrator = Orchestrator()
orchestrator.register_runner(agent=my_agent)
```

This is the intended DX: app repos import framework adapters and do not copy nfactorial internals.

---

## 11) Phased Implementation Plan

## Phase 0 - Preconditions and guardrails

- Add architecture tests asserting no behavior changes for current process runtime.
- Add benchmark baseline for:
  - enqueue to start latency
  - throughput
  - stale recovery time

Deliverable:

- baseline metrics and invariants documented.

Exit gate:

- No regression in existing integration suite.

## Phase 1 - Extract shared engine ticks

- Introduce `engine/worker_tick.py`.
- Move existing per-task processing and state machine into `engine`.
- Refactor process loops to call tick APIs.
- Ensure no behavior drift in integration tests.

Deliverable:

- process runtime still passing all tests, now using shared ticks.

Exit gate:

- Runtime parity tests pass in process mode with old vs new host wrappers.

## Phase 2 - Introduce `WakeDispatch`

- Add wake dispatch contract and noop implementation.
- Wire control-plane and recovery operations to call wake dispatch (noop by default).
- Add tests that wake calls are emitted where expected.

Deliverable:

- wake semantics represented in code paths without runtime coupling.

Exit gate:

- Wake emission coverage test matrix green for enqueue/resume/steer/messaging/hook/maintenance.

## Phase 3 - Add Vercel runtime adapters

- Implement:
  - `runtimes/vercel/worker_service.py`
  - `runtimes/vercel/cron_service.py`
  - `Orchestrator.create_app(...)`
  - `runtimes/vercel/wake_dispatcher.py`
- Integrate Vercel queue callback model.
- Implement bounded invocation budgets and settings.

Deliverable:

- runnable Vercel service adapters with unit tests.

Exit gate:

- Callback contract tests pass against a local Vercel worker callback harness.

## Phase 4 - Ship wrappers and example app

- Add first-party example under `examples/vercel_runtime/`.
- Include deployment wrappers and `vercel.json`.
- Document environment variables and setup.

Deliverable:

- end-to-end deployable sample on Vercel.

Exit gate:

- Sample app can complete multi-agent fanout + wait + resume scenarios in preview deployment.

## Phase 5 - Reliability hardening

- Add failure injection tests:
  - duplicate wake
  - callback retry
  - Redis transient errors
  - abrupt invocation termination
- Validate recovery SLA via cron cadence.

Deliverable:

- reliability report + recommended default limits.

Exit gate:

- Failure injection suite green with no invariant violations.

## Phase 6 - Rollout

- Mark feature as beta in one release.
- Keep process runtime default.
- Gather usage and error telemetry, then promote to stable.

Deliverable:

- stable dual runtime support.

Exit gate:

- Two consecutive releases with no Sev-1 runtime parity incidents.

## 11.1 Suggested execution cadence

Rough implementation cadence (single focused maintainer):

- Phase 0: 2-3 days
- Phase 1: 4-6 days
- Phase 2: 2-4 days
- Phase 3: 5-8 days
- Phase 4: 2-3 days
- Phase 5: 4-6 days
- Phase 6: 1-2 releases

Parallelization opportunities:

- one person on engine extraction
- one person on Vercel adapters/harness
- one person on docs/examples/quickstart

---

## 12) Test Strategy

## 12.1 Existing tests must continue passing

- all current integration tests for:
  - wait.activity
  - wait.sleep/cron
  - parent-child waits
  - messaging wake behavior
  - stale/backoff/scheduled recovery

## 12.2 New tests

- `tests/unit/runtime/test_worker_tick.py`
- `tests/unit/runtime/test_maintenance_tick.py`
- `tests/unit/runtime/test_wake_dispatch.py`
- `tests/integration/test_vercel_worker_callback.py`
- `tests/integration/test_vercel_cron_maintenance.py`
- `tests/integration/test_vercel_duplicate_wake_idempotency.py`

## 12.3 Contract tests

Add runtime parity tests:

- Process runtime and Vercel runtime produce equivalent final task outcomes for the same scenario matrix.

---

## 13) Configuration Matrix

Required env:

- `REDIS_HOST`
- `REDIS_PORT`
- `REDIS_DB`
- `OPENAI_API_KEY` (and others as needed)

Vercel runtime env:

- `VERCEL=1` (set automatically by Vercel; selects `vercel` host mode)
- `NFACTORIAL_WAKE_TRANSPORT=vercel_queue` (optional; default on Vercel)
- `NFACTORIAL_DISPATCH_TOPIC=nfactorial-dispatch`
- `NFACTORIAL_DISPATCH_CONSUMER=default`
- `NFACTORIAL_WORKER_MAX_BATCHES`
- `NFACTORIAL_WORKER_MAX_TASKS`
- `NFACTORIAL_WORKER_BUDGET_S`
- `NFACTORIAL_MAINTENANCE_BUDGET_S`
- `NFACTORIAL_CRON_INTERVAL_HINT_S`

Optional queue auth env (if needed by wrapper path):

- `VERCEL_QUEUE_TOKEN`
- `VERCEL_QUEUE_BASE_URL`
- `VERCEL_QUEUE_BASE_PATH`

---

## 14) Operational SLOs and Defaults

Initial SLO targets:

- P50 enqueue-to-start < 5s
- P95 enqueue-to-start < 20s
- stale processing recovery < 2 cron intervals
- no duplicate terminal transitions for same task ID

Initial defaults:

- worker callback max runtime budget: 20s
- maintenance callback max runtime budget: 20s
- cron frequency: 1 minute
- max batches per worker invocation: 5

Required telemetry to ship with runtime:

- counter: `factorial_wake_emitted_total{reason,agent}`
- counter: `factorial_wake_callback_total{status,agent}`
- histogram: `factorial_worker_tick_duration_ms{agent}`
- histogram: `factorial_maintenance_tick_duration_ms`
- gauge: `factorial_backlog_estimate{agent}`
- counter: `factorial_recovery_total{kind}`
- counter: `factorial_invariant_violation_total{type}`

---

## 15) Risks and Mitigations

1. **Wake storms from high fanout**
   - Mitigation: runtime-specific coalescing + rate limits (process: in-memory burst coalescing, Vercel: optional Redis TTL dedupe if metrics justify it).

2. **High cold start latency**
   - Mitigation: keep callbacks lightweight; lazy-load heavy clients where possible.

3. **Redis connection pressure from serverless burst**
   - Mitigation: connection pooling limits, bounded per-invocation work, sensible function concurrency settings.

4. **Cron lag impacts recovery latency**
   - Mitigation: keep maintenance idempotent and safe at higher frequencies; make interval configurable.

5. **DX confusion between app code and wrappers**
   - Mitigation: explicit docs + starter template + wrapper generators.

---

## 16) Implementation Checklist

- [ ] Phase 0 baseline metrics and invariants committed
- [ ] Shared `worker_tick` extracted and process runtime switched
- [ ] Shared `maintenance_tick` extracted and process runtime switched
- [ ] `WakeDispatch` contract + noop impl + call sites wired
- [ ] Vercel queue wake dispatcher implemented
- [ ] Vercel worker service adapter implemented
- [ ] Vercel cron service adapter implemented
- [ ] Web service adapter with control-plane endpoints implemented
- [ ] Example app with deployment wrappers added
- [ ] Runtime parity and failure-injection tests added
- [ ] Docs: app author quickstart + migration guide
- [ ] Beta release and rollout notes

---

## 17) "What it looks like to use"

App author writes:

1. `agents.py` with one or more `Agent(...)` definitions.
2. `orchestrator.py` that creates one shared `orchestrator` and calls `register_runner(...)`.
3. Tiny Vercel entrypoints:
   - `orchestrator.create_app(...)`
   - worker service entrypoint points to `orchestrator.py` (runtime calls `orchestrator.bootstrap_vercel_worker_app()` automatically)
   - cron service entrypoint points to `orchestrator.py` (the `if __name__ == "__main__"` block runs `orchestrator.run_maintenance_cron_tick()`)
4. `vercel.json` with services:
   - web
   - worker (`topic` + `consumer`)
   - cron (`schedule`)

Result:

- same nfactorial semantics
- no long-lived process requirement
- clear separation between framework code and deployment glue

---

## 18) Rollout and rollback plan

Rollout sequence:

1. Internal dogfood with one or two non-critical agents.
2. Preview deployments for selected users.
3. Beta release with clear opt-in docs.
4. Stable release after parity and incident-free burn-in.

Rollback policy:

- Immediate rollback trigger:
  - invariant violations
  - repeated missed recoveries
  - unexplained duplicate terminal transitions
- Rollback action:
  - shift traffic/workload back to non-Vercel process deployment
  - disable `vercel_queue` wake transport if needed
  - keep Redis state model unchanged so rollback is routing/configuration-driven

## 19) Open questions to resolve before coding starts

- Should `worker_tick` process only one agent per wake, or optionally opportunistically pull additional runnable agents?
- Wake coalescing placement: when many wakes for the same agent happen in a short window, should dedupe happen in app memory, Redis, or both?
- Poison-message observability: if malformed wake payloads are ACKed (not retried), what telemetry and diagnostics path is required so drops are visible and actionable?

Recommended default answers for MVP:

- one-agent-per-wake tick
- Vercel (`VERCEL=1`): SSE-first. Non-Vercel process deployments: SSE + optional WebSocket.
- Coalescing: process runtime uses in-memory burst coalescing; Vercel starts without global dedupe and adds Redis TTL dedupe only if wake-volume metrics justify it.
- Poison messages: ACK malformed payloads, increment `invalid_wake_payload` metrics, emit structured logs, and mirror to a dead-letter diagnostics stream.

## 20) Success Criteria

This plan is complete when all are true:

1. Existing process runtime remains stable and default.
2. Vercel runtime can run end-to-end workloads with parity in outcomes.
3. App authors can deploy with minimal wrappers and without copying internals.
4. Recovery and wait semantics remain reliable under duplicate queue deliveries and callback retries.
5. Maintainers can evolve runtime hosts without touching core state machine logic.
