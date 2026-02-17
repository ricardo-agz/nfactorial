# Hooks, Tool Dependencies, and Waits (Design Proposal)

> Status: Proposed, revised after architecture and DX review.
>
> This revision keeps the API FastAPI-like while supporting robust multi-hook workflows:
>
> - signature-level hook dependencies via `Annotated[..., hook.requires(...)]` and `Annotated[..., hook.awaits(...)]`
> - typed `Hook` payloads (`Approval`, `CodeExecResult`, etc.)
> - implicit dependency ordering from DI signatures in request builders (no user DAG DSL)
> - internal hook-session runtime for correctness, idempotency, and resumption
> - plain returns for simple tools; Pydantic `BaseModel` + `Hidden` for model/client split
> - no `tool.ok/fail/error`, no `ToolResult`, no `FunctionTool` -- just Python types
> - wait helpers with clear queue semantics (`wait.sleep`, `wait.cron`, `subagents.spawn`, `wait.jobs`)

## Problem Statement

Current patterns around deferred and forking tools are powerful but can feel implicit:

- behavior is inferred from decorators and function attributes
- return-shape conventions (for example tuples) are not fully explicit in type contracts
- complex human-in-the-loop and external completion flows require custom wiring
- multi-hook workflows can become ad-hoc and error-prone

We want a design that is:

- explicit and type-safe
- ergonomic in common cases (plain returns for simple tools, typed models for complex ones)
- compatible with distributed execution (no fragile coroutine/frame persistence)
- suitable for multi-agent teams and long-running workflows
- as lightweight to learn as FastAPI, not a heavyweight workflow framework
- no implicit tuple conventions or framework-specific return constructors

## Design Goals

- One primary tool decorator (`@tool`) with signature-level hook dependencies as the canonical pattern.
- Hooks are first-class typed schemas (`class Approval(Hook): ...`).
- Request creation should be ergonomic (`Approval.pending(...)`).
- Multi-hook flows must be first-class without forcing a user-facing DAG DSL.
- Runtime-enforced correctness for approval/rejection and external completion.
- Strong idempotency, auth, and timeout semantics.
- Continuation logic runs on workers, not control-plane API handlers.

## Non-Goals

- Persisting Python execution frames/call stacks across workers.
- Building a full BPM/workflow language.
- Replacing queue reliability internals (Lua state machine remains central).
- Exposing internal hook-session graph machinery as mandatory user concepts.

## Proposed API

## Namespace and Import Conventions

Use namespace-first imports for clarity and to avoid global symbol pollution:

```python
from typing import Annotated
from factorial import tool, hook, wait, Hook, Hidden
```

Conventions:

- `tool` as a decorator (no `.ok`, `.fail`, or `.error` methods)
- `hook.*` for hook dependencies (`requires`, `awaits`)
- `wait.*` for non-hook scheduling primitives
- `Hook` as a typed base for hook payloads
- `Hidden` as a field annotation to exclude fields from model context

## Hook Types

```python
from factorial import Hook

class Approval(Hook):
    granted: bool
    reason: str = ""

class CodeExecResult(Hook):
    exit_code: int
    stdout: str
    stderr: str
```

## Hook Delivery Modes

The framework supports two explicit modes. The same tool/request code works in both.

- BYO mode (recommended default): application hosts public endpoints and calls
  `orchestrator.resolve_hook(...)`.
- Managed mode (optional convenience): orchestrator exposes hook submit APIs and creates
  `submit_url`.

Rationale: keep orchestrator private by default (dashboard/control plane are not public).
Applications usually already have auth/session infrastructure and should terminate
external callbacks at app routes.

## Framework vs Application Responsibilities

Framework-provided:

- `Hook` base type and schema validation
- `PendingHook[T]` ticket type
- hook/session persistence (`pending -> resolved/expired`)
- token generation, storage, and validation
- idempotent resolve-once semantics
- optional managed hook APIs (`submit_url`) for dev/small setups

Application-provided:

- hook payload classes (`Approval`, `CodeExecResult`, etc.)
- request builders (`request_code_exec_approval`, `submit_job`)
- transport UX (email/SMS/UI pages), unless using managed helper pages
- optional BYO webhook endpoints

## `PendingHook` Contract

`PendingHook` is framework-provided. Users do not define this class.

```python
class PendingHook(Generic[T]):
    hook_id: str
    submit_url: str | None      # present in managed mode
    token: str                  # always issued; required for resolve
    expires_at: datetime
    hook_type: type[T]
    title: str | None
    metadata: dict[str, Any]

    def auth_headers(self) -> dict[str, str]: ...
    def auth_query(self) -> dict[str, str]: ...
```

Semantics:

- `submit_url` is only present when orchestrator public hook API is enabled; it is
  derived from configured `public_base_url` + route template.
- `token` is a per-hook capability secret and is always required to resolve a hook.
- `auth_headers()` is a convenience helper (for example `Authorization: Bearer <token>`).
- token is stored server-side as hash/metadata (not plaintext) and validated on resolve.

## Token-Only Auth (v1)

To keep v1 simple and explicit, hook resolution uses one auth mechanism:
token required.

```python
# Token is always issued
approval_ticket = Approval.pending(ctx=ctx)

# Same model for awaited hooks
result_ticket = CodeExecResult.pending(ctx=ctx)
```

Notes:

- v1 does not expose per-hook token TTL or `token_once` flags on `pending(...)`.
- token lifetime follows hook lifetime (`expires_at`) by default.
- use `rotate_hook_token(...)` explicitly for resend/leak workflows.
- Your API can still require user login/authorization, but this is additive.
- Orchestrator `resolve_hook` still validates hook token as the core gate.
- Prefer sending token in headers; avoid query params.

## Pending Hook Creation

Request builders return typed pending tickets, not raw framework payloads:

```python
from factorial import HookRequestContext, PendingHook

async def request_code_exec_approval(
    ctx: HookRequestContext,
    code: str,  # auto-injected from the tool call args
) -> PendingHook[Approval]:
    ticket = Approval.pending(
        ctx=ctx,
        title="Approve code execution?",
        body=code[:500],
        channel="email",
        timeout_s=300,
        metadata={"action": "run_code"},
    )

    # Keep token out of URLs. Store it server-side keyed by hook_id.
    await hook_secret_store.put(
        hook_id=ticket.hook_id,
        token=ticket.token,
        expires_at=ticket.expires_at,
    )

    approval_page = f"{APP_BASE_URL}/approve-code?hook_id={ticket.hook_id}"

    await email_service.send(
        to=await user_email(ctx.owner_id),
        subject=ticket.title,
        html=f"""
          <pre>{code[:500]}</pre>
          <a href="{approval_page}">Review and decide</a>
        """,
    )
    return ticket
```

`code` is automatically injected by name from the tool-call arguments, so request
builders do not need to reach into `ctx.args[...]` for common cases.

## Canonical Style: Signature-Level Hook Dependencies

The primary pattern is FastAPI-like dependency injection via `Annotated`:

```python
class CodeExecOutput(BaseModel):
    summary: str
    stdout: Annotated[str, Hidden]
    stderr: Annotated[str, Hidden]

@tool
async def run_code(
    code: str,
    approval: Annotated[Approval, hook.requires(request_code_exec_approval)],
) -> str | CodeExecOutput:
    if not approval.granted:
        return f"Rejected: {approval.reason}"

    result = await sandbox.run(code)
    return CodeExecOutput(
        summary=f"Execution complete (exit={result.exit_code})",
        stdout=result.stdout,
        stderr=result.stderr,
    )
```

## Multi-Hook Without a User-Facing DAG DSL

Multi-hook is supported as a first-class use case, but users do not define graph objects.

```python
@tool
async def wire_transfer(
    amount: int,
    manager: Annotated[Approval, hook.requires(request_manager_approval)],
    finance: Annotated[Approval, hook.requires(request_finance_approval)],
) -> str:
    ...
```

### Key abstraction

Ordering is inferred from request-builder DI signatures:

- If a request builder needs another hook payload, that creates a dependency edge.
- If no dependency edge exists, hooks are independent and can be requested in the same stage.

Example:

```python
async def request_finance_approval(
    ctx: HookRequestContext,
    amount: int,
    manager: Approval,  # inferred edge: manager -> finance
) -> PendingHook[Approval]:
    ...
```

No explicit ordering API is required in v1. Ordering comes from DI dependencies in
request-builder signatures.

## Implicit Dependency Inference Rules

For each hook dependency parameter in a tool signature:

1. `hook.requires(...)` / `hook.awaits(...)` defines the request builder.
2. Request-builder parameters may be injected from:
   - `ctx: HookRequestContext`
   - tool args by name
   - previously resolved hook payloads by hook parameter name
   - normal Python defaults when optional parameters are declared
3. Hook references are name-based for disambiguation (`manager`, `finance`), not only type-based.
4. Type compatibility is validated at registration/startup.
5. Cycles fail fast at startup.

This keeps the external API simple while giving deterministic internal scheduling.

## Scheduling Semantics

Runtime scheduling is topological and stage-based:

- all ready nodes in a stage can be requested concurrently
- next stage starts when prior stage dependencies are resolved
- deterministic tie-breaks use tool signature order

Rationale:

- supports low-latency parallel independent approvals
- preserves strict ordering when dataflow creates edges
- avoids a separate concurrency mode API surface

## Decorator Sugar (Optional) - Not for V1

For simple one-hook cases, decorator aliases can exist:

```python
@tool.gated(Approval, request=request_code_exec_approval)
async def run_code(...): ...

@tool.awaits(CodeExecResult, request=submit_job)
async def run_code_external(...): ...
```

These are convenience wrappers over the signature-level dependency model.

## Tool Results

Tools return plain Python values. The framework handles serialization to the model
and client/event streams automatically.

### Design Principles

- No `tool.ok`, `tool.fail`, or `tool.error`. `tool` is only a decorator.
- No `ToolResult`, `FunctionTool`, `FunctionToolAction`, or `FunctionToolActionResult`.
- No implicit tuple conventions (`tuple[str, dict]`).
- Plain returns go to both model and client. `Hidden` fields on Pydantic models
  go only to the client.
- Failures are exceptions.

### Plain Returns (Simple Tools)

Return any serializable value (`str`, `dict`, `list`, `int`, etc.). The model and
client both receive the full serialized value.

```python
@tool
def think(thoughts: str) -> str:
    """Think about the task."""
    return thoughts

@tool
def execute_code(code: str) -> dict:
    result = sandbox.run(code)
    return {
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
```

### Typed Returns with `Hidden` (Model/Client Split)

When the client needs data that the model does not (e.g., full file contents for a
diff viewer, structured metadata for UI rendering), use a Pydantic `BaseModel` with
`Hidden`-annotated fields.

```python
from pydantic import BaseModel
from typing import Annotated
from factorial import Hidden

class EditResult(BaseModel):
    summary: str
    new_code: Annotated[str, Hidden]
    lines_changed: Annotated[int, Hidden]
```

The framework serializes the result in two ways:

- **Model context:** all fields *except* those annotated with `Hidden`. In the example
  above, the model sees `{"summary": "Replaced 'foo' with 'bar'"}`.
- **Client/event stream:** all fields including `Hidden`. The client receives the full
  `EditResult` with `summary`, `new_code`, and `lines_changed`.

Usage:

```python
@tool
def edit_code(
    find: str, replace: str, agent_ctx: IdeAgentContext,
) -> EditResult:
    agent_ctx.code = agent_ctx.code.replace(find, replace)
    return EditResult(
        summary=f"Replaced '{find}' with '{replace}'",
        new_code=agent_ctx.code,
        lines_changed=5,
    )
```

`Hidden` is a simple sentinel object. It works with standard `typing.Annotated` and
any Pydantic `BaseModel` -- no framework base class required.

### Failures

Raise exceptions. The framework catches them, formats an error message for the model,
and includes the error in the event stream.

```python
@tool
def fetch_user(user_id: str) -> dict:
    user = db.get(user_id)
    if not user:
        raise ValueError("User not found")
    return {"name": user["name"], "email": user["email"]}
```

- `raise` any exception for expected failures (not found, validation, rejection).
- `raise FatalAgentError(...)` to hard-stop the task without retry.
- Transient exceptions (network, rate limit) are retried automatically per retry policy.

### Wait Results

`wait.*` methods accept an optional `data` parameter with the same rules:

```python
# Simple -- string goes to model and client
return wait.sleep(30, data="Retrying shortly")

# Dict -- model and client both see it
return wait.jobs(jobs, data={"query_count": len(queries)})

# Typed with Hidden -- model sees non-hidden fields, client sees all
class ResearchProgress(BaseModel):
    status: str
    job_ids: Annotated[list[str], Hidden]

return wait.jobs(jobs, data=ResearchProgress(
    status="Researching...",
    job_ids=[j.task_id for j in jobs],
))

# No data -- framework generates a default message for the model
return wait.cron("0 * * * *")
```

### Serialization Rules Summary

| Return type | Model sees | Client/events see |
|---|---|---|
| `str`, `dict`, `list`, primitive | Full serialized value | Full serialized value |
| `BaseModel` (no `Hidden` fields) | All fields | All fields |
| `BaseModel` (with `Hidden` fields) | Non-hidden fields only | All fields |
| Exception (raised) | Error message | Error message + traceback |

### `Hidden` Implementation

`Hidden` is a sentinel that the framework checks via `typing.get_type_hints` with
`include_extras=True` on `BaseModel` return values:

```python
from factorial import Hidden  # just a sentinel object

# Framework logic (simplified):
def serialize_for_model(result: BaseModel) -> dict:
    hints = get_type_hints(type(result), include_extras=True)
    return {
        name: getattr(result, name)
        for name, hint in hints.items()
        if not _has_hidden_annotation(hint)
    }

def serialize_for_client(result: BaseModel) -> dict:
    return result.model_dump()
```

## Why No Mid-Function Freeze

Freezing Python execution mid-function and restoring from Redis is intentionally avoided.

Reasons:

- distributed workers do not reliably support stack-frame persistence
- brittle with async state and third-party I/O handles
- hard to debug and reason about failures/retries
- misaligned with current queue architecture (turn-based + persisted task state)

Instead, resumption happens at the tool-call boundary:

- first pass: create/update hook session and pending hooks, then park task
- callback resolves hook state
- task resumes and worker performs continuation
- tool body executes only once all required hook inputs are resolved

## Runtime Architecture

## Worker vs Control Plane Responsibilities

Control plane (`resolve_hook`) is state transition only:

- validate auth/idempotency
- atomically resolve hook
- trigger wake/requeue when stage complete

Worker is execution plane:

- requests pending hooks
- decides next stage
- injects resolved hook payloads
- runs actual tool continuation/body

This prevents heavy business logic from running in API handlers and preserves worker retry/timeout semantics.

## Hook Session Model

Each tool call with hook dependencies creates a session record:

- `session_id`
- `task_id`
- `tool_call_id`
- `tool_name`
- serialized original tool args
- node table keyed by hook param name:
  - mode (`requires`/`awaits`)
  - hook type
  - inferred `depends_on`
  - state (`unrequested`, `requested`, `resolved`, `expired`, `failed`)
  - `hook_id` and resolved payload (if available)
- bookkeeping (`in_flight_count`, timestamps)

Users do not manipulate sessions directly; this is an internal runtime model.

## Hook API Exposure Defaults

Default posture: keep orchestrator private and expose public hook routes from your app.

```python
from factorial import Orchestrator

orchestrator = Orchestrator(
    ...,
)
```

Optional managed mode (for simple deployments):

```python
orchestrator = Orchestrator(
    ...,
    hooks_public_base_url="https://api.myapp.com",
)
```

When `hooks_public_base_url` is set, framework can issue:

- `submit_url = https://api.myapp.com/hooks/{hook_id}/submit`
- `token` (per-hook capability secret)

When unset, `submit_url` is `None` and app endpoints call
`orchestrator.resolve_hook(...)` directly.

Token validation and idempotency checks are enforced by framework defaults in both modes.

## Managed Endpoint Contract

```http
POST /hooks/{hook_id}/submit
X-NFactorial-Hook-Token: <token>       # required
Idempotency-Key: <external_event_id>   # recommended
Content-Type: application/json

{ ... payload matching Hook schema ... }
```

## Resolution API

```python
await orchestrator.resolve_hook(
    hook_id=hook_id,
    payload={...},
    token=hook_token,
    idempotency_key=event_id,
)
```

Resolution steps:

1. load hook + session metadata
2. token validation (hash/version/expiry/revocation)
3. idempotency check
4. atomic resolve-once transition
5. payload schema validation against hook type
6. persist resolved payload on session node
7. if current stage complete, trigger resume/requeue

## Token Rotation

Pending hooks support token rotation for leaked credentials, resend flows, and long-lived
pending states.

```python
rotated = await orchestrator.rotate_hook_token(
    hook_id=hook_id,
    revoke_previous=True,  # optional grace window can be added later
)
```

By default, rotated tokens inherit the same hook expiry window.

## Redis/Lua State Architecture

## Compatibility with Existing Internals

This proposal reuses current pending tool-result mechanics for parking/requeue:

- pending sentinel storage
- parked queue status
- resume scripts that reactivate tasks when pending set is resolved

Core compatibility principle:

- keep task-level parking (`pending_tool_results`, `queue_pending`) for now
- store multi-hook dependency truth in hook-session records

## Redis Key Additions (Hooks)

Recommended additions:

- `HOOK_SESSION = "{namespace}:hook_session:{session_id}"`
- `HOOK_SESSION_BY_TOOL_CALL = "{namespace}:hook_session:by_tool_call:{task_id}:{tool_call_id}"`
- `HOOK_SESSIONS_BY_TASK = "{namespace}:hook_sessions:by_task:{task_id}"`
- `HOOK = "{namespace}:hook:{hook_id}"`
- `HOOKS_BY_SESSION = "{namespace}:hooks:by_session:{session_id}"`
- `HOOKS_BY_TASK = "{namespace}:hooks:by_task:{task_id}"`
- `HOOKS_EXPIRING = "{namespace}:hooks:expiring"` (zset)
- `HOOK_IDEMPOTENCY = "{namespace}:hooks:idem:{hook_id}:{idempotency_key}"`

## Pending Slot Contract

The existing pending slot for `tool_call_id` is used as:

- sentinel while parked
- resume envelope when a stage is ready to continue, e.g.:
  - `{"kind":"hook_session_resume","session_id":"..."}`

## Lua Contract Changes (Hooks)

Add a dedicated hook resolution script:

- validate token/idempotency/resolve-once atomically
- update hook and session node state
- indicate whether stage completion should wake task

Then reuse existing deferred completion resume path to requeue active tasks.

## Wait Helpers

Wait helpers are returned from tool calls to park execution and define when the task
should resume:

```python
@tool
async def monitor_release(...):
    if cooling_down:
        return wait.sleep(300, data="Cooling down")
    if awaiting_next_tick:
        return wait.cron("*/5 * * * *", tz="UTC", data="Waiting for next tick")
    jobs = await subagents.spawn(agent=search_agent, inputs=payloads, key="search")
    return wait.jobs(jobs)
```

State mapping:

- external waits (`hook.awaits`, deferred external completions):
  - keep pending path (`pending_tool_results` / pending-wait alias)
  - queue: existing `queue_pending`
- child waits (`wait.jobs` over jobs returned by `subagents.spawn`):
  - keep existing `pending_child_tasks` path in v1
- time waits (`wait.sleep`, `wait.cron`):
  - status: `paused`
  - queue: new `queue_scheduled` (`ZSET score = wake_timestamp`)

Recommended additions:

- `QUEUE_SCHEDULED = "{namespace}:queue:{agent}:scheduled"`
- `SCHEDULED_WAIT_META = "{namespace}:scheduled:{task_id}"`

Why not backoff for sleep/cron:

- backoff is retry policy semantics
- product waits must not mutate retry counters or failure telemetry

## End-to-End Scenarios

## A) Pure Multi-Gated (`manager` + `finance`)

1. Tool call starts hook session with nodes `manager`, `finance`.
2. Both nodes are ready (no inferred edge), so both requests can be dispatched in same stage.
3. Task parks awaiting stage completion.
4. Both hooks resolve.
5. Session stage completes, task requeued.
6. Worker resumes and executes tool with both injected payloads.

## B) Dataflow Chain (`manager -> finance -> bank_ack`)

```python
@tool
async def wire_transfer(
    amount: int,
    manager: Annotated[Approval, hook.requires(request_manager_approval)],
    finance: Annotated[Approval, hook.requires(request_finance_approval)],
    bank_ack: Annotated[BankAck, hook.awaits(submit_bank_transfer)],
) -> str:
    ...
```

With request builders:

- `request_finance_approval(..., manager: Approval, amount: int)` => edge `manager -> finance`
- `submit_bank_transfer(..., manager: Approval, finance: Approval, amount: int)` =>
  edges `manager -> bank_ack`, `finance -> bank_ack`

Stage progression:

1. stage 1: manager
2. stage 2: finance
3. stage 3: bank_ack
4. tool executes with all injected payloads

## C) Recommended Default BYO Callback Flow

1. App receives user/external callback.
2. App calls `orchestrator.resolve_hook(...)` with token + idempotency key.
3. Framework validates and updates hook/session state.
4. If stage complete, task resumes via existing queue path.
5. Worker continues execution.

## Security and Correctness Requirements

- token required for hook resolution
- app/session auth can be additive but does not replace hook token checks
- idempotency keys to prevent duplicate resolution
- atomic resolve-once semantics
- timeout/expiry handling in maintenance loop
- clear behavior for already-terminal tasks
- deterministic startup-time validation (including cycle detection)

Structured lifecycle events:

- `hook_session_started`
- `hook_requested`
- `hook_resolved`
- `hook_token_rotated`
- `hook_timed_out`
- `hook_session_completed`

## DX Conventions

- Canonical style is `Annotated[..., hook.requires/awaits(...)]`.
- Keep request builders small and focused.
- Encode ordering via DI data dependencies first.
- Rely on implicit DI ordering; avoid explicit ordering knobs in v1.
- `tool` is a decorator only. No `.ok()`, `.fail()`, or `.error()` methods.
- Return plain values for simple tools. Use `BaseModel` + `Hidden` when the client
  needs data that would be wasteful or redundant in the model context.
- Use exceptions for failures. Use `FatalAgentError` for unrecoverable errors.
- `wait.*` accepts `data=` with the same serialization rules as tool returns.

## Migration Plan

This is a new major version. No backwards compatibility with v1 or current v2 draft
is required. All deprecated aliases and legacy patterns are removed outright.

### Removals

- `tool.ok()`, `tool.fail()`, `tool.error()` -- `tool` is a decorator only
- `ToolResult`, `FunctionToolActionResult`, `FunctionToolActionReturn`
- `FunctionTool`, `FunctionToolAction`, `function_tool`
- `ToolNamespace` result builder methods
- implicit `tuple[str, Any]` return convention
- `output_str` / `output_data` fields on internal result objects

### Additions

- `Hidden` sentinel for `Annotated[T, Hidden]` field exclusion from model context
- `data=` parameter on `wait.sleep`, `wait.cron`, `wait.jobs`
- Pydantic `BaseModel` return type support with `Hidden`-aware serialization

### Implementation

Phase 1:

- implement `Hidden` sentinel and model/client serialization split in `agent.py`
- rewrite `tool_action` return normalization to handle `BaseModel` with `Hidden`
- rewrite `format_tool_result` / `extract_tool_call_result` for new contract
- remove `ToolNamespace.ok/fail/error`, `ToolResult`, and all deprecated aliases
- remove tuple return convention handling
- add `data=` parameter to `WaitNamespace.sleep/cron/jobs`

Phase 2:

- update all examples (`code_agent`, `multi_agent`) to new patterns
- update all docs
- update all tests

Phase 3:

- add `Hook`, `PendingHook`, `HookRequestContext`
- add `hook.requires(...)` and `hook.awaits(...)` with implicit DI dependency inference
- add internal hook-session runtime and persistence
- add `orchestrator.resolve_hook` and token rotation

## Open Questions

- Should wake-up happen on every hook resolution, or only stage completion?
- Do we keep `pending_tool_results` as compatibility naming in Redis, or alias to
  `pending_wait_results` in API/docs?
- Do we expose hook request templates in framework core or keep transport app-defined?

## Summary

This model keeps reliability guarantees of the current queue architecture while improving DX:

- plain returns for simple tools, Pydantic models for complex ones
- `Hidden` annotation for model/client split -- no framework base class required
- exceptions for failures instead of `tool.fail/error` constructors
- typed hooks with signature-level dependency injection
- implicit dependency ordering from DI signatures
- first-class multi-hook workflows without a user DAG DSL
- worker-only continuation (no control-plane execution leakage)
- strong callback correctness guarantees

The tool result contract is:

| Return type | Model sees | Client/events see |
|---|---|---|
| `str`, `dict`, `list`, primitive | Everything | Everything |
| `BaseModel` (no `Hidden` fields) | All fields | All fields |
| `BaseModel` (with `Hidden` fields) | Non-hidden fields only | All fields |
| Exception (raised) | Error message | Error + traceback |

One rule: `Annotated[T, Hidden]` excludes a field from the model. Everything else
is visible everywhere.

This gives a practical path for human-in-the-loop approvals, external webhooks, multi-gated
flows, and long-running team workflows with a FastAPI-like learning curve.
