# Code Agent UI

This page explains the current frontend flow for the code-agent example in `examples/code_agent/ui`.

The current implementation is built around:

- SSE for runtime updates
- diff-based code acceptance in the browser
- hook resolution via `/api/resolve_hook`
- server-side execution in Vercel Sandbox after approval

## High-level flow

1. The user submits a prompt plus the current code buffer.
2. The backend enqueues the run with the latest code stored in typed state.
3. The UI listens to runtime events over SSE.
4. `edit_code` tool completions update the proposed diff shown in the editor.
5. `request_code_execution` emits a pending hook session.
6. The UI resolves that hook by calling `/api/resolve_hook`.
7. If approved, the backend executes the code server-side and streams the result back as tool output.

## Key files

- app shell: `examples/code_agent/ui/src/App.tsx`
- run submission: `examples/code_agent/ui/src/hooks/useChat.tsx`
- event handling: `examples/code_agent/ui/src/hooks/useWebSocket.tsx`
- API server: `examples/code_agent/server.py`
- agent definition: `examples/code_agent/agent.py`

## Event transport

The current UI uses server-sent events, not WebSockets:

```typescript
const stream = new EventSource(`${SSE_BASE}/${userId}`);
stream.onmessage = handleSSEMessage;
```

The server exposes:

- `GET /events/{user_id}`
- `GET /api/events/{user_id}`

Both stream the raw orchestrator update feed.

## Submitting runs

When the user sends a prompt, the UI posts:

- `user_id`
- `query`
- `code`
- `message_history`

to:

- `POST /enqueue`
- `POST /api/enqueue`

The backend stores `code` and `query` in the agent state and enqueues the run.

## Handling code edits

When `edit_code` completes, the UI reads `resp.client_output.new_code` from the tool result and opens a diff editor so the user can accept or reject the proposed change locally.

That means the code editor has two distinct approval surfaces:

- **edit acceptance** in the browser diff editor
- **execution approval** through a pending hook

## Handling execution approval

The current execution flow is hook-based.

When `request_code_execution` completes with a pending hook session, the UI extracts:

- `hook_id`
- `token`

from the returned hook session metadata and stores them on the pending execution action.

Accept and reject both call:

```typescript
await fetch(`${API_BASE}/resolve_hook`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    hook_id,
    token,
    approved: true,
    idempotency_key,
  }),
});
```

This replaces the older `complete_tool` flow.

## What happens after approval

After the hook is resolved with `approved: true`, the backend continues the parked tool and executes the code in Vercel Sandbox on the server:

```python
execution = await execute_code(agent_ctx.state.code)
```

The resulting stdout, stderr, exit code, and runtime metadata are returned as tool output and streamed back to the UI through the normal runtime event channel.

## Important API endpoints

- `POST /api/enqueue`
- `POST /api/cancel`
- `POST /api/resolve_hook`
- `GET /api/events/{user_id}`

## Why this architecture matters

This example shows how to combine three separate approval loops cleanly:

- agent-generated code edits proposed to the user
- explicit hook approval for execution
- server-side execution after approval, without trusting the browser as the execution environment

That makes it a good reference if you want Cursor-style approval UX backed by Factorial's hook runtime.
