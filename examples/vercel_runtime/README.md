# Vercel Runtime Example

This example shows how to run nfactorial on Vercel services with:

- a web service (`server.py`)
- a queue worker service (`orchestrator.py` entrypoint)
- a cron maintenance trigger service (`orchestrator.py` entrypoint)
- a simple browser chat UI (`/chat`) to watch task/event progress

## Files

- `agent.py`: defines the agent(s).
- `orchestrator.py`: creates the shared orchestrator instance and serves as both worker and cron entrypoint.
- `server.py`: parent FastAPI app that mounts `orchestrator.create_app()` as a sub-app.
- `chat.html`: minimal frontend for enqueue + SSE progress.
- `vercel.json`: service definitions (web/worker/maintenance).

## Environment Variables

Set at least:

- `REDIS_HOST`
- `REDIS_PORT`
- `OPENAI_API_KEY`

Optional tuning:

- `NFACTORIAL_DISPATCH_TOPIC` (default: `nfactorial-dispatch`)
- `NFACTORIAL_DISPATCH_CONSUMER` (default: `default`)
- `NFACTORIAL_WORKER_MAX_BATCHES`
- `NFACTORIAL_WORKER_MAX_TASKS`
- `NFACTORIAL_WORKER_BUDGET_S`
- `NFACTORIAL_MAINTENANCE_BUDGET_S`

On Vercel, `VERCEL=1` is set automatically and nfactorial will auto-select the Vercel host mode.

## Deploy

Deploy from this directory so the root `vercel.json` is used:

```bash
cd examples/vercel_runtime
vercel
```

Open `/chat` after deploy to interact with the agent and inspect event/task progress.

The orchestrator API is mounted at `/orchestrator` in this example.
