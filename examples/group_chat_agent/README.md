# Group Chat Agent Example

This example demonstrates:

- a **parent agent** spawning multiple subagents
- a **shared group channel** used by all agents
- **direct messages** between pairs of subagents
- `wait.activity` pause/resume behavior
- a UI that clearly shows hierarchy + threads

## Quick Start with Docker

1. Configure environment:

```bash
export OPENAI_API_KEY=...
```

1. Run everything:

```bash
docker-compose up
```

1. Open:

- UI: <http://localhost:5173>
- Dashboard: <http://localhost:8081>

## Deploy on Vercel

This example supports both runtime modes:

- **Process mode (local):** `docker-compose up`
- **Serverless mode (Vercel):** `vercel.json` + queue worker

### Required environment variables (Vercel Project)

- `OPENAI_API_KEY`
- `REDIS_HOST`
- `REDIS_PORT`
- `REDIS_DB`
- `REDIS_MAX_CONNECTIONS` (optional; defaults to `1000`)

### Deploy

```bash
cd examples/group_chat_agent
vercel
```

Service wiring in `vercel.json`:

- `ui` -> `ui`
- `api` -> `server.py`
- `worker` -> `orchestrator.py` (Vercel Queue consumer + self-renewing maintenance heartbeat)

## Manual Setup

### Prerequisites

- Python 3.9+
- Node.js 18+
- Redis

### Install and Run

```bash
# Install Python dependencies
pip install -r requirements.txt
pip install -e ../../

# Install UI dependencies
cd ui
npm install

# Start Redis (separate terminal)
redis-server

# Run worker and API (separate terminals)
python orchestrator.py
python server.py

# Run UI
cd ui && npm run dev
```

## What to Observe

- Parent creates team and group channel.
- Subagents post to `#team_room`.
- DM threads appear between agent pairs, including optional parent <-> subagent DMs.
- Nodes enter `wait.activity` and later wake.
- Click an agent in the hierarchy to inspect its individual trace timeline
  (tool starts/completions, wait enter/wake, sent/received messages).
- Parent finishes with a final synthesis message.
