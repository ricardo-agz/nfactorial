# Mafia Game Agent Example

This example demonstrates a complete social deduction game powered by nfactorial:

- one **game master** parent task orchestrating rounds
- multiple **AI player** child tasks
- optional **human player** joining via API/UI
- hidden-role gameplay with public + private channels
- explicit turn gating with `wait.until_signal(...)` and `subagents.signal(...)`
- structured vote/night-action collection via inbox payloads
- optional omniscient activity mode in UI for debugging and demos

## Project Structure

- `agents/` — Python game logic, agent definitions, orchestrator worker, API server
- `ui/` — React frontend
- `pyproject.toml` + `uv.lock` — single source of truth for Python dependencies
- root files (`Dockerfile`, `docker-compose.yml`, `vercel.json`) wire services together

## Rules (v1)

- Exactly **one werewolf**
- Everyone else is a villager
- **Day:** players discuss, then everyone votes one player out
- **Night:** werewolf chooses one target
- **Villagers win** when the werewolf is eliminated
- **Werewolf wins** when werewolf count reaches villager parity

## Quick Start

1. Configure environment:

```bash
export OPENAI_API_KEY=...
```

1. Run services:

```bash
cd examples/mafia_game_agent
docker-compose up
```

1. Open:

- UI: <http://localhost:5173>
- Dashboard: <http://localhost:8081>

## Manual Setup

### Prerequisites

- Python 3.12+
- Node.js 18+
- Redis

### Install and run

```bash
cd examples/mafia_game_agent

# Python deps (uses local wheel configured in pyproject.toml)
uv sync --active --no-dev --link-mode copy --frozen --no-editable

# UI deps
cd ui
npm install
cd ..

# Start Redis (separate terminal)
redis-server

# Start worker + API (separate terminals)
python agents/orchestrator.py
python agents/server.py

# Start UI
cd ui && npm run dev
```

## API Endpoints Used by UI

- `POST /api/enqueue` — start a game
- `GET /api/events/{user_id}` — stream game events (SSE)
- `POST /api/games/{task_id}/chat` — human sends public/wolf chat
- `POST /api/games/{task_id}/vote` — human day vote
- `POST /api/games/{task_id}/night_action` — human night action
- `POST /api/cancel` — cancel game task

## What to Observe

- AI players only act when explicitly signaled by the game master.
- Day and night phases are visible as system announcements in the thread view.
- Votes and night actions are submitted as structured payloads, not prompt-parsed text.
- Toggling omniscient mode reveals hidden roles and deeper activity traces.
