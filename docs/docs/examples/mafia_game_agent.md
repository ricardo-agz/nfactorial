# Mafia Game Agent

This example shows a full social-deduction workflow built on Factorial's queued runtime.

## What it demonstrates

- one parent game-master task coordinating the whole game
- multiple child player tasks
- optional human participation through an API/UI
- public and private channels
- explicit phase gating with `wait.until_signal(...)`
- structured vote and night-action payloads delivered through messaging and inbox flows

## Why this example matters

This is the richest example in the repo for:

- signals
- runtime messaging
- inbox-driven structured actions
- human control-plane messaging
- mixed agent and human participation in the same workflow

## Key files

- game master: `examples/mafia_game_agent/agents/game_master.py`
- player logic: `examples/mafia_game_agent/agents/players.py`
- API server: `examples/mafia_game_agent/agents/server.py`
- UI: `examples/mafia_game_agent/ui`

## Runtime shape

The game master controls the phase transitions and wakes player agents with signals such as day-discussion, vote-open, and night-action transitions. Players react to those signals, send structured messages back to the parent, and coordinate over public or werewolf-only group channels.

## API endpoints used by the UI

- `POST /api/enqueue` to start a game
- `GET /api/events/{user_id}` to stream runtime events
- `POST /api/games/{task_id}/chat` for human public or werewolf chat
- `POST /api/games/{task_id}/vote` for human day votes
- `POST /api/games/{task_id}/night_action` for human night actions
- `POST /api/cancel` to cancel the game

## Run locally

```bash
cd examples/mafia_game_agent
export OPENAI_API_KEY=...
docker-compose up
```

Open:

- UI: [http://localhost:5173](http://localhost:5173)
- Dashboard: [http://localhost:8081](http://localhost:8081)

## Manual setup

```bash
cd examples/mafia_game_agent

# Python deps
uv sync --active --no-dev --link-mode copy --frozen --no-editable

# UI deps
cd ui
npm install
cd ..

# Start Redis
redis-server

# Start worker + API
python agents/orchestrator.py
python agents/server.py

# Start UI
cd ui && npm run dev
```
