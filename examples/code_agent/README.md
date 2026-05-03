# Code Agent Example

An IDE-based agent that can write and edit code and request execution approval.
When approved, code is executed server-side in Vercel Sandbox.

## Quick Start with Docker

1. Set up environment:

```bash
# Option A: export in your shell
export OPENAI_API_KEY=...
export VERCEL_TOKEN=...
export VERCEL_PROJECT_ID=...
export VERCEL_TEAM_ID=...

# Option B: create a `.env` file next to docker-compose.yml with:
# OPENAI_API_KEY=...
# VERCEL_TOKEN=...
# VERCEL_PROJECT_ID=...
# VERCEL_TEAM_ID=...
```

1. Run everything:

```bash
docker-compose up
```

1. Open:

- UI: <http://localhost:5173>
- Dashboard: <http://localhost:8081>

## Manual Setup

### Prerequisites

- Python 3.9+, Node.js 18+, Redis
- Vercel Sandbox credentials (`VERCEL_TOKEN` or `VERCEL_OIDC_TOKEN`, plus
  `VERCEL_PROJECT_ID` and `VERCEL_TEAM_ID`)

### Install & Run

```bash
# 1. Set up environment
# Either export values in your shell or create a `.env` file in this directory:
# OPENAI_API_KEY=...
# VERCEL_TOKEN=...
# VERCEL_PROJECT_ID=...
# VERCEL_TEAM_ID=...

# 2. Install Python Dependencies
pip install -r requirements.txt
pip install -e ../../

# 3. Install UI Dependencies
cd ui
npm install

# 4. Start Redis
redis-server

# 5. Run components (separate terminals):
python orchestrator.py    # Agent workers
python server.py   # API server  
cd ui && npm run dev  # UI
```

### URLs

- UI: <http://localhost:5173>
- Dashboard: <http://localhost:8081>

## Notes

- The UI resolves execution approvals through `/api/resolve_hook`.
- Approved code executes server-side in Vercel Sandbox, not in the browser.
