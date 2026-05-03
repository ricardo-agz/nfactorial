# Basic Agent Example

An IDE-based agent that can write and edit code and request execution approval.
When approved, code is executed server-side in Vercel Sandbox.

## Quick Start with Docker

1. Set up environment:
```bash
cp .env.template .env
# Add your API keys to .env (OpenAI + Vercel Sandbox credentials)
```

2. Run everything:
```bash
docker-compose up
```

3. Open:
- UI: http://localhost:5173
- Dashboard: http://localhost:8081

## Manual Setup

### Prerequisites
- Python 3.9+, Node.js 18+, Redis
- Vercel Sandbox credentials (`VERCEL_TOKEN` or `VERCEL_OIDC_TOKEN`, plus
  `VERCEL_PROJECT_ID` and `VERCEL_TEAM_ID`)

### Install & Run
```bash
# 1. Set up .env file
cp .env.template .env
# Set your OpenAI and Vercel Sandbox credentials

# 2. Install Python Dependencies
pip install -r requirements.txt
pip install -e .

# 3. Install UI Dependencies
cd ui
npm install

# 4. Start Redis
redis-server

# 5. Run components (separate terminals):
python agent.py    # Agent workers
python server.py   # API server  
cd ui && npm run dev  # UI
```

### URLs
- UI: http://localhost:5173
- Dashboard: http://localhost:8081