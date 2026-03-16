FROM python:3.12-slim AS base

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPYCACHEPREFIX=/tmp/pycache

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv

# Install app deps from the example lockfile (including local nfactorial wheel)
# into system site-packages so `python ...` works directly at runtime.
COPY examples/mafia_game_agent/pyproject.toml examples/mafia_game_agent/uv.lock ./
COPY examples/mafia_game_agent/wheels ./wheels
RUN uv export \
    --frozen \
    --no-dev \
    --no-editable \
    --format requirements.txt \
    --output-file requirements.lock \
    && pip install --no-cache-dir -r requirements.lock

COPY examples/mafia_game_agent/ ./

FROM base AS agent

EXPOSE 8081

CMD ["sh", "-c", "rm -rf /tmp/pycache && exec python agents/orchestrator.py"]

FROM base AS server

EXPOSE 8000

CMD ["sh", "-c", "rm -rf /tmp/pycache && exec python agents/server.py"]
