FROM python:3.11-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src/
RUN pip install -e .

COPY examples/group_chat_agent/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY examples/group_chat_agent/ ./

FROM base AS agent

EXPOSE 8081

CMD ["python", "orchestrator.py"]

FROM base AS server

EXPOSE 8000

CMD ["python", "server.py"]
