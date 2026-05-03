FROM python:3.11-slim AS base

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPYCACHEPREFIX=/tmp/pycache

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies from the packaged local nfactorial wheel
COPY examples/code_agent/requirements.txt ./
COPY examples/code_agent/wheels ./wheels
RUN pip install --no-cache-dir -r requirements.txt

# Overlay local framework source for Docker-based repo development
COPY pyproject.toml README.md ./
COPY src ./src/
RUN pip install -e .

# Copy example code
COPY examples/code_agent/ ./

FROM base AS agent

# Expose the observability port
EXPOSE 8080

# Run the agent
CMD ["sh", "-c", "rm -rf /tmp/pycache && exec python orchestrator.py"]

FROM base AS server

# Expose the server port
EXPOSE 8000

# Run the server
CMD ["sh", "-c", "rm -rf /tmp/pycache && exec python server.py"]
