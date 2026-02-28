from __future__ import annotations

import asyncio
import json
import os
import sys

import redis.asyncio as redis
from agent import ide_agent

from factorial import AgentWorkerConfig, Orchestrator


def _build_redis_pool() -> redis.ConnectionPool:
    max_connections = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
    redis_url = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
    if redis_url:
        if not redis_url.startswith(("redis://", "rediss://")):
            raise RuntimeError(
                "REDIS_URL must be a redis:// or rediss:// connection string. "
                "REST endpoints are not supported by redis-py."
            )
        return redis.ConnectionPool.from_url(
            redis_url,
            max_connections=max_connections,
        )
    if os.getenv("VERCEL") == "1":
        raise RuntimeError(
            "Missing REDIS_URL on Vercel. Set REDIS_URL (or UPSTASH_REDIS_URL) "
            "to a redis:// or rediss:// connection string."
        )
    return redis.ConnectionPool(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        max_connections=max_connections,
    )

orchestrator = Orchestrator(
    redis_pool=_build_redis_pool(),
    openai_api_key=os.getenv("OPENAI_API_KEY"),
)

orchestrator.register_runner(
    agent=ide_agent,
    agent_worker_config=AgentWorkerConfig(
        workers=20,
        batch_size=15,
        max_retries=5,
    ),
)


def main() -> None:
    if os.getenv("VERCEL_SERVICE_TYPE") == "cron":
        result = asyncio.run(orchestrator.run_maintenance_cron_tick())
        print(json.dumps(result), file=sys.stderr)
        return
    orchestrator.run()


if __name__ == "__main__":
    main()
