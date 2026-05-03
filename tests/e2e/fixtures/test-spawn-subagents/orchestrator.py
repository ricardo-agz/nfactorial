from __future__ import annotations

import redis.asyncio as redis

from factorial import Orchestrator

from .agents import child_agent, parent_agent


def build_orchestrator(
    *,
    redis_pool: redis.ConnectionPool | None = None,
    namespace: str | None = None,
) -> Orchestrator:
    orchestrator = Orchestrator(
        redis_pool=redis_pool,
        namespace=namespace,
    )
    orchestrator.register(child_agent)
    orchestrator.register(parent_agent)
    return orchestrator
