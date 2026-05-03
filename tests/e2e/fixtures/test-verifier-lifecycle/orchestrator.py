from __future__ import annotations

import redis.asyncio as redis

from factorial import Orchestrator

from .agents import verification_failure_agent, verification_retry_agent


def build_orchestrator(
    *,
    redis_pool: redis.ConnectionPool | None = None,
    namespace: str | None = None,
) -> Orchestrator:
    orchestrator = Orchestrator(
        redis_pool=redis_pool,
        namespace=namespace,
    )
    orchestrator.register(verification_retry_agent)
    orchestrator.register(verification_failure_agent)
    return orchestrator
