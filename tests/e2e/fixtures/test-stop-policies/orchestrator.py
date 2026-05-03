from __future__ import annotations

import redis.asyncio as redis

from factorial import Orchestrator

from .agents import (
    composite_all_of_stop_agent,
    finish_tool_stop_agent,
    turn_limit_failure_agent,
)


def build_orchestrator(
    *,
    redis_pool: redis.ConnectionPool | None = None,
    namespace: str | None = None,
) -> Orchestrator:
    orchestrator = Orchestrator(
        redis_pool=redis_pool,
        namespace=namespace,
    )
    orchestrator.register(finish_tool_stop_agent)
    orchestrator.register(turn_limit_failure_agent)
    orchestrator.register(composite_all_of_stop_agent)
    return orchestrator
