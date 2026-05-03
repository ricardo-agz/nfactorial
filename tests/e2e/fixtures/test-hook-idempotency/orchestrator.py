from __future__ import annotations

import redis.asyncio as redis

from factorial import AgentWorkerConfig, Orchestrator

from .agents import idempotent_hook_agent


def _agent_worker_config() -> AgentWorkerConfig:
    return AgentWorkerConfig(
        workers=1,
        batch_size=10,
        max_retries=1,
        heartbeat_interval=1,
        missed_heartbeats_threshold=5,
        missed_heartbeats_grace_period=1,
        turn_timeout=30,
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
    orchestrator.register(
        idempotent_hook_agent,
        agent_worker_config=_agent_worker_config(),
    )
    return orchestrator
