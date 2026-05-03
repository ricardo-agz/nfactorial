from __future__ import annotations

import redis.asyncio as redis

from factorial import AgentWorkerConfig, Orchestrator

from .agents import (
    direct_listener_agent,
    direct_messaging_parent_agent,
    group_listener_agent,
    group_messaging_parent_agent,
)


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


def _register_agent(orchestrator: Orchestrator, agent: object) -> None:
    orchestrator.register(
        agent,
        agent_worker_config=_agent_worker_config(),
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
    _register_agent(orchestrator, direct_listener_agent)
    _register_agent(orchestrator, group_listener_agent)
    _register_agent(orchestrator, direct_messaging_parent_agent)
    _register_agent(orchestrator, group_messaging_parent_agent)
    return orchestrator
