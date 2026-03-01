from __future__ import annotations

import os

from agent import basic_agent, search_agent

from factorial import (
    AgentWorkerConfig,
    MaintenanceWorkerConfig,
    MetricsTimelineConfig,
    ObservabilityConfig,
    Orchestrator,
    TaskTTLConfig,
)

orchestrator = Orchestrator(
    redis_host=os.getenv("REDIS_HOST", "localhost"),
    redis_port=int(os.getenv("REDIS_PORT", 6379)),
    redis_db=int(os.getenv("REDIS_DB", 0)),
    redis_max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", 1000)),
    observability_config=ObservabilityConfig(
        enabled=True,
        host="0.0.0.0",
        port=8081,
        cors_origins=["*"],
    ),
)

orchestrator.register_runner(
    agent=search_agent,
    agent_worker_config=AgentWorkerConfig(
        workers=25,
        batch_size=15,
        max_retries=3,
        heartbeat_interval=2,
        missed_heartbeats_threshold=3,
        missed_heartbeats_grace_period=1,
        turn_timeout=60,
    ),
)

orchestrator.register_runner(
    agent=basic_agent,
    agent_worker_config=AgentWorkerConfig(
        workers=50,
        batch_size=15,
        max_retries=5,
        heartbeat_interval=2,
        missed_heartbeats_threshold=3,
        missed_heartbeats_grace_period=1,
        turn_timeout=120,
    ),
    maintenance_worker_config=MaintenanceWorkerConfig(
        workers=5,
        interval=5,
        task_ttl=TaskTTLConfig(
            failed_ttl=1800,
            completed_ttl=60,
            cancelled_ttl=30,
        ),
        metrics_timeline=MetricsTimelineConfig(
            timeline_duration=3600,  # 1 hour
            bucket_size="minutes",
            retention_multiplier=2.0,
        ),
    ),
)


def main() -> None:
    orchestrator.run()


if __name__ == "__main__":
    main()
