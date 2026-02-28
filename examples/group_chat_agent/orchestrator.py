from __future__ import annotations

import asyncio
import json
import os
import sys

from agent import parent_agent, researcher_agent, skeptic_agent, synthesizer_agent

from factorial import AgentWorkerConfig, ObservabilityConfig, Orchestrator

orchestrator = Orchestrator(
    redis_host=os.getenv("REDIS_HOST", "localhost"),
    redis_port=int(os.getenv("REDIS_PORT", 6379)),
    redis_db=int(os.getenv("REDIS_DB", 0)),
    redis_max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", 1000)),
    openai_api_key=os.getenv("OPENAI_API_KEY"),
    observability_config=ObservabilityConfig(
        enabled=True,
        host="0.0.0.0",
        port=8081,
        cors_origins=["*"],
    ),
)

for agent in [parent_agent, researcher_agent, skeptic_agent, synthesizer_agent]:
    orchestrator.register_runner(
        agent=agent,
        agent_worker_config=AgentWorkerConfig(
            workers=20,
            batch_size=10,
            max_retries=3,
            heartbeat_interval=2,
            missed_heartbeats_threshold=3,
            missed_heartbeats_grace_period=1,
            turn_timeout=120,
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
