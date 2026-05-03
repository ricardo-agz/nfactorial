from __future__ import annotations

from agents import mafia_game_master_agent, mafia_player_agent
from factorial import AgentWorkerConfig, MaintenanceWorkerConfig, Orchestrator

orchestrator = Orchestrator()

for agent in [mafia_game_master_agent, mafia_player_agent]:
    orchestrator.register(
        agent=agent,
        agent_worker_config=AgentWorkerConfig(
            workers=24,
            batch_size=12,
            max_retries=3,
            heartbeat_interval=2,
            missed_heartbeats_threshold=3,
            missed_heartbeats_grace_period=1,
            turn_timeout=120,
        ),
        maintenance_worker_config=MaintenanceWorkerConfig(
            interval=2,
        ),
    )


if __name__ == "__main__":
    orchestrator.run()
