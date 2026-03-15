from __future__ import annotations

from agent import parent_agent, researcher_agent, skeptic_agent, synthesizer_agent

from factorial import AgentWorkerConfig, Orchestrator

orchestrator = Orchestrator()

for agent in [parent_agent, researcher_agent, skeptic_agent, synthesizer_agent]:
    orchestrator.register(
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


if __name__ == "__main__":
    orchestrator.run()
