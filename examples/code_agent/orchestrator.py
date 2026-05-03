from __future__ import annotations

from agent import ide_agent

from factorial import AgentWorkerConfig, Orchestrator

orchestrator = Orchestrator()

orchestrator.register(
    agent=ide_agent,
    agent_worker_config=AgentWorkerConfig(
        workers=20,
        batch_size=15,
        max_retries=5,
    ),
)


if __name__ == "__main__":
    orchestrator.run()
