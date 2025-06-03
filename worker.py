import os

from agent.manager import (
    ControlPlane,
    AgentWorkerConfig,
    MaintenanceWorkerConfig,
    TaskTTLConfig,
)
from agent.agent import DummyAgent, FreeAgent
from example_agents.video_gen_agent import VideoGenAgent

if __name__ == "__main__":
    control_plane = ControlPlane(
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=6379,
        redis_db=0,
        redis_max_connections=50,
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        xai_api_key=os.getenv("XAI_API_KEY"),
    )

    agent = DummyAgent(client=control_plane.llm_client)
    # agent = FreeAgent(client=control_plane.llm_client)
    # agent = VideoGenAgent(client=control_plane.llm_client)

    control_plane.register_runner(
        agent=agent,
        agent_worker_config=AgentWorkerConfig(
            workers=100,
            batch_size=25,
            max_retries=3,
            heartbeat_interval=2,
            missed_heartbeats_threshold=3,
            missed_heartbeats_grace_period=1,
            turn_timeout=90,
        ),
        maintenance_worker_config=MaintenanceWorkerConfig(
            workers=1,
            interval=5,
            task_ttl=TaskTTLConfig(
                failed_ttl=90,
                completed_ttl=60,
                cancelled_ttl=30,
            ),
        ),
    )

    control_plane.run()
