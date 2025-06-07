import os
from agent.manager import (
    ControlPlane,
    AgentWorkerConfig,
    MaintenanceWorkerConfig,
    TaskTTLConfig,
    ObservabilityConfig,
    MetricsTimelineConfig,
)
from agent.agent import DummyAgent, FreeAgent

control_plane = ControlPlane(
    redis_host=os.getenv("REDIS_HOST", "localhost"),
    redis_port=6379,
    redis_db=0,
    redis_max_connections=1000,
    openai_api_key=os.getenv("OPENAI_API_KEY"),
    xai_api_key=os.getenv("XAI_API_KEY"),
    observability_config=ObservabilityConfig(
        enabled=True,
        host="0.0.0.0",
        port=8081,
        cors_origins=["*"],
    ),
    name="orchestrator",
)

# Default agent configuration
agent = DummyAgent()
# agent = FreeAgent(client=control_plane.llm_client)

control_plane.register_runner(
    agent=agent,
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

# Export the control plane and default agent for easy access
__all__ = ["control_plane", "agent"]

if __name__ == "__main__":
    control_plane.run()
