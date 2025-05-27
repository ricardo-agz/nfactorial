from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any
import json
from redis.asyncio import Redis


class EventType(str, Enum):
    # Queue Run-level events
    RUN_STARTED = "run_started"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"

    # Queue Task-level events
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_RETRIED = "task_retried"
    TASK_FAILED = "task_failed"

    # Agent events
    TURN_STARTED = "turn_started"
    TURN_COMPLETED = "turn_completed"
    TURN_FAILED = "turn_failed"
    TOOL_CALLED = "tool_called"
    TOOL_RESULT = "tool_result"
    AGENT_OUTPUT = "agent_output"

    # LLM interactions
    LLM_REQUEST_STARTED = "llm_request_started"
    LLM_RESPONSE_RECEIVED = "llm_response_received"
    LLM_REQUEST_FAILED = "llm_request_failed"
    LLM_REQUEST_RETRIED = "llm_request_retried"

    # Tool execution
    TOOL_CALL_STARTED = "tool_call_started"
    TOOL_CALL_COMPLETED = "tool_call_completed"
    TOOL_CALL_FAILED = "tool_call_failed"
    TOOL_CALL_RETRIED = "tool_call_retried"


@dataclass
class BaseEvent:
    event_type: EventType
    task_id: str | None = None
    timestamp: datetime = datetime.now(timezone.utc)
    owner_id: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat()
        result["event_type"] = self.event_type.value
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class QueueEvent(BaseEvent):
    worker_id: str | None = None
    batch_id: str | None = None
    error: str | None = None


@dataclass
class AgentEvent(BaseEvent):
    agent_name: str | None = None
    turn: int | None = None
    data: dict[str, Any] | None = None
    error: str | None = None


class EventPublisher:
    def __init__(
        self, redis_client: Redis, channel_template: str = "updates:{owner_id}"
    ):
        self.redis_client = redis_client
        self.channel_template = channel_template

    async def publish_event(self, event: BaseEvent) -> None:
        channel = self.channel_template.format(owner_id=event.owner_id)
        await self.redis_client.publish(channel, event.to_json())  # type: ignore
