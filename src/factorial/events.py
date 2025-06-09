from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any
import json
from redis.asyncio import Redis

from factorial.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BaseEvent:
    event_type: str
    task_id: str | None = None
    timestamp: datetime = datetime.now(timezone.utc)
    owner_id: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat()
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class QueueEvent(BaseEvent):
    agent_name: str | None = None
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
    def __init__(self, redis_client: Redis, channel: str):
        self.redis_client = redis_client
        self.channel = channel

    async def publish_event(self, event: BaseEvent) -> None:
        await self.redis_client.publish(self.channel, event.to_json())  # type: ignore
