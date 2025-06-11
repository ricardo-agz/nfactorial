from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
import typing
import uuid
import json
from typing import Generic, Type, Any, cast, TypeVar

from factorial.utils import decode
from factorial.context import AgentContext


ContextType = TypeVar("ContextType", bound="AgentContext")


class TaskStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    PENDING_TOOL_RESULTS = "pending_tool_results"
    BACKOFF = "backoff"


@dataclass
class TaskMetadata:
    owner_id: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "owner_id": self.owner_id,
            "created_at": self.created_at.timestamp(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, typing.Any]):
        data["created_at"] = datetime.fromtimestamp(
            float(data["created_at"]), tz=timezone.utc
        )
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str | bytes):
        return cls.from_dict(json.loads(decode(json_str)))


@dataclass
class Task(Generic[ContextType]):
    status: TaskStatus
    agent: str
    payload: ContextType
    metadata: TaskMetadata
    pickups: int = 0
    retries: int = 0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def create(
        cls, owner_id: str, agent: str, payload: ContextType
    ) -> "Task[ContextType]":
        return Task(
            status=TaskStatus.QUEUED,
            agent=agent,
            payload=payload,
            metadata=TaskMetadata(
                owner_id=owner_id,
            ),
        )

    def to_dict(self):
        return {
            "id": self.id,
            "status": self.status.value,
            "agent": self.agent,
            "payload": self.payload.to_dict() if self.payload else None,
            "pickups": self.pickups,
            "retries": self.retries,
            "metadata": self.metadata.to_dict(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        context_class: Type[ContextType],
    ) -> "Task[ContextType]":
        data["status"] = TaskStatus(data["status"])
        data["metadata"] = TaskMetadata.from_dict(data["metadata"])

        if data["payload"]:
            if isinstance(data["payload"], dict):
                data["payload"] = context_class.from_dict(
                    cast(dict[str, Any], data["payload"])
                )
            else:
                payload_str = decode(data["payload"])
                data["payload"] = context_class.from_dict(json.loads(payload_str))
        else:
            data["payload"] = None

        return cls(**data)

    @classmethod
    def from_json(
        cls, json_str: str | bytes, context_class: Type[ContextType]
    ) -> "Task[ContextType]":
        data = json.loads(decode(json_str))
        return cls.from_dict(data, context_class)
