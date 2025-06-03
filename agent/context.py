from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
import typing
import json
import uuid
from enum import Enum
from typing import TypeVar, Generic, Type

ContextType = TypeVar("ContextType", bound="AgentContext")


@dataclass
class AgentContext:
    query: str
    messages: list[dict[str, typing.Any]]
    turn: int

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, typing.Any]):
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str):
        return cls.from_dict(json.loads(json_str))


class TaskStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    PENDING_TOOL_RESULTS = "pending_tool_results"
    PENDING_EXTERNAL_INPUT = "pending_external_input"


@dataclass
class Task(Generic[ContextType]):
    id: str
    owner_id: str
    payload: ContextType
    agent: str
    retries: int
    status: TaskStatus
    created_at: datetime = datetime.now(timezone.utc)

    @classmethod
    def create(
        cls, owner_id: str, agent: str, payload: ContextType
    ) -> "Task[ContextType]":
        return Task(
            id=str(uuid.uuid4()),
            owner_id=owner_id,
            agent=agent,
            payload=payload,
            retries=0,
            status=TaskStatus.QUEUED,
        )

    def to_dict(self):
        return {
            "id": self.id,
            "owner_id": self.owner_id,
            "agent": self.agent,
            "payload": self.payload.to_dict() if self.payload else None,
            "retries": self.retries,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(
        cls,
        data: dict[str, typing.Any],
        context_class: Type[ContextType],
    ):
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        data["status"] = TaskStatus(data["status"])

        # Handle payload which might be a JSON string
        if data["payload"]:
            if isinstance(data["payload"], str):
                # Parse JSON string to dict first
                payload_dict = json.loads(data["payload"])
                data["payload"] = context_class.from_dict(payload_dict)
            elif isinstance(data["payload"], bytes):
                # Decode bytes to string, then parse JSON
                payload_str = data["payload"].decode("utf-8")
                payload_dict = json.loads(payload_str)
                data["payload"] = context_class.from_dict(payload_dict)
            else:
                # Already a dict
                data["payload"] = context_class.from_dict(data["payload"])
        else:
            data["payload"] = None

        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str, context_class: Type[ContextType]):
        data = json.loads(json_str)
        return cls.from_dict(data, context_class)
