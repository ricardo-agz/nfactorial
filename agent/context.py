from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
import typing
import json
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


@dataclass
class Task(Generic[ContextType]):
    id: str
    owner_id: str
    payload: ContextType
    retries: int
    created_at: datetime = datetime.now(timezone.utc)

    def to_dict(self):
        return {
            "id": self.id,
            "owner_id": self.owner_id,
            "payload": self.payload.to_dict() if self.payload else None,
            "retries": self.retries,
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
        data["payload"] = (
            context_class.from_dict(data["payload"]) if data["payload"] else None
        )
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str, context_class: Type[ContextType]):
        data = json.loads(json_str)
        return cls.from_dict(data, context_class)
