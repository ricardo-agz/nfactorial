from dataclasses import dataclass, asdict
from datetime import datetime
import typing
import json


@dataclass
class AgentContext:
    query: str
    messages: list[dict[str, typing.Any]]
    turn: int

    def to_dict(self):
        return asdict(self)


@dataclass
class Task:
    id: str
    user_id: str
    payload: AgentContext
    retries: int
    created_at: datetime

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "payload": self.payload.to_dict() if self.payload else None,
            "retries": self.retries,
            "created_at": self.created_at.isoformat(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, typing.Any]):
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        data["payload"] = AgentContext(**data["payload"]) if data["payload"] else None
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        return cls.from_dict(data)
