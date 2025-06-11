from dataclasses import dataclass, asdict, field
import json
from typing import Any
from contextvars import ContextVar

from factorial.events import EventPublisher
from factorial.utils import decode


execution_context: ContextVar["ExecutionContext"] = ContextVar("execution_context")


@dataclass
class ExecutionContext:
    """Per-request context (not stored on agent)"""

    task_id: str
    owner_id: str
    retries: int
    iterations: int
    events: EventPublisher

    @classmethod
    def current(cls) -> "ExecutionContext":
        """Get current execution context"""
        return execution_context.get()


@dataclass
class AgentContext:
    query: str
    messages: list[dict[str, Any]] = field(default_factory=list)
    turn: int = 0
    output: Any = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str):
        return cls.from_dict(json.loads(decode(json_str)))
