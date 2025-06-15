from dataclasses import dataclass
from typing import Any, TypeVar
from contextvars import ContextVar
from pydantic import BaseModel

from factorial.events import EventPublisher


execution_context: ContextVar["ExecutionContext"] = ContextVar("execution_context")

ContextType = TypeVar("ContextType", bound="AgentContext")


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


class AgentContext(BaseModel):
    """
    Agent state passed to the agent for turn execution.

    Base Fields:
    - query: str
    - messages: list[dict[str, Any]] = []
    - turn: int = 0
    - output: Any = None
    """

    query: str
    messages: list[dict[str, Any]] = []
    turn: int = 0
    output: Any = None

    class Config:
        extra = "allow"  # Users can add extra fields
        arbitrary_types_allowed = True  # For Any type flexibility

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        return cls(**data)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str):
        return cls.model_validate_json(json_str)
