import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Generic, cast

from factorial._internal.serialization import decode
from factorial.agent.context import ContextType
from factorial.core.exceptions import (
    CorruptedTaskDataError,
)


class TaskStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    PENDING_TOOL_RESULTS = "pending_tool_results"
    PENDING_CHILD_TASKS = "pending_child_tasks"
    BACKOFF = "backoff"


@dataclass
class TaskMetadata:
    owner_id: str
    team_id: str | None = None
    parent_id: str | None = None
    resumed_from_task_id: str | None = None
    batch_id: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    max_turns: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner_id": self.owner_id,
            "team_id": self.team_id,
            "parent_id": self.parent_id,
            "resumed_from_task_id": self.resumed_from_task_id,
            "batch_id": self.batch_id,
            "created_at": self.created_at.timestamp(),
            "max_turns": self.max_turns,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskMetadata":
        normalized = dict(data)
        normalized["created_at"] = datetime.fromtimestamp(
            float(normalized["created_at"]), tz=timezone.utc
        )
        return cls(**normalized)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str | bytes) -> "TaskMetadata":
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
        cls,
        owner_id: str,
        agent: str,
        payload: ContextType,
        batch_id: str | None = None,
        max_turns: int | None = None,
        team_id: str | None = None,
    ) -> "Task[ContextType]":
        return Task(
            status=TaskStatus.QUEUED,
            agent=agent,
            payload=payload,
            metadata=TaskMetadata(
                owner_id=owner_id,
                team_id=team_id,
                batch_id=batch_id,
                max_turns=max_turns,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "agent": self.agent,
            "payload": self.payload.to_dict(),
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
        payload_parser: Callable[[dict[str, Any]], ContextType],
    ) -> "Task[ContextType]":
        task_id = str(data["id"])
        status = TaskStatus(data["status"])
        metadata = TaskMetadata.from_dict(data["metadata"])
        if not isinstance(metadata.team_id, str) or not metadata.team_id:
            raise CorruptedTaskDataError(task_id, ["metadata.team_id"])

        payload: ContextType
        if data["payload"]:
            if isinstance(data["payload"], dict):
                payload_dict = cast(dict[str, Any], data["payload"])
                payload = payload_parser(payload_dict)
            else:
                payload_str = decode(data["payload"])
                payload_dict = json.loads(payload_str)
                payload = payload_parser(payload_dict)
        else:
            payload = payload_parser({})

        return cls(
            id=task_id,
            status=status,
            agent=data["agent"],
            payload=payload,
            metadata=metadata,
            pickups=data["pickups"],
            retries=data["retries"],
        )

    @classmethod
    def from_json(
        cls,
        json_str: str | bytes,
        payload_parser: Callable[[dict[str, Any]], ContextType],
    ) -> "Task[ContextType]":
        data = json.loads(decode(json_str))
        return cls.from_dict(data, payload_parser)

@dataclass
class BatchMetadata:
    owner_id: str
    created_at: datetime
    total_tasks: int
    max_progress: int
    status: str
    parent_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner_id": self.owner_id,
            "parent_id": self.parent_id,
            "created_at": self.created_at.timestamp(),
            "total_tasks": self.total_tasks,
            "max_progress": self.max_progress,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BatchMetadata":
        data["created_at"] = datetime.fromtimestamp(
            float(data["created_at"]), tz=timezone.utc
        )
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class Batch:
    id: str
    metadata: BatchMetadata
    task_ids: list[str]
    remaining_task_ids: list[str] = field(default_factory=list)
    progress: float = 0.0


__all__ = [
    "Batch",
    "BatchMetadata",
    "Task",
    "TaskMetadata",
    "TaskStatus",
]
