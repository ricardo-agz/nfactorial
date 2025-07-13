from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
import uuid
import json
from typing import Generic, Type, Any, cast
import redis.asyncio as redis

from factorial.utils import decode
from factorial.context import ContextType
from factorial.exceptions import (
    TaskNotFoundError,
    InvalidTaskIdError,
    CorruptedTaskDataError,
    BatchNotFoundError,
)
from factorial.queue.keys import RedisKeys
from factorial.utils import is_valid_task_id


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
    parent_id: str | None = None
    batch_id: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    max_turns: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner_id": self.owner_id,
            "parent_id": self.parent_id,
            "batch_id": self.batch_id,
            "created_at": self.created_at.timestamp(),
            "max_turns": self.max_turns,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
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
        cls,
        owner_id: str,
        agent: str,
        payload: ContextType,
        batch_id: str | None = None,
        max_turns: int | None = None,
    ) -> "Task[ContextType]":
        return Task(
            status=TaskStatus.QUEUED,
            agent=agent,
            payload=payload,
            metadata=TaskMetadata(
                owner_id=owner_id,
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
        context_class: Type[ContextType],
    ) -> "Task[ContextType]":
        status = TaskStatus(data["status"])
        metadata = TaskMetadata.from_dict(data["metadata"])

        if data["payload"]:
            if isinstance(data["payload"], dict):
                payload = context_class.from_dict(cast(dict[str, Any], data["payload"]))
            else:
                payload_str = decode(data["payload"])
                payload = context_class.from_dict(json.loads(payload_str))
        else:
            payload = context_class.from_dict({})

        return cls(
            id=data["id"],
            status=status,
            agent=data["agent"],
            payload=payload,
            metadata=metadata,
            pickups=data["pickups"],
            retries=data["retries"],
        )

    @classmethod
    def from_json(
        cls, json_str: str | bytes, context_class: Type[ContextType]
    ) -> "Task[ContextType]":
        data = json.loads(decode(json_str))
        return cls.from_dict(data, context_class)


async def get_task_data(
    redis_client: redis.Redis, namespace: str, task_id: str
) -> dict[str, Any]:
    keys = RedisKeys.format(namespace=namespace)

    pipe = redis_client.pipeline(transaction=True)
    pipe.multi()
    pipe.hget(keys.task_status, task_id)
    pipe.hget(keys.task_agent, task_id)
    pipe.hget(keys.task_payload, task_id)
    pipe.hget(keys.task_pickups, task_id)
    pipe.hget(keys.task_retries, task_id)
    pipe.hget(keys.task_meta, task_id)

    status, agent, payload_json, pickups, retries, meta_json = await pipe.execute()

    if not status and not agent and not payload_json and not meta_json:
        raise TaskNotFoundError(task_id)
    elif not all([status, agent, payload_json, pickups, retries, meta_json]):
        fields = {
            "status": status,
            "agent": agent,
            "payload": payload_json,
            "pickups": pickups,
            "retries": retries,
            "metadata": meta_json,
        }
        missing_fields = [field for field, value in fields.items() if not value]
        raise CorruptedTaskDataError(task_id, missing_fields)

    task_data: dict[str, Any] = {
        "id": task_id,
        "status": decode(status),
        "agent": decode(agent),
        "payload": json.loads(decode(payload_json)),
        "pickups": int(decode(pickups)),
        "retries": int(decode(retries)),
        "metadata": json.loads(decode(meta_json)),
    }

    return task_data


async def get_task_status(
    redis_client: redis.Redis, namespace: str, task_id: str
) -> TaskStatus:
    if not is_valid_task_id(task_id):
        raise InvalidTaskIdError(task_id)

    keys = RedisKeys.format(namespace=namespace)
    status: str | bytes = await redis_client.hget(  # type: ignore
        keys.task_status, task_id
    )
    if not status:
        raise TaskNotFoundError(task_id)

    return TaskStatus(decode(status))


async def get_task_agent(
    redis_client: redis.Redis, namespace: str, task_id: str
) -> str:
    if not is_valid_task_id(task_id):
        raise InvalidTaskIdError(task_id)

    keys = RedisKeys.format(namespace=namespace)
    agent: str | bytes = await redis_client.hget(  # type: ignore
        keys.task_agent, task_id
    )
    if not agent:
        raise TaskNotFoundError(task_id)

    return decode(agent)


async def get_task_steering_messages(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
) -> list[tuple[str, dict[str, Any]]]:
    """Get steering messages for a task"""
    await get_task_status(
        redis_client, namespace, task_id
    )  # Raise if task does not exist

    keys = RedisKeys.format(namespace=namespace, task_id=task_id)
    steering_key = keys.task_steering
    message_data: list[tuple[str, dict[str, Any]]] = []
    steering_messages = cast(
        dict[str | bytes, str | bytes],
        await redis_client.hgetall(steering_key),  # type: ignore
    )
    if not steering_messages:
        return []

    for message_id, message in steering_messages.items():
        message_id_str = decode(message_id)
        message_str = decode(message)
        message_data.append((message_id_str, json.loads(message_str)))

    return message_data


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


async def get_batch_data(
    redis_client: redis.Redis,
    namespace: str,
    batch_id: str,
) -> Batch:
    keys = RedisKeys.format(namespace=namespace)
    # Atomically fetch metadata, task_ids, and remaining_task_ids
    pipe = redis_client.pipeline(transaction=True)
    pipe.multi()
    pipe.hget(keys.batch_meta, batch_id)
    pipe.hget(keys.batch_tasks, batch_id)
    pipe.hget(keys.batch_remaining_tasks, batch_id)
    pipe.hget(keys.batch_progress, batch_id)
    (
        metadata_json_bytes,
        task_ids_json_bytes,
        remaining_tasks_bytes,
        progress_bytes,
    ) = await pipe.execute()

    if (
        metadata_json_bytes is None
        or remaining_tasks_bytes is None
        or progress_bytes is None
    ):
        raise BatchNotFoundError(batch_id)

    metadata = BatchMetadata.from_dict(json.loads(decode(metadata_json_bytes)))
    task_ids = json.loads(decode(task_ids_json_bytes)) if task_ids_json_bytes else []
    remaining_task_ids = (
        json.loads(decode(remaining_tasks_bytes)) if remaining_tasks_bytes else []
    )

    progress_sum = int(decode(progress_bytes))
    progress = (
        (progress_sum / metadata.max_progress) * 100
        if metadata.max_progress > 0
        else 0.0
    )

    return Batch(
        id=batch_id,
        task_ids=task_ids,
        remaining_task_ids=remaining_task_ids,
        metadata=metadata,
        progress=progress,
    )
