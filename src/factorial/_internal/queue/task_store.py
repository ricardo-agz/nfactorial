from __future__ import annotations

import json
from typing import Any, cast

import redis.asyncio as redis

from factorial._internal.queue.keys import RedisKeys
from factorial._internal.serialization import decode
from factorial.core.exceptions import (
    BatchNotFoundError,
    CorruptedTaskDataError,
    InvalidTaskIdError,
    TaskNotFoundError,
)
from factorial.core.utils import is_valid_task_id
from factorial.queue.task import Batch, BatchMetadata, TaskStatus


def task_team_id(*, task_id: str, metadata: dict[str, Any]) -> str:
    """Return a task's persisted team id or raise on malformed task data."""
    raw_team_id = metadata.get("team_id")
    if isinstance(raw_team_id, str) and raw_team_id:
        return raw_team_id
    raise CorruptedTaskDataError(task_id, ["metadata.team_id"])


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


__all__ = [
    "get_batch_data",
    "get_task_agent",
    "get_task_data",
    "get_task_status",
    "get_task_steering_messages",
    "task_team_id",
]
