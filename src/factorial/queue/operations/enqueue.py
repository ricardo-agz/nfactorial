from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Any, cast

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.context import VerificationState
from factorial.events import AgentEvent, EventPublisher
from factorial.exceptions import InvalidTaskIdError
from factorial.logging import get_logger
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    EnqueueBatchScript,
    create_enqueue_batch_script,
    create_enqueue_task_script,
    create_resume_enqueue_script,
)
from factorial.queue.task import (
    Batch,
    BatchMetadata,
    ContextType,
    Task,
    TaskStatus,
    get_batch_data,
    get_task_data,
)
from factorial.utils import is_valid_task_id, serialize_data

logger = get_logger(__name__)

_ENQUEUE_IDEMPOTENCY_TTL_S = 60 * 60 * 24

_RESUME_IDEMPOTENCY_TTL_S = 60 * 60 * 24

_NAMESPACE_ROOT = uuid.uuid5(uuid.NAMESPACE_DNS, "factorial.sh")

_ENQUEUE_NAMESPACE_ROOT = uuid.uuid5(_NAMESPACE_ROOT, "enqueue")

_ENQUEUE_TASK_NAMESPACE = uuid.uuid5(_ENQUEUE_NAMESPACE_ROOT, "task-id.v1")

_ENQUEUE_BATCH_NAMESPACE = uuid.uuid5(_ENQUEUE_NAMESPACE_ROOT, "batch-id.v1")

_ENQUEUE_BATCH_TASK_NAMESPACE = uuid.uuid5(
    _ENQUEUE_NAMESPACE_ROOT,
    "batch-task-id.v1",
)

_RESUME_NAMESPACE_ROOT = uuid.uuid5(_NAMESPACE_ROOT, "resume.task")

_RESUME_TASK_NAMESPACE = uuid.uuid5(_RESUME_NAMESPACE_ROOT, "task-id.v1")

def _resume_request_hash(*, messages: list[dict[str, Any]]) -> str:
    messages_json = json.dumps(
        serialize_data(messages),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(messages_json.encode("utf-8")).hexdigest()

def _enqueue_request_hash(*, agent_name: str, task: Task[Any]) -> str:
    payload = (
        serialize_data(task.payload.to_dict())
        if task.payload is not None
        else {}
    )
    request_envelope = {
        "agent_name": agent_name,
        "owner_id": task.metadata.owner_id,
        "team_id": task.metadata.team_id,
        "parent_id": task.metadata.parent_id,
        "resumed_from_task_id": task.metadata.resumed_from_task_id,
        "batch_id": task.metadata.batch_id,
        "max_turns": task.metadata.max_turns,
        "payload": payload,
    }
    request_json = json.dumps(
        request_envelope,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(request_json.encode("utf-8")).hexdigest()

def _batch_enqueue_request_hash(
    *,
    agent_name: str,
    owner_id: str,
    parent_id: str | None,
    team_id: str | None,
    payloads: list[ContextType],
    task_ids: list[str] | None,
    batch_id: str | None,
) -> str:
    request_envelope = {
        "agent_name": agent_name,
        "owner_id": owner_id,
        "team_id": team_id,
        "parent_id": parent_id,
        "payloads": [serialize_data(payload.to_dict()) for payload in payloads],
        "task_ids": task_ids,
        "batch_id": batch_id,
    }
    request_json = json.dumps(
        request_envelope,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(request_json.encode("utf-8")).hexdigest()

def _deterministic_enqueued_task_id(
    *,
    owner_id: str,
    agent_name: str,
    idempotency_key: str,
    request_hash: str,
) -> str:
    seed = f"{owner_id}:{agent_name}:{idempotency_key}:{request_hash}"
    return str(uuid.uuid5(_ENQUEUE_TASK_NAMESPACE, seed))

def _deterministic_enqueued_batch_id(
    *,
    owner_id: str,
    agent_name: str,
    idempotency_key: str,
    request_hash: str,
) -> str:
    seed = f"{owner_id}:{agent_name}:{idempotency_key}:{request_hash}"
    return str(uuid.uuid5(_ENQUEUE_BATCH_NAMESPACE, seed))

def _deterministic_enqueued_batch_task_id(
    *,
    owner_id: str,
    agent_name: str,
    idempotency_key: str,
    request_hash: str,
    input_index: int,
) -> str:
    seed = f"{owner_id}:{agent_name}:{idempotency_key}:{request_hash}:{input_index}"
    return str(uuid.uuid5(_ENQUEUE_BATCH_TASK_NAMESPACE, seed))

def _deterministic_resumed_task_id(
    *,
    source_task_id: str,
    idempotency_key: str,
    request_hash: str,
) -> str:
    seed = f"{source_task_id}:{idempotency_key}:{request_hash}"
    return str(uuid.uuid5(_RESUME_TASK_NAMESPACE, seed))

def _is_terminal_status(task_status: TaskStatus) -> bool:
    return task_status in [
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
    ]

async def enqueue_task(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    task: Task[ContextType],
    idempotency_key: str | None = None,
) -> str:
    normalized_idempotency_key: str | None = None
    if idempotency_key is not None:
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValueError(
                "enqueue_task idempotency_key must be a non-empty string when provided"
            )
        normalized_idempotency_key = idempotency_key.strip()

    enqueue_request_hash = _enqueue_request_hash(
        agent_name=agent.name,
        task=task,
    )
    idempotency_enabled = normalized_idempotency_key is not None
    enqueue_idem_storage_key = ""
    if normalized_idempotency_key is not None:
        root_keys = RedisKeys.format(namespace=namespace)
        enqueue_idem_storage_key = root_keys.enqueue_idempotency(
            task.metadata.owner_id,
            agent.name,
            normalized_idempotency_key,
        )
        task.id = _deterministic_enqueued_task_id(
            owner_id=task.metadata.owner_id,
            agent_name=agent.name,
            idempotency_key=normalized_idempotency_key,
            request_hash=enqueue_request_hash,
        )

    if not isinstance(task.metadata.team_id, str) or not task.metadata.team_id:
        task.metadata.team_id = task.id

    keys = RedisKeys.format(namespace=namespace, agent=agent.name)

    if not is_valid_task_id(task.id):
        raise InvalidTaskIdError(task.id)

    enqueue_script = await create_enqueue_task_script(redis_client)
    enqueue_result = await enqueue_script.execute(
        agent_queue_key=keys.queue_main,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        task_id=task.id,
        task_agent=agent.name,
        task_payload_json=task.payload.to_json() if task.payload else "{}",
        task_pickups=0,
        task_retries=0,
        task_meta_json=task.metadata.to_json(),
        enqueue_idempotency_key=enqueue_idem_storage_key,
        task_children_key_template=keys.task_children("{parent_task_id}"),
        request_hash=enqueue_request_hash,
        ttl_seconds=_ENQUEUE_IDEMPOTENCY_TTL_S,
        idempotency_enabled=idempotency_enabled,
    )
    if enqueue_result.decision == "conflict":
        raise ValueError(
            "enqueue_task idempotency_key conflict: key was reused "
            "with a different request payload."
        )

    task.id = enqueue_result.task_id
    return task.id

async def resume_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    agent: BaseAgent[Any],
    messages: list[dict[str, Any]],
    idempotency_key: str | None = None,
) -> Task[Any]:
    """Resume a terminal task as a new queued task.

    This operation clones the source task context, appends `messages`, resets
    run-scoped state, and enqueues a brand new task ID.
    """

    if not isinstance(messages, list):
        raise TypeError("resume_task messages must be a list of dict objects")

    normalized_idempotency_key: str | None = None
    if idempotency_key is not None:
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValueError(
                "resume_task idempotency_key must be a non-empty string when provided"
            )
        normalized_idempotency_key = idempotency_key.strip()

    normalized_messages: list[dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            raise TypeError("resume_task messages must be a list of dict objects")
        normalized_messages.append(dict(message))

    source_task_data = await get_task_data(redis_client, namespace, task_id)
    source_status = TaskStatus(source_task_data["status"])
    if not _is_terminal_status(source_status):
        raise ValueError(
            "resume_task requires the source task to be terminal "
            "(completed, failed, or cancelled)"
        )

    source_agent_name = source_task_data["agent"]
    if source_agent_name != agent.name:
        raise ValueError(
            "resume_task source task belongs to a different agent. "
            f"Expected '{agent.name}', got '{source_agent_name}'."
        )

    context_class = cast(Any, agent.context_class)
    source_task: Task[Any] = Task.from_dict(
        source_task_data,
        context_class=context_class,
    )
    root_keys = RedisKeys.format(namespace=namespace)
    resume_request_hash = _resume_request_hash(messages=normalized_messages)

    idempotency_enabled = normalized_idempotency_key is not None
    deterministic_resumed_task_id: str | None = None
    resume_idem_storage_key = ""
    if normalized_idempotency_key is not None:
        deterministic_resumed_task_id = _deterministic_resumed_task_id(
            source_task_id=task_id,
            idempotency_key=normalized_idempotency_key,
            request_hash=resume_request_hash,
        )
        resume_idem_storage_key = root_keys.resume_idempotency(
            task_id,
            normalized_idempotency_key,
        )

    resumed_payload = context_class.from_dict(source_task.payload.to_dict())

    existing_messages = (
        list(resumed_payload.messages)
        if isinstance(getattr(resumed_payload, "messages", None), list)
        else []
    )
    resumed_payload.messages = [*existing_messages, *normalized_messages]
    resumed_payload.turn = 0
    resumed_payload.output = None
    resumed_payload.attempt = 0
    resumed_payload.verification = VerificationState()

    resumed_task: Task[Any] = Task.create(
        owner_id=source_task.metadata.owner_id,
        agent=agent.name,
        payload=resumed_payload,
        max_turns=agent.max_turns,
    )
    if deterministic_resumed_task_id is not None:
        resumed_task.id = deterministic_resumed_task_id

    # Keep operational parent linkage while adding revision lineage.
    resumed_task.metadata.parent_id = source_task.metadata.parent_id
    resumed_task.metadata.resumed_from_task_id = source_task.id
    resumed_task.metadata.team_id = source_task.metadata.team_id or source_task.id

    task_keys = RedisKeys.format(namespace=namespace, agent=agent.name)
    resume_enqueue_script = await create_resume_enqueue_script(redis_client)
    resume_result = await resume_enqueue_script.execute(
        agent_queue_key=task_keys.queue_main,
        task_statuses_key=task_keys.task_status,
        task_agents_key=task_keys.task_agent,
        task_payloads_key=task_keys.task_payload,
        task_pickups_key=task_keys.task_pickups,
        task_retries_key=task_keys.task_retries,
        task_metas_key=task_keys.task_meta,
        resume_idempotency_key=resume_idem_storage_key,
        task_id=resumed_task.id,
        task_agent=agent.name,
        task_payload_json=(
            resumed_task.payload.to_json() if resumed_task.payload else "{}"
        ),
        task_pickups=resumed_task.pickups,
        task_retries=resumed_task.retries,
        task_meta_json=resumed_task.metadata.to_json(),
        task_children_key_template=task_keys.task_children("{parent_task_id}"),
        request_hash=resume_request_hash,
        source_task_id=task_id,
        ttl_seconds=_RESUME_IDEMPOTENCY_TTL_S,
        idempotency_enabled=idempotency_enabled,
    )

    if resume_result.decision == "conflict":
        raise ValueError(
            "resume_task idempotency_key conflict: key was reused "
            "with a different request payload."
        )
    if resume_result.decision == "replay":
        existing_task_data = await get_task_data(
            redis_client,
            namespace,
            resume_result.resumed_task_id,
        )
        return Task.from_dict(
            existing_task_data,
            context_class=context_class,
        )

    resumed_task.id = resume_result.resumed_task_id

    try:
        updates_channel = RedisKeys.for_owner(
            namespace=namespace,
            owner_id=source_task.metadata.owner_id,
        ).updates_channel
        event_publisher = EventPublisher(
            redis_client=redis_client,
            channel=updates_channel,
        )
        event_data: dict[str, Any] = {
            "source_task_id": task_id,
            "resumed_task_id": resumed_task.id,
            "idempotent_replay": False,
        }
        if normalized_idempotency_key is not None:
            event_data["idempotency_key"] = normalized_idempotency_key
        await event_publisher.publish_event(
            AgentEvent(
                event_type="task_resumed",
                task_id=resumed_task.id,
                owner_id=source_task.metadata.owner_id,
                agent_name=agent.name,
                data=event_data,
            )
        )
    except Exception as exc:
        logger.error(
            "Failed to publish task_resumed event for source task %s",
            task_id,
            exc_info=exc,
        )

    return resumed_task

async def create_batch_and_enqueue(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    payloads: list[ContextType],
    owner_id: str,
    parent_id: str | None = None,
    team_id: str | None = None,
    task_ids: list[str] | None = None,
    batch_id: str | None = None,
    idempotency_key: str | None = None,
) -> Batch:
    """Atomically enqueue a batch of tasks via batch_enqueue.lua.

    Returns the *batch_id* created.
    """
    normalized_idempotency_key: str | None = None
    if idempotency_key is not None:
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValueError(
                "create_batch_and_enqueue idempotency_key must be a non-empty "
                "string when provided"
            )
        normalized_idempotency_key = idempotency_key.strip()

    if task_ids is not None:
        if len(task_ids) != len(payloads):
            raise ValueError(
                "create_batch_and_enqueue task_ids length must match payload count"
            )
        if len(set(task_ids)) != len(task_ids):
            raise ValueError("create_batch_and_enqueue task_ids must be unique")

    batch_enqueue_request_hash = _batch_enqueue_request_hash(
        agent_name=agent.name,
        owner_id=owner_id,
        parent_id=parent_id,
        team_id=team_id,
        payloads=payloads,
        task_ids=task_ids,
        batch_id=batch_id,
    )

    idempotency_enabled = normalized_idempotency_key is not None
    batch_idem_storage_key = ""
    if normalized_idempotency_key is not None:
        root_keys = RedisKeys.format(namespace=namespace)
        batch_idem_storage_key = root_keys.batch_enqueue_idempotency(
            owner_id,
            agent.name,
            normalized_idempotency_key,
        )

        if task_ids is None:
            task_ids = [
                _deterministic_enqueued_batch_task_id(
                    owner_id=owner_id,
                    agent_name=agent.name,
                    idempotency_key=normalized_idempotency_key,
                    request_hash=batch_enqueue_request_hash,
                    input_index=index,
                )
                for index, _ in enumerate(payloads)
            ]
        if batch_id is None:
            batch_id = _deterministic_enqueued_batch_id(
                owner_id=owner_id,
                agent_name=agent.name,
                idempotency_key=normalized_idempotency_key,
                request_hash=batch_enqueue_request_hash,
            )

    if batch_id is None:
        batch_id = str(uuid.uuid4())
    if not isinstance(team_id, str) or not team_id:
        team_id = batch_id
    created_at = time.time()

    task_objs: list[Task[ContextType]] = []
    for index, payload in enumerate(payloads):
        t: Task[ContextType] = Task.create(
            owner_id=owner_id, agent=agent.name, payload=payload, batch_id=batch_id
        )
        if task_ids is not None:
            t.id = task_ids[index]
        if parent_id is not None:
            t.metadata.parent_id = parent_id
        t.metadata.team_id = team_id
        if not is_valid_task_id(t.id):
            raise InvalidTaskIdError(t.id)
        task_objs.append(t)

    tasks_json = [
        {
            "id": t.id,
            "payload_json": t.payload.to_json(),
        }
        for t in task_objs
    ]

    base_task_meta = {
        "owner_id": owner_id,
        "team_id": team_id,
        "parent_id": parent_id,
        "batch_id": batch_id,
        "created_at": created_at,
        "max_turns": agent.max_turns,
    }

    max_progress = (
        agent.max_turns * len(task_objs) if agent.max_turns else len(task_objs)
    )
    batch_meta = {
        "owner_id": owner_id,
        "parent_id": parent_id,
        "created_at": created_at,
        "total_tasks": len(task_objs),
        "max_progress": max_progress,
        "status": "active",
    }

    keys = RedisKeys.format(namespace=namespace, agent=agent.name)
    script: EnqueueBatchScript = await create_enqueue_batch_script(redis_client)
    result = await script.execute(
        agent_queue_key=keys.queue_main,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        batch_tasks_key=keys.batch_tasks,
        batch_meta_key=keys.batch_meta,
        batch_id=batch_id,
        owner_id=owner_id,
        created_at=created_at,
        agent_name=agent.name,
        tasks_json=json.dumps(tasks_json),
        base_task_meta_json=json.dumps(base_task_meta),
        batch_meta_json=json.dumps(batch_meta),
        batch_remaining_tasks_key=keys.batch_remaining_tasks,
        batch_progress_key=keys.batch_progress,
        batch_enqueue_idempotency_key=batch_idem_storage_key,
        task_children_key_template=keys.task_children("{parent_task_id}"),
        request_hash=batch_enqueue_request_hash,
        ttl_seconds=_ENQUEUE_IDEMPOTENCY_TTL_S,
        idempotency_enabled=idempotency_enabled,
    )
    if result.decision == "conflict":
        raise ValueError(
            "create_batch_and_enqueue idempotency_key conflict: key "
            "was reused with a different request payload."
        )
    if result.decision == "replay":
        return await get_batch_data(
            redis_client=redis_client,
            namespace=namespace,
            batch_id=result.batch_id,
        )

    batch_id = result.batch_id
    task_ids = result.task_ids

    batch = Batch(
        id=batch_id,
        metadata=BatchMetadata.from_dict(batch_meta),
        task_ids=task_ids,
        remaining_task_ids=task_ids,
        progress=0.0,
    )
    return batch
