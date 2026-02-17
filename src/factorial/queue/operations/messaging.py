from __future__ import annotations

import json
import time
from typing import Any, cast

import redis.asyncio as redis

from factorial.events import AgentEvent, EventPublisher
from factorial.exceptions import (
    MessagingGroupAlreadyExistsError,
    MessagingGroupNotFoundError,
    MessagingInvalidRecipientError,
    MessagingPermissionError,
    MessagingScopeError,
    TaskNotFoundError,
)
from factorial.logging import get_logger
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    MessagingGroupMutationScriptResult,
    MessagingSendScriptResult,
    create_messaging_direct_send_script,
    create_messaging_group_add_members_script,
    create_messaging_group_create_script,
    create_messaging_group_send_script,
)
from factorial.queue.task import effective_team_id, get_task_data
from factorial.utils import decode, serialize_data

logger = get_logger(__name__)

_MESSAGING_HISTORY_MAXLEN = 20_000

def _normalize_group_name(group_name: str) -> str:
    if not isinstance(group_name, str) or not group_name.strip():
        raise ValueError("group_name must be a non-empty string")
    return group_name.strip()

def _resolve_task_team_id(task_data: dict[str, Any]) -> str:
    task_id = str(task_data["id"])
    metadata = cast(dict[str, Any], task_data["metadata"])
    return effective_team_id(task_id=task_id, metadata=metadata)

def _group_thread_id(*, team_id: str, group_name: str) -> str:
    return f"group:{team_id}:{group_name}"

def _direct_thread_id(*, team_id: str, sender_task_id: str, to_task_id: str) -> str:
    left, right = sorted([sender_task_id, to_task_id])
    return f"dm:{team_id}:{left}:{right}"

def _decode_group_meta(
    *,
    raw_meta: str | bytes | None,
    group_name: str,
    team_id: str,
) -> dict[str, Any]:
    if raw_meta is None:
        raise MessagingGroupNotFoundError(group_name, team_id)
    decoded = json.loads(decode(raw_meta))
    return cast(dict[str, Any], decoded)

async def _publish_messaging_event(
    *,
    redis_client: redis.Redis,
    namespace: str,
    owner_id: str,
    task_id: str,
    agent_name: str,
    event_type: str,
    data: dict[str, Any],
) -> None:
    try:
        keys = RedisKeys.format(namespace=namespace, owner_id=owner_id)
        await EventPublisher(
            redis_client=redis_client,
            channel=keys.updates_channel,
        ).publish_event(
            AgentEvent(
                event_type=event_type,
                task_id=task_id,
                owner_id=owner_id,
                agent_name=agent_name,
                data=data,
            )
        )
    except Exception as exc:  # pragma: no cover - best-effort observability path
        logger.error(
            "Failed to publish messaging event %s for task %s",
            event_type,
            task_id,
            exc_info=exc,
        )

async def messaging_groups_create(
    *,
    redis_client: redis.Redis,
    namespace: str,
    sender_task_id: str,
    group_name: str,
    member_task_ids: list[str] | None = None,
) -> dict[str, Any]:
    normalized_group_name = _normalize_group_name(group_name)
    sender_task_data = await get_task_data(redis_client, namespace, sender_task_id)
    sender_team_id = _resolve_task_team_id(sender_task_data)
    sender_owner_id = str(sender_task_data["metadata"]["owner_id"])
    sender_agent_name = str(sender_task_data["agent"])

    keys = RedisKeys.format(namespace=namespace)
    group_meta_key = keys.messaging_group_meta(sender_team_id)
    group_members_key = keys.messaging_group_members(
        sender_team_id,
        normalized_group_name,
    )
    team_tasks_key = keys.messaging_team_tasks(sender_team_id)

    script = await create_messaging_group_create_script(redis_client)
    result: MessagingGroupMutationScriptResult = await script.execute(
        task_metas_key=keys.task_meta,
        group_meta_key=group_meta_key,
        group_members_key=group_members_key,
        team_tasks_key=team_tasks_key,
        sender_task_id=sender_task_id,
        team_id=sender_team_id,
        group_name=normalized_group_name,
        group_meta_json=json.dumps(
            {
                "group_name": normalized_group_name,
                "team_id": sender_team_id,
                "created_at": time.time(),
                "created_by_task_id": sender_task_id,
            },
            sort_keys=True,
        ),
        member_task_ids_json=json.dumps(member_task_ids or []),
        groups_by_task_key_template=keys.messaging_groups_by_task("{task_id}"),
    )

    if result.decision == "exists":
        raise MessagingGroupAlreadyExistsError(normalized_group_name, sender_team_id)
    if result.decision == "sender_not_found":
        raise TaskNotFoundError(sender_task_id)
    if result.decision == "scope_mismatch":
        raise MessagingScopeError(
            "Sender task scope does not match team_id for group creation."
        )
    if result.decision == "member_not_found":
        raise MessagingInvalidRecipientError(
            f"Member task '{result.detail}' was not found"
        )
    if result.decision == "member_scope_mismatch":
        raise MessagingScopeError(
            f"Member task '{result.detail}' does not belong to sender team"
        )
    if result.decision == "invalid_group_name":
        raise ValueError("group_name must be a non-empty string")
    if result.decision != "created":
        raise RuntimeError(
            f"Unexpected messaging_group_create decision '{result.decision}'"
        )

    payload = {
        "team_id": sender_team_id,
        "group_name": normalized_group_name,
        "member_task_ids": result.member_task_ids,
    }
    await _publish_messaging_event(
        redis_client=redis_client,
        namespace=namespace,
        owner_id=sender_owner_id,
        task_id=sender_task_id,
        agent_name=sender_agent_name,
        event_type="messaging_group_created",
        data=payload,
    )
    return payload

async def messaging_groups_get(
    *,
    redis_client: redis.Redis,
    namespace: str,
    sender_task_id: str,
    group_name: str,
) -> dict[str, Any]:
    normalized_group_name = _normalize_group_name(group_name)
    sender_task_data = await get_task_data(redis_client, namespace, sender_task_id)
    sender_team_id = _resolve_task_team_id(sender_task_data)

    keys = RedisKeys.format(namespace=namespace)
    group_meta_key = keys.messaging_group_meta(sender_team_id)
    group_members_key = keys.messaging_group_members(
        sender_team_id,
        normalized_group_name,
    )
    groups_by_task_key = keys.messaging_groups_by_task(sender_task_id)

    raw_meta = await redis_client.hget(group_meta_key, normalized_group_name)  # type: ignore[misc]
    group_meta = _decode_group_meta(
        raw_meta=raw_meta,
        group_name=normalized_group_name,
        team_id=sender_team_id,
    )
    is_member = bool(
        await redis_client.sismember(groups_by_task_key, normalized_group_name)  # type: ignore[misc]
    )
    if not is_member:
        raise MessagingPermissionError(
            f"Task {sender_task_id} is not a member of group '{normalized_group_name}'"
        )
    members_raw = cast(
        set[str | bytes],
        await redis_client.smembers(group_members_key),  # type: ignore[misc]
    )
    member_task_ids = sorted(decode(member_id) for member_id in members_raw)
    return {
        "team_id": sender_team_id,
        "group_name": normalized_group_name,
        "member_task_ids": member_task_ids,
        "created_at": group_meta.get("created_at"),
        "created_by_task_id": group_meta.get("created_by_task_id"),
    }

async def messaging_groups_list(
    *,
    redis_client: redis.Redis,
    namespace: str,
    sender_task_id: str,
) -> list[dict[str, Any]]:
    sender_task_data = await get_task_data(redis_client, namespace, sender_task_id)
    sender_team_id = _resolve_task_team_id(sender_task_data)
    keys = RedisKeys.format(namespace=namespace)
    groups_by_task_key = keys.messaging_groups_by_task(sender_task_id)
    group_names_raw = cast(
        set[str | bytes],
        await redis_client.smembers(groups_by_task_key),  # type: ignore[misc]
    )
    group_names = sorted(decode(name) for name in group_names_raw)
    if not group_names:
        return []

    group_meta_key = keys.messaging_group_meta(sender_team_id)
    raw_meta_values = cast(
        list[str | bytes | None],
        await redis_client.hmget(group_meta_key, group_names),  # type: ignore[arg-type,misc]
    )
    results: list[dict[str, Any]] = []
    for name, raw_meta in zip(group_names, raw_meta_values, strict=True):
        if raw_meta is None:
            continue
        meta = cast(dict[str, Any], json.loads(decode(raw_meta)))
        results.append(
            {
                "team_id": sender_team_id,
                "group_name": name,
                "created_at": meta.get("created_at"),
                "created_by_task_id": meta.get("created_by_task_id"),
            }
        )
    return results

async def messaging_groups_find(
    *,
    redis_client: redis.Redis,
    namespace: str,
    sender_task_id: str,
    group_name: str,
) -> list[dict[str, Any]]:
    normalized_group_name = _normalize_group_name(group_name)
    groups = await messaging_groups_list(
        redis_client=redis_client,
        namespace=namespace,
        sender_task_id=sender_task_id,
    )
    return [group for group in groups if group["group_name"] == normalized_group_name]

async def messaging_groups_add_members(
    *,
    redis_client: redis.Redis,
    namespace: str,
    sender_task_id: str,
    group_name: str,
    member_task_ids: list[str],
) -> list[str]:
    normalized_group_name = _normalize_group_name(group_name)
    sender_task_data = await get_task_data(redis_client, namespace, sender_task_id)
    sender_team_id = _resolve_task_team_id(sender_task_data)
    sender_owner_id = str(sender_task_data["metadata"]["owner_id"])
    sender_agent_name = str(sender_task_data["agent"])

    keys = RedisKeys.format(namespace=namespace)
    group_meta_key = keys.messaging_group_meta(sender_team_id)
    group_members_key = keys.messaging_group_members(
        sender_team_id,
        normalized_group_name,
    )
    team_tasks_key = keys.messaging_team_tasks(sender_team_id)

    script = await create_messaging_group_add_members_script(redis_client)
    result: MessagingGroupMutationScriptResult = await script.execute(
        task_metas_key=keys.task_meta,
        group_meta_key=group_meta_key,
        group_members_key=group_members_key,
        team_tasks_key=team_tasks_key,
        sender_task_id=sender_task_id,
        team_id=sender_team_id,
        group_name=normalized_group_name,
        member_task_ids_json=json.dumps(member_task_ids),
        groups_by_task_key_template=keys.messaging_groups_by_task("{task_id}"),
    )

    if result.decision == "group_not_found":
        raise MessagingGroupNotFoundError(normalized_group_name, sender_team_id)
    if result.decision == "sender_not_found":
        raise TaskNotFoundError(sender_task_id)
    if result.decision == "sender_not_member":
        raise MessagingPermissionError(
            f"Task {sender_task_id} is not a member of group '{normalized_group_name}'"
        )
    if result.decision == "scope_mismatch":
        raise MessagingScopeError("Sender task scope mismatch")
    if result.decision == "member_not_found":
        raise MessagingInvalidRecipientError(
            f"Member task '{result.detail}' was not found"
        )
    if result.decision == "member_scope_mismatch":
        raise MessagingScopeError(
            f"Member task '{result.detail}' does not belong to sender team"
        )
    if result.decision != "updated":
        raise RuntimeError(
            f"Unexpected messaging_group_add_members decision '{result.decision}'"
        )

    await _publish_messaging_event(
        redis_client=redis_client,
        namespace=namespace,
        owner_id=sender_owner_id,
        task_id=sender_task_id,
        agent_name=sender_agent_name,
        event_type="messaging_group_members_added",
        data={
            "team_id": sender_team_id,
            "group_name": normalized_group_name,
            "added_member_task_ids": result.member_task_ids,
        },
    )
    return result.member_task_ids

async def messaging_groups_send(
    *,
    redis_client: redis.Redis,
    namespace: str,
    sender_task_id: str,
    group_name: str,
    content: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_group_name = _normalize_group_name(group_name)
    sender_task_data = await get_task_data(redis_client, namespace, sender_task_id)
    sender_team_id = _resolve_task_team_id(sender_task_data)
    sender_owner_id = str(sender_task_data["metadata"]["owner_id"])
    sender_agent_name = str(sender_task_data["agent"])

    keys = RedisKeys.format(namespace=namespace)
    group_meta_key = keys.messaging_group_meta(sender_team_id)
    group_members_key = keys.messaging_group_members(
        sender_team_id,
        normalized_group_name,
    )
    thread_id = _group_thread_id(
        team_id=sender_team_id,
        group_name=normalized_group_name,
    )
    script = await create_messaging_group_send_script(redis_client)
    steering_key_template = RedisKeys.format(
        namespace=namespace,
        task_id="{task_id}",
    ).task_steering
    agent_queue_key_template = RedisKeys.format(
        namespace=namespace,
        agent="{agent}",
    )
    result: MessagingSendScriptResult = await script.execute(
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_metas_key=keys.task_meta,
        group_meta_key=group_meta_key,
        group_members_key=group_members_key,
        thread_history_key=keys.messaging_thread_history(thread_id),
        global_history_key=keys.messaging_history_global,
        message_seq_key=keys.messaging_message_seq,
        activity_wait_meta_key=keys.activity_wait_meta,
        team_tasks_key=keys.messaging_team_tasks(sender_team_id),
        sender_task_id=sender_task_id,
        team_id=sender_team_id,
        group_name=normalized_group_name,
        content=content,
        metadata_json=json.dumps(serialize_data(metadata or {}), sort_keys=True),
        steering_key_template=steering_key_template,
        history_maxlen=_MESSAGING_HISTORY_MAXLEN,
        queue_main_key_template=agent_queue_key_template.queue_main,
        queue_pending_key_template=agent_queue_key_template.queue_pending,
        groups_by_task_key_template=keys.messaging_groups_by_task("{task_id}"),
    )

    if result.decision == "group_not_found":
        raise MessagingGroupNotFoundError(normalized_group_name, sender_team_id)
    if result.decision == "sender_not_found":
        raise TaskNotFoundError(sender_task_id)
    if result.decision == "sender_not_member":
        raise MessagingPermissionError(
            f"Task {sender_task_id} is not a member of group '{normalized_group_name}'"
        )
    if result.decision == "scope_mismatch":
        raise MessagingScopeError("Sender task scope mismatch")
    if result.decision != "sent":
        raise RuntimeError(
            f"Unexpected messaging_group_send decision '{result.decision}'"
        )

    report = {
        "thread_message_id": result.thread_message_id,
        "global_message_id": result.global_message_id,
        "delivered_task_ids": result.delivered_task_ids,
        "skipped_inactive_task_ids": result.skipped_inactive_task_ids,
        "failed_task_ids": result.failed_task_ids,
    }
    await _publish_messaging_event(
        redis_client=redis_client,
        namespace=namespace,
        owner_id=sender_owner_id,
        task_id=sender_task_id,
        agent_name=sender_agent_name,
        event_type="messaging_group_message_sent",
        data={
            "team_id": sender_team_id,
            "group_name": normalized_group_name,
            **report,
        },
    )
    if result.skipped_inactive_task_ids or result.failed_task_ids:
        await _publish_messaging_event(
            redis_client=redis_client,
            namespace=namespace,
            owner_id=sender_owner_id,
            task_id=sender_task_id,
            agent_name=sender_agent_name,
            event_type="messaging_delivery_partial",
            data={
                "team_id": sender_team_id,
                "group_name": normalized_group_name,
                **report,
            },
        )
    return report

async def messaging_send_direct(
    *,
    redis_client: redis.Redis,
    namespace: str,
    sender_task_id: str,
    to_task_id: str,
    content: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sender_task_data = await get_task_data(redis_client, namespace, sender_task_id)
    sender_team_id = _resolve_task_team_id(sender_task_data)
    sender_owner_id = str(sender_task_data["metadata"]["owner_id"])
    sender_agent_name = str(sender_task_data["agent"])
    keys = RedisKeys.format(namespace=namespace)

    thread_id = _direct_thread_id(
        team_id=sender_team_id,
        sender_task_id=sender_task_id,
        to_task_id=to_task_id,
    )
    script = await create_messaging_direct_send_script(redis_client)
    steering_key_template = RedisKeys.format(
        namespace=namespace,
        task_id="{task_id}",
    ).task_steering
    agent_queue_key_template = RedisKeys.format(
        namespace=namespace,
        agent="{agent}",
    )
    result: MessagingSendScriptResult = await script.execute(
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_metas_key=keys.task_meta,
        thread_history_key=keys.messaging_thread_history(thread_id),
        global_history_key=keys.messaging_history_global,
        message_seq_key=keys.messaging_message_seq,
        activity_wait_meta_key=keys.activity_wait_meta,
        sender_task_id=sender_task_id,
        to_task_id=to_task_id,
        team_id=sender_team_id,
        content=content,
        metadata_json=json.dumps(serialize_data(metadata or {}), sort_keys=True),
        steering_key_template=steering_key_template,
        history_maxlen=_MESSAGING_HISTORY_MAXLEN,
        queue_main_key_template=agent_queue_key_template.queue_main,
        queue_pending_key_template=agent_queue_key_template.queue_pending,
    )

    if result.decision == "sender_not_found":
        raise TaskNotFoundError(sender_task_id)
    if result.decision == "recipient_not_found":
        raise MessagingInvalidRecipientError(f"Recipient task '{to_task_id}' not found")
    if result.decision in {"scope_mismatch", "recipient_scope_mismatch"}:
        raise MessagingScopeError("Direct message crosses team scope")
    if result.decision != "sent":
        raise RuntimeError(
            f"Unexpected messaging_direct_send decision '{result.decision}'"
        )

    report = {
        "thread_message_id": result.thread_message_id,
        "global_message_id": result.global_message_id,
        "delivered_task_ids": result.delivered_task_ids,
        "skipped_inactive_task_ids": result.skipped_inactive_task_ids,
        "failed_task_ids": result.failed_task_ids,
    }
    await _publish_messaging_event(
        redis_client=redis_client,
        namespace=namespace,
        owner_id=sender_owner_id,
        task_id=sender_task_id,
        agent_name=sender_agent_name,
        event_type="messaging_direct_message_sent",
        data={
            "team_id": sender_team_id,
            "to_task_id": to_task_id,
            **report,
        },
    )
    if result.skipped_inactive_task_ids or result.failed_task_ids:
        await _publish_messaging_event(
            redis_client=redis_client,
            namespace=namespace,
            owner_id=sender_owner_id,
            task_id=sender_task_id,
            agent_name=sender_agent_name,
            event_type="messaging_delivery_partial",
            data={
                "team_id": sender_team_id,
                "to_task_id": to_task_id,
                **report,
            },
        )
    return report
