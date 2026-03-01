from __future__ import annotations

import base64
import json
import time
from typing import Any, Literal, cast

import redis.asyncio as redis

from factorial.core.events import AgentEvent, EventPublisher
from factorial.core.exceptions import (
    MessagingGroupAlreadyExistsError,
    MessagingGroupNotFoundError,
    MessagingInvalidRecipientError,
    MessagingPermissionError,
    MessagingScopeError,
    TaskNotFoundError,
)
from factorial.core.logging import get_logger
from factorial.core.utils import decode, serialize_data
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    MessagingGroupMutationScriptResult,
    MessagingSendScriptResult,
    create_messaging_direct_send_script,
    create_messaging_group_add_members_script,
    create_messaging_group_create_script,
    create_messaging_group_remove_members_script,
    create_messaging_group_send_script,
    create_messaging_human_direct_send_script,
    create_messaging_human_group_send_script,
)
from factorial.queue.task import effective_team_id, get_task_data

logger = get_logger(__name__)

_MESSAGING_HISTORY_MAXLEN = 20_000
_GROUP_ID_PREFIX = "grp1."
_DEFAULT_HISTORY_LIMIT = 50
_MAX_HISTORY_LIMIT = 500
_DEFAULT_LIST_LIMIT = 50
_MAX_LIST_LIMIT = 200

def _normalize_group_name(group_name: str) -> str:
    if not isinstance(group_name, str) or not group_name.strip():
        raise ValueError("group_name must be a non-empty string")
    return group_name.strip()

def _normalize_team_id(team_id: str) -> str:
    if not isinstance(team_id, str) or not team_id.strip():
        raise ValueError("team_id must be a non-empty string")
    return team_id.strip()

def _normalize_owner_id(owner_id: str) -> str:
    if not isinstance(owner_id, str) or not owner_id.strip():
        raise ValueError("owner_id must be a non-empty string")
    return owner_id.strip()

def _normalize_content(content: str) -> str:
    if not isinstance(content, str) or not content.strip():
        raise ValueError("content must be a non-empty string")
    return content.strip()

def _encode_group_id(*, team_id: str, group_name: str) -> str:
    payload = json.dumps(
        {"team_id": team_id, "group_name": group_name},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    token = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    return f"{_GROUP_ID_PREFIX}{token}"

def _decode_group_id(group_id: str) -> tuple[str, str]:
    if not isinstance(group_id, str) or not group_id.startswith(_GROUP_ID_PREFIX):
        raise ValueError("group_id has invalid format")
    token = group_id[len(_GROUP_ID_PREFIX) :]
    if not token:
        raise ValueError("group_id has invalid format")
    padded_token = token + "=" * (-len(token) % 4)
    try:
        payload_bytes = base64.urlsafe_b64decode(padded_token.encode("ascii"))
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise ValueError("group_id has invalid format") from exc
    if not isinstance(payload, dict):
        raise ValueError("group_id has invalid format")
    raw_team_id = payload.get("team_id")
    raw_group_name = payload.get("group_name")
    if not isinstance(raw_team_id, str) or not isinstance(raw_group_name, str):
        raise ValueError("group_id has invalid format")
    return _normalize_team_id(raw_team_id), _normalize_group_name(raw_group_name)

def _resolve_task_team_id(task_data: dict[str, Any]) -> str:
    task_id = str(task_data["id"])
    metadata = cast(dict[str, Any], task_data["metadata"])
    return effective_team_id(task_id=task_id, metadata=metadata)

def _group_thread_id(*, team_id: str, group_name: str) -> str:
    return f"group:{team_id}:{group_name}"

def _direct_thread_id(*, team_id: str, sender_task_id: str, to_task_id: str) -> str:
    left, right = sorted([sender_task_id, to_task_id])
    return f"dm:{team_id}:{left}:{right}"

def _human_direct_thread_id(*, owner_id: str, to_task_id: str) -> str:
    return f"human:{owner_id}:{to_task_id}"

def _delivery_report(result: MessagingSendScriptResult) -> dict[str, Any]:
    return {
        "thread_message_id": result.thread_message_id,
        "global_message_id": result.global_message_id,
        "delivered_task_ids": result.delivered_task_ids,
        "skipped_inactive_task_ids": result.skipped_inactive_task_ids,
        "failed_task_ids": result.failed_task_ids,
    }


def _normalize_history_limit(limit: int) -> int:
    if not isinstance(limit, int):
        raise TypeError("limit must be an integer")
    if limit < 1:
        raise ValueError("limit must be >= 1")
    return min(limit, _MAX_HISTORY_LIMIT)


def _normalize_list_limit(limit: int) -> int:
    if not isinstance(limit, int):
        raise TypeError("limit must be an integer")
    if limit < 1:
        raise ValueError("limit must be >= 1")
    return min(limit, _MAX_LIST_LIMIT)


def _normalize_history_order(order: str) -> Literal["asc", "desc"]:
    if order not in {"asc", "desc"}:
        raise ValueError("order must be 'asc' or 'desc'")
    return cast(Literal["asc", "desc"], order)


def _decode_list_cursor(cursor: str | None) -> int:
    if cursor is None:
        return 0
    if not isinstance(cursor, str) or not cursor:
        raise ValueError("cursor must be a non-empty string when provided")
    try:
        offset = int(cursor)
    except ValueError as exc:
        raise ValueError("cursor has invalid format") from exc
    if offset < 0:
        raise ValueError("cursor has invalid format")
    return offset


def _decode_message_payload(raw_payload: Any) -> dict[str, Any]:
    if raw_payload is None:
        return {}
    try:
        text = decode(raw_payload)
        parsed = json.loads(text)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return cast(dict[str, Any], parsed)


def _coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _coerce_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return cast(dict[str, Any], value)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


async def _touch_group_thread_index(
    *,
    redis_client: redis.Redis,
    namespace: str,
    team_id: str,
    thread_id: str,
) -> None:
    keys = RedisKeys.format(namespace=namespace)
    try:
        await redis_client.zadd(  # type: ignore[misc]
            keys.messaging_group_threads_by_team(team_id),
            {thread_id: int(time.time() * 1000)},
        )
    except Exception as exc:  # pragma: no cover - best effort index update
        logger.warning(
            "Failed to update group thread index for %s",
            thread_id,
            exc_info=exc,
        )


async def _touch_direct_thread_index(
    *,
    redis_client: redis.Redis,
    namespace: str,
    team_id: str,
    thread_id: str,
) -> None:
    keys = RedisKeys.format(namespace=namespace)
    try:
        await redis_client.zadd(  # type: ignore[misc]
            keys.messaging_direct_threads_by_team(team_id),
            {thread_id: int(time.time() * 1000)},
        )
    except Exception as exc:  # pragma: no cover - best effort index update
        logger.warning(
            "Failed to update direct thread index for %s",
            thread_id,
            exc_info=exc,
        )

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

async def _resolve_human_group_target(
    *,
    redis_client: redis.Redis,
    namespace: str,
    group_id: str | None,
    group_name: str | None,
    task_id: str | None,
    team_id: str | None,
) -> tuple[str, str, str | None, dict[str, Any] | None]:
    anchor_task_data: dict[str, Any] | None = None
    if group_id is not None:
        resolved_team_id, resolved_group_name = _decode_group_id(group_id)
        if (
            group_name is not None
            and _normalize_group_name(group_name) != resolved_group_name
        ):
            raise ValueError("group_id and group_name refer to different groups")
        if team_id is not None and _normalize_team_id(team_id) != resolved_team_id:
            raise MessagingScopeError("group_id and team_id scope mismatch")
        if task_id is not None:
            anchor_task_data = await get_task_data(redis_client, namespace, task_id)
            anchor_team_id = _resolve_task_team_id(anchor_task_data)
            if anchor_team_id != resolved_team_id:
                raise MessagingScopeError("task_id scope does not match group_id")
        return resolved_team_id, resolved_group_name, task_id, anchor_task_data

    if group_name is None:
        raise ValueError(
            "Target requires group_id, or group_name with task_id/team_id"
        )
    normalized_group_name = _normalize_group_name(group_name)
    if task_id is not None:
        anchor_task_data = await get_task_data(redis_client, namespace, task_id)
        anchor_team_id = _resolve_task_team_id(anchor_task_data)
        if team_id is not None and _normalize_team_id(team_id) != anchor_team_id:
            raise MessagingScopeError("task_id scope does not match team_id")
        return anchor_team_id, normalized_group_name, task_id, anchor_task_data
    if team_id is not None:
        return _normalize_team_id(team_id), normalized_group_name, None, None
    raise ValueError("Target requires group_id, or group_name with task_id/team_id")

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


async def _read_thread_history_page(
    *,
    redis_client: redis.Redis,
    thread_history_key: str,
    limit: int,
    before: str | None,
    after: str | None,
    order: Literal["asc", "desc"],
) -> dict[str, Any]:
    if before is not None and after is not None:
        raise ValueError("Specify only one of before or after")

    normalized_limit = _normalize_history_limit(limit)
    fetch_limit = normalized_limit + 1

    if order == "desc":
        max_bound = f"({before}" if before is not None else "+"
        min_bound = f"({after}" if after is not None else "-"
        rows = cast(
            list[tuple[str, dict[str, Any]]],
            await redis_client.xrevrange(  # type: ignore[misc]
                thread_history_key,
                max=max_bound,
                min=min_bound,
                count=fetch_limit,
            ),
        )
    else:
        min_bound = f"({after}" if after is not None else "-"
        max_bound = f"({before}" if before is not None else "+"
        rows = cast(
            list[tuple[str, dict[str, Any]]],
            await redis_client.xrange(  # type: ignore[misc]
                thread_history_key,
                min=min_bound,
                max=max_bound,
                count=fetch_limit,
            ),
        )

    has_more = len(rows) > normalized_limit
    if has_more:
        rows = rows[:normalized_limit]

    messages: list[dict[str, Any]] = []
    for message_id, fields in rows:
        payload = _decode_message_payload(fields.get("payload"))
        messages.append({"message_id": message_id, "payload": payload})

    next_before: str | None = None
    next_after: str | None = None
    if rows:
        last_row_message_id = rows[-1][0]
        if has_more and order == "desc":
            next_before = last_row_message_id
        if has_more and order == "asc":
            next_after = last_row_message_id

    return {
        "messages": messages,
        "next_before": next_before,
        "next_after": next_after,
        "has_more": has_more,
    }

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
    group_id = _encode_group_id(
        team_id=sender_team_id,
        group_name=normalized_group_name,
    )
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
                "group_id": group_id,
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
        "group_id": group_id,
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
    resolved_group_id = cast(
        str,
        group_meta.get("group_id")
        or _encode_group_id(team_id=sender_team_id, group_name=normalized_group_name),
    )
    return {
        "team_id": sender_team_id,
        "group_id": resolved_group_id,
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
                "group_id": cast(
                    str,
                    meta.get("group_id")
                    or _encode_group_id(team_id=sender_team_id, group_name=name),
                ),
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


async def messaging_groups_history(
    *,
    redis_client: redis.Redis,
    namespace: str,
    group_id: str | None = None,
    team_id: str | None = None,
    group_name: str | None = None,
    limit: int = _DEFAULT_HISTORY_LIMIT,
    before: str | None = None,
    after: str | None = None,
    order: Literal["asc", "desc"] = "desc",
) -> dict[str, Any]:
    if group_id is not None:
        resolved_team_id, resolved_group_name = _decode_group_id(group_id)
        if team_id is not None and _normalize_team_id(team_id) != resolved_team_id:
            raise MessagingScopeError("group_id and team_id scope mismatch")
        if (
            group_name is not None
            and _normalize_group_name(group_name) != resolved_group_name
        ):
            raise ValueError("group_id and group_name refer to different groups")
    else:
        if team_id is None or group_name is None:
            raise ValueError("Provide group_id, or both team_id and group_name")
        resolved_team_id = _normalize_team_id(team_id)
        resolved_group_name = _normalize_group_name(group_name)

    normalized_order = _normalize_history_order(order)
    keys = RedisKeys.format(namespace=namespace)
    group_meta_key = keys.messaging_group_meta(resolved_team_id)
    raw_meta = await redis_client.hget(group_meta_key, resolved_group_name)  # type: ignore[misc]
    group_meta = _decode_group_meta(
        raw_meta=raw_meta,
        group_name=resolved_group_name,
        team_id=resolved_team_id,
    )

    thread_id = _group_thread_id(
        team_id=resolved_team_id,
        group_name=resolved_group_name,
    )
    history = await _read_thread_history_page(
        redis_client=redis_client,
        thread_history_key=keys.messaging_thread_history(thread_id),
        limit=limit,
        before=before,
        after=after,
        order=normalized_order,
    )
    resolved_group_id = cast(
        str,
        group_meta.get("group_id")
        or _encode_group_id(team_id=resolved_team_id, group_name=resolved_group_name),
    )

    messages: list[dict[str, Any]] = []
    for message in cast(list[dict[str, Any]], history["messages"]):
        payload = cast(dict[str, Any], message["payload"])
        from_task_id_raw = payload.get("from_task_id")
        from_owner_id_raw = payload.get("from_owner_id")
        messages.append(
            {
                "message_id": str(message["message_id"]),
                "thread_id": thread_id,
                "team_id": resolved_team_id,
                "group_id": resolved_group_id,
                "group_name": resolved_group_name,
                "from_task_id": (
                    str(from_task_id_raw) if isinstance(from_task_id_raw, str) else None
                ),
                "from_owner_id": (
                    str(from_owner_id_raw)
                    if isinstance(from_owner_id_raw, str)
                    else None
                ),
                "to_task_ids": _coerce_string_list(payload.get("to_task_ids")),
                "delivered_task_ids": _coerce_string_list(
                    payload.get("delivered_task_ids")
                ),
                "skipped_inactive_task_ids": _coerce_string_list(
                    payload.get("skipped_inactive_task_ids")
                ),
                "failed_task_ids": _coerce_string_list(payload.get("failed_task_ids")),
                "content": str(payload.get("content", "")),
                "metadata": _coerce_dict(payload.get("metadata")),
                "created_at": _coerce_float(payload.get("created_at")),
            }
        )

    return {
        "team_id": resolved_team_id,
        "group_id": resolved_group_id,
        "group_name": resolved_group_name,
        "thread_id": thread_id,
        "messages": messages,
        "next_before": history["next_before"],
        "next_after": history["next_after"],
        "has_more": history["has_more"],
    }


async def messaging_groups_list_threads(
    *,
    redis_client: redis.Redis,
    namespace: str,
    team_id: str,
    limit: int = _DEFAULT_LIST_LIMIT,
    cursor: str | None = None,
) -> dict[str, Any]:
    normalized_team_id = _normalize_team_id(team_id)
    normalized_limit = _normalize_list_limit(limit)
    offset = _decode_list_cursor(cursor)

    keys = RedisKeys.format(namespace=namespace)
    index_key = keys.messaging_group_threads_by_team(normalized_team_id)
    raw_thread_ids = cast(
        list[str | bytes],
        await redis_client.zrevrange(  # type: ignore[misc]
            index_key,
            offset,
            offset + normalized_limit,
        ),
    )
    thread_ids = [decode(thread_id) for thread_id in raw_thread_ids]
    has_more = len(thread_ids) > normalized_limit
    if has_more:
        thread_ids = thread_ids[:normalized_limit]
    next_cursor = str(offset + normalized_limit) if has_more else None

    pipe = redis_client.pipeline(transaction=False)
    for thread_id in thread_ids:
        pipe.xrevrange(keys.messaging_thread_history(thread_id), "+", "-", count=1)
    latest_rows = cast(list[list[tuple[str, dict[str, Any]]]], await pipe.execute())

    prefix = f"group:{normalized_team_id}:"
    conversations: list[dict[str, Any]] = []
    for thread_id, rows in zip(thread_ids, latest_rows, strict=True):
        if not thread_id.startswith(prefix):
            continue
        resolved_group_name = thread_id[len(prefix) :]
        if not resolved_group_name:
            continue
        last_message_id: str | None = None
        last_message_at: float | None = None
        last_message_preview: str | None = None
        if rows:
            last_message_id = rows[0][0]
            payload = _decode_message_payload(rows[0][1].get("payload"))
            last_message_at = _coerce_float(payload.get("created_at"))
            content = payload.get("content")
            if isinstance(content, str) and content:
                last_message_preview = content

        conversations.append(
            {
                "team_id": normalized_team_id,
                "group_id": _encode_group_id(
                    team_id=normalized_team_id,
                    group_name=resolved_group_name,
                ),
                "group_name": resolved_group_name,
                "thread_id": thread_id,
                "last_message_id": last_message_id,
                "last_message_at": last_message_at,
                "last_message_preview": last_message_preview,
            }
        )

    return {
        "conversations": conversations,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


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


async def messaging_groups_remove_members(
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

    script = await create_messaging_group_remove_members_script(redis_client)
    result: MessagingGroupMutationScriptResult = await script.execute(
        task_metas_key=keys.task_meta,
        group_meta_key=group_meta_key,
        group_members_key=group_members_key,
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
            "Unexpected messaging_group_remove_members decision "
            f"'{result.decision}'"
        )

    await _publish_messaging_event(
        redis_client=redis_client,
        namespace=namespace,
        owner_id=sender_owner_id,
        task_id=sender_task_id,
        agent_name=sender_agent_name,
        event_type="messaging_group_members_removed",
        data={
            "team_id": sender_team_id,
            "group_name": normalized_group_name,
            "removed_member_task_ids": result.member_task_ids,
        },
    )
    return result.member_task_ids


async def messaging_groups_leave(
    *,
    redis_client: redis.Redis,
    namespace: str,
    sender_task_id: str,
    group_name: str,
) -> bool:
    try:
        removed_member_task_ids = await messaging_groups_remove_members(
            redis_client=redis_client,
            namespace=namespace,
            sender_task_id=sender_task_id,
            group_name=group_name,
            member_task_ids=[sender_task_id],
        )
    except MessagingPermissionError:
        # Idempotent leave: already left returns False.
        return False
    return sender_task_id in removed_member_task_ids


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
        scheduled_wait_meta_key=keys.scheduled_wait_meta,
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
        queue_scheduled_key_template=agent_queue_key_template.queue_scheduled,
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

    report = _delivery_report(result)
    await _touch_group_thread_index(
        redis_client=redis_client,
        namespace=namespace,
        team_id=sender_team_id,
        thread_id=thread_id,
    )
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
        scheduled_wait_meta_key=keys.scheduled_wait_meta,
        sender_task_id=sender_task_id,
        to_task_id=to_task_id,
        team_id=sender_team_id,
        content=content,
        metadata_json=json.dumps(serialize_data(metadata or {}), sort_keys=True),
        steering_key_template=steering_key_template,
        history_maxlen=_MESSAGING_HISTORY_MAXLEN,
        queue_main_key_template=agent_queue_key_template.queue_main,
        queue_pending_key_template=agent_queue_key_template.queue_pending,
        queue_scheduled_key_template=agent_queue_key_template.queue_scheduled,
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

    report = _delivery_report(result)
    await _touch_direct_thread_index(
        redis_client=redis_client,
        namespace=namespace,
        team_id=sender_team_id,
        thread_id=thread_id,
    )
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


async def messaging_direct_history(
    *,
    redis_client: redis.Redis,
    namespace: str,
    task_a_id: str,
    task_b_id: str,
    limit: int = _DEFAULT_HISTORY_LIMIT,
    before: str | None = None,
    after: str | None = None,
    order: Literal["asc", "desc"] = "desc",
) -> dict[str, Any]:
    if not isinstance(task_a_id, str) or not task_a_id:
        raise ValueError("task_a_id must be a non-empty string")
    if not isinstance(task_b_id, str) or not task_b_id:
        raise ValueError("task_b_id must be a non-empty string")
    if task_a_id == task_b_id:
        raise ValueError("task_a_id and task_b_id must be different")

    task_a_data = await get_task_data(redis_client, namespace, task_a_id)
    task_b_data = await get_task_data(redis_client, namespace, task_b_id)
    team_a_id = _resolve_task_team_id(task_a_data)
    team_b_id = _resolve_task_team_id(task_b_data)
    if team_a_id != team_b_id:
        raise MessagingScopeError("Direct history requires tasks in the same team")
    normalized_order = _normalize_history_order(order)
    canonical_task_a_id, canonical_task_b_id = sorted([task_a_id, task_b_id])
    thread_id = _direct_thread_id(
        team_id=team_a_id,
        sender_task_id=canonical_task_a_id,
        to_task_id=canonical_task_b_id,
    )
    keys = RedisKeys.format(namespace=namespace)
    history = await _read_thread_history_page(
        redis_client=redis_client,
        thread_history_key=keys.messaging_thread_history(thread_id),
        limit=limit,
        before=before,
        after=after,
        order=normalized_order,
    )

    messages: list[dict[str, Any]] = []
    for message in cast(list[dict[str, Any]], history["messages"]):
        payload = cast(dict[str, Any], message["payload"])
        from_task_id_raw = payload.get("from_task_id")
        messages.append(
            {
                "message_id": str(message["message_id"]),
                "thread_id": thread_id,
                "team_id": team_a_id,
                "task_a_id": canonical_task_a_id,
                "task_b_id": canonical_task_b_id,
                "from_task_id": (
                    str(from_task_id_raw) if isinstance(from_task_id_raw, str) else None
                ),
                "to_task_ids": _coerce_string_list(payload.get("to_task_ids")),
                "delivered_task_ids": _coerce_string_list(
                    payload.get("delivered_task_ids")
                ),
                "skipped_inactive_task_ids": _coerce_string_list(
                    payload.get("skipped_inactive_task_ids")
                ),
                "failed_task_ids": _coerce_string_list(payload.get("failed_task_ids")),
                "content": str(payload.get("content", "")),
                "metadata": _coerce_dict(payload.get("metadata")),
                "created_at": _coerce_float(payload.get("created_at")),
            }
        )

    return {
        "team_id": team_a_id,
        "thread_id": thread_id,
        "task_a_id": canonical_task_a_id,
        "task_b_id": canonical_task_b_id,
        "messages": messages,
        "next_before": history["next_before"],
        "next_after": history["next_after"],
        "has_more": history["has_more"],
    }


async def messaging_direct_list_threads(
    *,
    redis_client: redis.Redis,
    namespace: str,
    team_id: str,
    limit: int = _DEFAULT_LIST_LIMIT,
    cursor: str | None = None,
) -> dict[str, Any]:
    normalized_team_id = _normalize_team_id(team_id)
    normalized_limit = _normalize_list_limit(limit)
    offset = _decode_list_cursor(cursor)

    keys = RedisKeys.format(namespace=namespace)
    index_key = keys.messaging_direct_threads_by_team(normalized_team_id)
    raw_thread_ids = cast(
        list[str | bytes],
        await redis_client.zrevrange(  # type: ignore[misc]
            index_key,
            offset,
            offset + normalized_limit,
        ),
    )
    thread_ids = [decode(thread_id) for thread_id in raw_thread_ids]
    has_more = len(thread_ids) > normalized_limit
    if has_more:
        thread_ids = thread_ids[:normalized_limit]
    next_cursor = str(offset + normalized_limit) if has_more else None

    pipe = redis_client.pipeline(transaction=False)
    for thread_id in thread_ids:
        pipe.xrevrange(keys.messaging_thread_history(thread_id), "+", "-", count=1)
    latest_rows = cast(list[list[tuple[str, dict[str, Any]]]], await pipe.execute())

    prefix = f"dm:{normalized_team_id}:"
    conversations: list[dict[str, Any]] = []
    for thread_id, rows in zip(thread_ids, latest_rows, strict=True):
        if not thread_id.startswith(prefix):
            continue
        participants = thread_id[len(prefix) :].split(":", 1)
        if len(participants) != 2:
            continue
        task_a_id, task_b_id = participants
        if not task_a_id or not task_b_id:
            continue

        last_message_id: str | None = None
        last_message_at: float | None = None
        last_message_preview: str | None = None
        if rows:
            last_message_id = rows[0][0]
            payload = _decode_message_payload(rows[0][1].get("payload"))
            last_message_at = _coerce_float(payload.get("created_at"))
            content = payload.get("content")
            if isinstance(content, str) and content:
                last_message_preview = content

        conversations.append(
            {
                "team_id": normalized_team_id,
                "task_a_id": task_a_id,
                "task_b_id": task_b_id,
                "thread_id": thread_id,
                "last_message_id": last_message_id,
                "last_message_at": last_message_at,
                "last_message_preview": last_message_preview,
            }
        )

    return {
        "conversations": conversations,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


async def messaging_human_send_direct(
    *,
    redis_client: redis.Redis,
    namespace: str,
    owner_id: str,
    to_task_id: str,
    content: str,
    metadata: dict[str, Any] | None = None,
    from_task_id: str | None = None,
) -> dict[str, Any]:
    normalized_owner_id = _normalize_owner_id(owner_id)
    normalized_content = _normalize_content(content)
    target_task_data = await get_task_data(redis_client, namespace, to_task_id)
    recipient_team_id = _resolve_task_team_id(target_task_data)
    recipient_agent_name = str(target_task_data["agent"])

    if from_task_id is not None:
        from_task_data = await get_task_data(redis_client, namespace, from_task_id)
        from_team_id = _resolve_task_team_id(from_task_data)
        if from_team_id != recipient_team_id:
            raise MessagingScopeError(
                "from_task_id scope does not match recipient scope"
            )

    keys = RedisKeys.format(namespace=namespace)
    thread_id = _human_direct_thread_id(
        owner_id=normalized_owner_id,
        to_task_id=to_task_id,
    )
    script = await create_messaging_human_direct_send_script(redis_client)
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
        scheduled_wait_meta_key=keys.scheduled_wait_meta,
        to_task_id=to_task_id,
        team_id=recipient_team_id,
        content=normalized_content,
        metadata_json=json.dumps(serialize_data(metadata or {}), sort_keys=True),
        steering_key_template=steering_key_template,
        history_maxlen=_MESSAGING_HISTORY_MAXLEN,
        queue_main_key_template=agent_queue_key_template.queue_main,
        queue_pending_key_template=agent_queue_key_template.queue_pending,
        queue_scheduled_key_template=agent_queue_key_template.queue_scheduled,
        from_owner_id=normalized_owner_id,
        from_task_id=from_task_id,
    )
    if result.decision == "recipient_not_found":
        raise TaskNotFoundError(to_task_id)
    if result.decision == "recipient_scope_mismatch":
        raise MessagingScopeError("Direct message crosses team scope")
    if result.decision == "invalid_owner":
        raise ValueError("owner_id must be a non-empty string")
    if result.decision != "sent":
        raise RuntimeError(
            f"Unexpected messaging_human_direct_send decision '{result.decision}'"
        )

    report = _delivery_report(result)
    payload = {
        "team_id": recipient_team_id,
        "to_task_id": to_task_id,
        "from_owner_id": normalized_owner_id,
        "from_task_id": from_task_id,
        "thread_id": thread_id,
        **report,
    }
    await _publish_messaging_event(
        redis_client=redis_client,
        namespace=namespace,
        owner_id=normalized_owner_id,
        task_id=to_task_id,
        agent_name=recipient_agent_name,
        event_type="messaging_human_direct_message_sent",
        data=payload,
    )
    if result.skipped_inactive_task_ids or result.failed_task_ids:
        await _publish_messaging_event(
            redis_client=redis_client,
            namespace=namespace,
            owner_id=normalized_owner_id,
            task_id=to_task_id,
            agent_name=recipient_agent_name,
            event_type="messaging_delivery_partial",
            data=payload,
        )
    return payload

async def messaging_human_send_group(
    *,
    redis_client: redis.Redis,
    namespace: str,
    owner_id: str,
    content: str,
    group_id: str | None = None,
    group_name: str | None = None,
    task_id: str | None = None,
    team_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_owner_id = _normalize_owner_id(owner_id)
    normalized_content = _normalize_content(content)
    (
        resolved_team_id,
        resolved_group_name,
        anchor_task_id,
        anchor_task_data,
    ) = await _resolve_human_group_target(
        redis_client=redis_client,
        namespace=namespace,
        group_id=group_id,
        group_name=group_name,
        task_id=task_id,
        team_id=team_id,
    )
    resolved_group_id = _encode_group_id(
        team_id=resolved_team_id,
        group_name=resolved_group_name,
    )
    thread_id = _group_thread_id(
        team_id=resolved_team_id,
        group_name=resolved_group_name,
    )
    keys = RedisKeys.format(namespace=namespace)
    group_meta_key = keys.messaging_group_meta(resolved_team_id)
    group_members_key = keys.messaging_group_members(
        resolved_team_id,
        resolved_group_name,
    )
    script = await create_messaging_human_group_send_script(redis_client)
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
        scheduled_wait_meta_key=keys.scheduled_wait_meta,
        team_tasks_key=keys.messaging_team_tasks(resolved_team_id),
        team_id=resolved_team_id,
        group_name=resolved_group_name,
        content=normalized_content,
        metadata_json=json.dumps(serialize_data(metadata or {}), sort_keys=True),
        steering_key_template=steering_key_template,
        history_maxlen=_MESSAGING_HISTORY_MAXLEN,
        queue_main_key_template=agent_queue_key_template.queue_main,
        queue_pending_key_template=agent_queue_key_template.queue_pending,
        queue_scheduled_key_template=agent_queue_key_template.queue_scheduled,
        groups_by_task_key_template=keys.messaging_groups_by_task("{task_id}"),
        from_owner_id=normalized_owner_id,
        from_task_id=anchor_task_id,
    )

    if result.decision == "group_not_found":
        raise MessagingGroupNotFoundError(resolved_group_name, resolved_team_id)
    if result.decision == "invalid_owner":
        raise ValueError("owner_id must be a non-empty string")
    if result.decision != "sent":
        raise RuntimeError(
            f"Unexpected messaging_human_group_send decision '{result.decision}'"
        )

    report = _delivery_report(result)
    await _touch_group_thread_index(
        redis_client=redis_client,
        namespace=namespace,
        team_id=resolved_team_id,
        thread_id=thread_id,
    )
    event_task_id = (
        anchor_task_id
        or (result.delivered_task_ids[0] if result.delivered_task_ids else thread_id)
    )
    event_agent_name = (
        str(anchor_task_data["agent"]) if anchor_task_data is not None else "human"
    )
    payload = {
        "team_id": resolved_team_id,
        "group_id": resolved_group_id,
        "group_name": resolved_group_name,
        "from_owner_id": normalized_owner_id,
        "from_task_id": anchor_task_id,
        "thread_id": thread_id,
        **report,
    }
    await _publish_messaging_event(
        redis_client=redis_client,
        namespace=namespace,
        owner_id=normalized_owner_id,
        task_id=event_task_id,
        agent_name=event_agent_name,
        event_type="messaging_human_group_message_sent",
        data=payload,
    )
    if result.skipped_inactive_task_ids or result.failed_task_ids:
        await _publish_messaging_event(
            redis_client=redis_client,
            namespace=namespace,
            owner_id=normalized_owner_id,
            task_id=event_task_id,
            agent_name=event_agent_name,
            event_type="messaging_delivery_partial",
            data=payload,
        )
    return payload
