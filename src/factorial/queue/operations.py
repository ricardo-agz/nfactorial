import asyncio
import hashlib
import json
import random
import secrets
import time
import uuid
from dataclasses import dataclass
from datetime import timezone
from typing import Any, Literal, cast

import redis.asyncio as redis
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)

from factorial.agent import BaseAgent, ExecutionContext
from factorial.context import VerificationState, execution_context
from factorial.events import AgentEvent, BatchEvent, EventPublisher
from factorial.exceptions import (
    HookAlreadyResolvedError,
    HookExpiredError,
    HookNotFoundError,
    HookTokenValidationError,
    InactiveTaskError,
    InvalidTaskIdError,
    TaskNotFoundError,
)
from factorial.hooks import (
    HookRecord,
    HookRequestContext,
    HookResolutionResult,
    HookSessionRecord,
    PendingHook,
    build_request_builder_kwargs,
)
from factorial.logging import colored, get_logger
from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    BatchPickupScript,
    BatchPickupScriptResult,
    CancelTaskScriptResult,
    EnqueueBatchScript,
    create_cancel_task_script,
    create_child_task_completion_script,
    create_enqueue_batch_script,
    create_enqueue_task_script,
    create_hook_resolve_script,
    create_hook_wake_script,
    create_resume_enqueue_script,
)
from factorial.queue.task import (
    Batch,
    BatchMetadata,
    ContextType,
    Task,
    TaskStatus,
    get_batch_data,
    get_task_agent,
    get_task_data,
    get_task_status,
)
from factorial.tools import _ToolResultInternal
from factorial.utils import decode, is_valid_task_id, serialize_data

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


def _hash_hook_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _resume_request_hash(*, messages: list[dict[str, Any]]) -> str:
    messages_json = json.dumps(
        serialize_data(messages),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(messages_json.encode("utf-8")).hexdigest()


def _hook_resolution_request_hash(*, payload: Any) -> str:
    payload_json = json.dumps(
        serialize_data(payload),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _enqueue_request_hash(*, agent_name: str, task: Task[Any]) -> str:
    payload = (
        serialize_data(task.payload.to_dict())
        if task.payload is not None
        else {}
    )
    request_envelope = {
        "agent_name": agent_name,
        "owner_id": task.metadata.owner_id,
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
    payloads: list[ContextType],
    task_ids: list[str] | None,
    batch_id: str | None,
) -> str:
    request_envelope = {
        "agent_name": agent_name,
        "owner_id": owner_id,
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


async def _load_hook_session(
    redis_client: redis.Redis,
    *,
    keys: RedisKeys,
    session_id: str,
) -> HookSessionRecord | None:
    session_json = await redis_client.hget(keys.hook_sessions, session_id)  # type: ignore[misc]
    if not session_json:
        return None
    return HookSessionRecord.from_json(session_json)


async def _save_hook_session(
    redis_client: redis.Redis,
    *,
    keys: RedisKeys,
    session: HookSessionRecord,
) -> None:
    await redis_client.hset(keys.hook_sessions, session.session_id, session.to_json())  # type: ignore[misc]


async def _clear_pending_tool_call_runtime_state(
    redis_client: redis.Redis,
    *,
    keys: RedisKeys,
    tool_call_id: str,
) -> None:
    """Clear task-scoped pending hook runtime markers for one tool call."""
    pipe = redis_client.pipeline(transaction=True)
    pipe.hdel(keys.pending_tool_results, tool_call_id)
    pipe.hdel(keys.hook_runtime_ready, tool_call_id)
    pipe.hdel(keys.hook_session_by_tool_call, tool_call_id)
    await pipe.execute()


async def _finalize_hook_session_state(
    redis_client: redis.Redis,
    *,
    keys: RedisKeys,
    session: HookSessionRecord,
    status: Literal["completed", "failed", "expired"],
) -> None:
    """Persist final session status and clear pending runtime state."""
    session.status = status
    session.updated_at = time.time()
    await _save_hook_session(redis_client=redis_client, keys=keys, session=session)
    await _clear_pending_tool_call_runtime_state(
        redis_client=redis_client,
        keys=keys,
        tool_call_id=session.tool_call_id,
    )


async def persist_hook_runtime_payload(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    runtime_payload: dict[str, Any],
) -> None:
    """Persist hook session metadata emitted by tool runtime."""
    if runtime_payload.get("kind") != "hook_session_init":
        return

    task_data = await get_task_data(redis_client, namespace, task_id)
    task_status = TaskStatus(task_data["status"])
    if _is_terminal_status(task_status):
        raise InactiveTaskError(task_id)

    agent_name = task_data["agent"]
    owner_id = task_data["metadata"]["owner_id"]
    task_keys = RedisKeys.format(
        namespace=namespace,
        task_id=task_id,
        agent=agent_name,
        owner_id=owner_id,
    )
    session = HookSessionRecord.from_dict(runtime_payload["session"])
    if session.task_id != task_id:
        raise ValueError(
            f"Hook session task mismatch: payload={session.task_id}, task={task_id}"
        )

    existing_session_id = await redis_client.hget(  # type: ignore[misc]
        task_keys.hook_session_by_tool_call, session.tool_call_id
    )
    if existing_session_id:
        # Idempotent replay (e.g. worker retry): keep the existing session.
        return

    requested_hooks = cast(
        list[dict[str, Any]], runtime_payload.get("requested_hooks", [])
    )
    hook_records: list[HookRecord] = []
    for requested in requested_hooks:
        expires_at = float(requested["expires_at"])
        hook_records.append(
            HookRecord(
                hook_id=str(requested["hook_id"]),
                task_id=task_id,
                tool_call_id=session.tool_call_id,
                agent_name=agent_name,
                owner_id=owner_id,
                hook_type=str(requested.get("hook_type", "Hook")),
                mode=cast(
                    Literal["requires", "awaits"],
                    requested.get("mode", "requires"),
                ),
                tool_name=session.tool_name,
                tool_args=session.tool_args,
                hook_param_name=str(requested["param_name"]),
                depends_on=tuple(str(dep) for dep in requested.get("depends_on", [])),
                session_id=session.session_id,
                status="requested",
                submit_url=(
                    str(requested["submit_url"])
                    if requested.get("submit_url") is not None
                    else None
                ),
                metadata=dict(requested.get("metadata") or {}),
                created_at=time.time(),
                expires_at=expires_at,
                resolved_at=None,
                payload=None,
                token_hashes=[_hash_hook_token(str(requested["token"]))],
                token_version=1,
            )
        )

    pipe = redis_client.pipeline(transaction=True)
    pipe.hset(task_keys.hook_sessions, session.session_id, session.to_json())
    pipe.hset(
        task_keys.hook_session_by_tool_call,
        session.tool_call_id,
        session.session_id,
    )
    pipe.sadd(task_keys.hook_sessions_by_task, session.session_id)
    pipe.hsetnx(task_keys.pending_tool_results, session.tool_call_id, PENDING_SENTINEL)  # type: ignore[arg-type]
    for record in hook_records:
        pipe.hset(task_keys.hooks_index, record.hook_id, record.to_json())
        pipe.sadd(task_keys.hooks_by_task, record.hook_id)
        pipe.zadd(task_keys.hooks_expiring, {record.hook_id: record.expires_at})  # type: ignore[arg-type]
    await pipe.execute()


async def register_pending_hook(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    tool_call_id: str,
    pending_hook: PendingHook[Any],
    *,
    mode: Literal["requires", "awaits"] = "requires",
    session_id: str,
    tool_name: str,
    tool_args: dict[str, Any] | None = None,
    hook_param_name: str,
    depends_on: tuple[str, ...] | None = None,
    hook_type_name: str | None = None,
) -> bool:
    """Register a pending hook ticket and persist token metadata.

    Returns True when this call creates a new hook record.
    Returns False when the exact hook record already exists.
    """
    if not session_id:
        raise ValueError("register_pending_hook requires a non-empty session_id")

    task_data = await get_task_data(redis_client, namespace, task_id)
    task_status = TaskStatus(task_data["status"])
    if _is_terminal_status(task_status):
        raise InactiveTaskError(task_id)
    if task_status not in {TaskStatus.PENDING_TOOL_RESULTS, TaskStatus.PROCESSING}:
        raise ValueError(
            "register_pending_hook requires task status in "
            f"{{'{TaskStatus.PENDING_TOOL_RESULTS.value}', "
            f"'{TaskStatus.PROCESSING.value}'}}, "
            f"got '{task_status.value}' for task {task_id}"
        )

    agent_name = task_data["agent"]
    owner_id = task_data["metadata"]["owner_id"]
    task_keys = RedisKeys.format(
        namespace=namespace,
        task_id=task_id,
        agent=agent_name,
        owner_id=owner_id,
    )

    existing_json = await redis_client.hget(task_keys.hooks_index, pending_hook.hook_id)  # type: ignore[misc]
    if existing_json:
        existing = HookRecord.from_json(existing_json)
        if (
            existing.task_id != task_id
            or existing.tool_call_id != tool_call_id
        ):
            raise ValueError(
                f"Hook {pending_hook.hook_id} is already registered for "
                "another task or tool call."
            )
        return False

    expires_at = pending_hook.expires_at.astimezone(timezone.utc).timestamp()
    created_at = time.time()

    hook_record = HookRecord(
        hook_id=pending_hook.hook_id,
        task_id=task_id,
        tool_call_id=tool_call_id,
        agent_name=agent_name,
        owner_id=owner_id,
        hook_type=hook_type_name or pending_hook.hook_type.__name__,
        mode=mode,
        tool_name=tool_name,
        tool_args=dict(tool_args or {}),
        hook_param_name=hook_param_name,
        depends_on=tuple(depends_on or ()),
        session_id=session_id,
        status="requested",
        submit_url=pending_hook.submit_url,
        metadata=pending_hook.metadata,
        created_at=created_at,
        expires_at=expires_at,
        resolved_at=None,
        payload=None,
        token_hashes=[_hash_hook_token(pending_hook.token)],
        token_version=1,
    )

    pipe = redis_client.pipeline(transaction=True)
    pipe.hsetnx(task_keys.pending_tool_results, tool_call_id, PENDING_SENTINEL)  # type: ignore[arg-type]
    pipe.hset(
        task_keys.hooks_index,
        pending_hook.hook_id,
        hook_record.to_json(),
    )
    pipe.sadd(task_keys.hooks_by_task, pending_hook.hook_id)
    pipe.zadd(task_keys.hooks_expiring, {pending_hook.hook_id: expires_at})  # type: ignore[arg-type]
    await pipe.execute()
    return True


@dataclass
class HookRuntimeTickOutcome:
    had_wake_requests: bool
    should_repark: bool
    pending_tool_call_ids: list[str]
    completed_results: list[tuple[str, Any]]


async def _pending_sentinel_tool_call_ids(
    redis_client: redis.Redis,
    *,
    task_keys: RedisKeys,
) -> list[str]:
    pending_map = cast(
        dict[str | bytes, str | bytes],
        await redis_client.hgetall(task_keys.pending_tool_results),  # type: ignore
    )
    ids: list[str] = []
    for raw_tool_call_id, raw_result in pending_map.items():
        tool_call_id = decode(raw_tool_call_id)
        result_str = decode(raw_result)
        if result_str == PENDING_SENTINEL:
            ids.append(tool_call_id)
    return sorted(ids)


async def _execute_hook_tool_continuation(
    *,
    agent: BaseAgent[Any],
    task: Task[Any],
    execution_ctx: ExecutionContext,
    session: HookSessionRecord,
) -> Any | _ToolResultInternal:
    continuation_args = dict(session.tool_args)
    for node in session.nodes.values():
        if node.status == "resolved":
            continuation_args[node.param_name] = node.payload

    synthetic_tool_call = ChatCompletionMessageFunctionToolCall(
        id=session.tool_call_id,
        type="function",
        function=ToolCallFunction(
            name=session.tool_name,
            arguments=json.dumps(continuation_args, default=str),
        ),
    )

    token = execution_context.set(execution_ctx)
    try:
        continuation_result = await agent.tool_action(synthetic_tool_call, task.payload)
    finally:
        execution_context.reset(token)

    if continuation_result.pending_result:
        raise RuntimeError(
            "Hook continuation produced another pending result, "
            "which is not supported in this phase."
        )

    return continuation_result


async def process_hook_runtime_wake_requests(
    *,
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    task: Task[Any],
    execution_ctx: ExecutionContext,
) -> HookRuntimeTickOutcome:
    """Run worker-side hook DAG progression for any queued wake requests."""
    task_keys = RedisKeys.format(
        namespace=namespace,
        task_id=task.id,
        agent=agent.name,
        owner_id=task.metadata.owner_id,
    )

    wake_requests = cast(
        dict[str | bytes, str | bytes],
        await redis_client.hgetall(task_keys.hook_runtime_ready),  # type: ignore
    )
    if not wake_requests:
        return HookRuntimeTickOutcome(
            had_wake_requests=False,
            should_repark=False,
            pending_tool_call_ids=[],
            completed_results=[],
        )

    completed_results: list[tuple[str, Any]] = []
    should_repark = False

    for raw_tool_call_id, raw_session_id in sorted(
        wake_requests.items(), key=lambda item: decode(item[0])
    ):
        tool_call_id = decode(raw_tool_call_id)
        session_id = decode(raw_session_id)
        pending_value_raw = await redis_client.hget(  # type: ignore[misc]
            task_keys.pending_tool_results, tool_call_id
        )
        has_pending_sentinel = (
            pending_value_raw is not None
            and decode(pending_value_raw) == PENDING_SENTINEL
        )
        session = await _load_hook_session(
            redis_client,
            keys=task_keys,
            session_id=session_id,
        )
        if session is None:
            if has_pending_sentinel:
                completed_results.append(
                    (
                        tool_call_id,
                        RuntimeError(
                            f"Hook session '{session_id}' was missing for "
                            f"pending tool call '{tool_call_id}'."
                        ),
                    )
                )
                await _clear_pending_tool_call_runtime_state(
                    redis_client=redis_client,
                    keys=task_keys,
                    tool_call_id=tool_call_id,
                )
            else:
                await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)  # type: ignore[misc]
            continue

        if session.tool_call_id != tool_call_id:
            if has_pending_sentinel:
                completed_results.append(
                    (
                        tool_call_id,
                        RuntimeError(
                            f"Hook session/tool call mismatch for session "
                            f"'{session_id}': expected '{tool_call_id}', got "
                            f"'{session.tool_call_id}'."
                        ),
                    )
                )
                await _finalize_hook_session_state(
                    redis_client=redis_client,
                    keys=task_keys,
                    session=session,
                    status="failed",
                )
            else:
                await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)  # type: ignore[misc]
            continue

        if session.status != "active":
            if has_pending_sentinel:
                completed_results.append(
                    (
                        tool_call_id,
                        RuntimeError(
                            f"Hook session '{session.session_id}' is in terminal "
                            f"state '{session.status}' for pending tool call "
                            f"'{tool_call_id}'."
                        ),
                    )
                )
                await _clear_pending_tool_call_runtime_state(
                    redis_client=redis_client,
                    keys=task_keys,
                    tool_call_id=tool_call_id,
                )
            else:
                await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)  # type: ignore[misc]
            continue

        terminal_nodes = [
            node.param_name
            for node in session.nodes.values()
            if node.status in {"expired", "failed"}
        ]
        if terminal_nodes:
            if has_pending_sentinel:
                has_expired_nodes = any(
                    node.status == "expired" for node in session.nodes.values()
                )
                completed_results.append(
                    (
                        tool_call_id,
                        RuntimeError(
                            f"Hook session '{session.session_id}' cannot continue: "
                            f"terminal dependency nodes={terminal_nodes}."
                        ),
                    )
                )
                await _finalize_hook_session_state(
                    redis_client=redis_client,
                    keys=task_keys,
                    session=session,
                    status="expired" if has_expired_nodes else "failed",
                )
            else:
                await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)  # type: ignore[misc]
            continue

        tool_def = next(
            (tool for tool in agent.tools if tool.name == session.tool_name),
            None,
        )
        hook_plan = tool_def.hook_plan if tool_def else None
        if hook_plan is None:
            if has_pending_sentinel:
                completed_results.append(
                    (
                        tool_call_id,
                        RuntimeError(
                            f"Hook continuation tool '{session.tool_name}' was "
                            "not found or has no hook plan."
                        ),
                    )
                )
                await _finalize_hook_session_state(
                    redis_client=redis_client,
                    keys=task_keys,
                    session=session,
                    status="failed",
                )
            else:
                await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)  # type: ignore[misc]
            continue

        requested_unresolved = [
            node for node in session.nodes.values() if node.status == "requested"
        ]
        if requested_unresolved:
            # Another callback is still pending; drop this wake marker.
            await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)  # type: ignore[misc]
            continue

        ready_unrequested: list[str] = []
        for param_name in hook_plan.hook_order:
            current = session.nodes[param_name]
            if current.status != "unrequested":
                continue
            if all(
                session.nodes[parent].status == "resolved"
                for parent in current.depends_on
            ):
                ready_unrequested.append(param_name)

        if ready_unrequested:
            resolved_payloads = {
                name: current.payload
                for name, current in session.nodes.items()
                if current.status == "resolved"
            }
            request_ctx = HookRequestContext(
                task_id=session.task_id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                tool_name=session.tool_name,
                tool_call_id=session.tool_call_id,
                args=session.tool_args,
            )
            now = time.time()
            for param_name in ready_unrequested:
                node_spec = hook_plan.nodes[param_name]
                request_kwargs = build_request_builder_kwargs(
                    request_builder=node_spec.request_builder,
                    request_ctx=request_ctx,
                    tool_args=session.tool_args,
                    resolved_hook_payloads=resolved_payloads,
                )
                next_pending = (
                    await node_spec.request_builder(**request_kwargs)
                    if asyncio.iscoroutinefunction(node_spec.request_builder)
                    else node_spec.request_builder(**request_kwargs)
                )
                if not isinstance(next_pending, PendingHook):
                    raise TypeError(
                        f"Hook request builder for '{param_name}' must return "
                        "PendingHook[...]"
                    )

                node_state = session.nodes[param_name]
                stable_hook_id = (
                    node_state.hook_id or f"{session.session_id}:{param_name}"
                )
                next_pending.hook_id = stable_hook_id

                await register_pending_hook(
                    redis_client=redis_client,
                    namespace=namespace,
                    task_id=session.task_id,
                    tool_call_id=session.tool_call_id,
                    pending_hook=next_pending,
                    mode=node_spec.mode,
                    session_id=session.session_id,
                    tool_name=session.tool_name,
                    tool_args=session.tool_args,
                    hook_param_name=param_name,
                    depends_on=node_spec.depends_on,
                    hook_type_name=node_spec.hook_type.__name__,
                )
                node_state.status = "requested"
                node_state.requested_at = now
                node_state.hook_id = stable_hook_id

            session.updated_at = now
            await _save_hook_session(
                redis_client=redis_client,
                keys=task_keys,
                session=session,
            )
            await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)  # type: ignore[misc]
            should_repark = True
            continue

        if all(node.status == "resolved" for node in session.nodes.values()):
            final_result = await _execute_hook_tool_continuation(
                agent=agent,
                task=task,
                execution_ctx=execution_ctx,
                session=session,
            )
            completed_results.append((tool_call_id, final_result))
            await _finalize_hook_session_state(
                redis_client=redis_client,
                keys=task_keys,
                session=session,
                status="completed",
            )
            continue

        await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)  # type: ignore[misc]

    pending_tool_call_ids = await _pending_sentinel_tool_call_ids(
        redis_client,
        task_keys=task_keys,
    )
    return HookRuntimeTickOutcome(
        had_wake_requests=True,
        should_repark=should_repark or bool(pending_tool_call_ids),
        pending_tool_call_ids=pending_tool_call_ids,
        completed_results=completed_results,
    )


async def resolve_hook(
    redis_client: redis.Redis,
    namespace: str,
    hook_id: str,
    payload: Any,
    token: str,
    *,
    idempotency_key: str | None = None,
) -> HookResolutionResult:
    """Resolve a pending hook and wake the parent task when ready."""
    keys = RedisKeys.format(namespace=namespace)
    record_json = await redis_client.hget(keys.hooks_index, hook_id)  # type: ignore[misc]
    if not record_json:
        raise HookNotFoundError(hook_id)

    hook_record = HookRecord.from_json(record_json)
    task_id = hook_record.task_id
    tool_call_id = hook_record.tool_call_id

    now = time.time()
    if hook_record.expires_at <= now:
        hook_record.status = "expired"
        await redis_client.hset(  # type: ignore[misc]
            keys.hooks_index, hook_id, hook_record.to_json()
        )
        await redis_client.zrem(keys.hooks_expiring, hook_id)
        raise HookExpiredError(hook_id)

    provided_token_hash = _hash_hook_token(token)
    token_hashes = hook_record.token_hashes
    if not any(
        isinstance(stored_hash, str)
        and secrets.compare_digest(stored_hash, provided_token_hash)
        for stored_hash in token_hashes
    ):
        raise HookTokenValidationError(hook_id)

    hook_resolution_key: str | None = None
    hook_request_hash: str | None = None
    hook_resolve_script = None
    hook_resolution_claimed = False
    hook_resolution_finalized = False
    if idempotency_key:
        hook_resolution_key = keys.hook_resolution(hook_id, idempotency_key)
        hook_request_hash = _hook_resolution_request_hash(payload=payload)
        hook_resolve_script = await create_hook_resolve_script(redis_client)
        hook_resolution_ttl_s = max(1, int(hook_record.expires_at - time.time()))
        claim_result = await hook_resolve_script.claim(
            hook_resolution_key=hook_resolution_key,
            request_hash=hook_request_hash,
            ttl_seconds=hook_resolution_ttl_s,
            hook_id=hook_id,
            task_id=task_id,
            tool_call_id=tool_call_id,
        )
        if claim_result.decision == "conflict":
            raise ValueError(
                "resolve_hook idempotency_key conflict: key was reused "
                "with a different request payload."
            )
        if claim_result.decision == "replay":
            return HookResolutionResult(
                hook_id=hook_id,
                task_id=task_id,
                tool_call_id=tool_call_id,
                status="idempotent",
                task_resumed=bool(claim_result.task_resumed),
            )
        if claim_result.decision == "pending":
            replay_lookup_delays_s = (0.02, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6)
            for idx, delay_s in enumerate(replay_lookup_delays_s):
                await asyncio.sleep(delay_s)
                hook_resolution_ttl_s = max(
                    1, int(hook_record.expires_at - time.time())
                )
                claim_result = await hook_resolve_script.claim(
                    hook_resolution_key=hook_resolution_key,
                    request_hash=hook_request_hash,
                    ttl_seconds=hook_resolution_ttl_s,
                    hook_id=hook_id,
                    task_id=task_id,
                    tool_call_id=tool_call_id,
                )
                if claim_result.decision == "conflict":
                    raise ValueError(
                        "resolve_hook idempotency_key conflict: key was reused "
                        "with a different request payload."
                    )
                if claim_result.decision == "replay":
                    return HookResolutionResult(
                        hook_id=hook_id,
                        task_id=task_id,
                        tool_call_id=tool_call_id,
                        status="idempotent",
                        task_resumed=bool(claim_result.task_resumed),
                    )
                if claim_result.decision == "claimed":
                    hook_resolution_claimed = True
                    break
                if idx == len(replay_lookup_delays_s) - 1:
                    raise RuntimeError(
                        "resolve_hook idempotent replay is still being "
                        "materialized. Retry shortly."
                    )
        elif claim_result.decision == "claimed":
            hook_resolution_claimed = True

    try:
        already_resolved = hook_record.status == "resolved"
        if hook_record.status == "expired":
            raise HookExpiredError(hook_id)
        if hook_record.status == "failed":
            raise HookAlreadyResolvedError(hook_id)

        if not already_resolved:
            hook_record.status = "resolved"
            hook_record.resolved_at = now
            hook_record.payload = payload

            await redis_client.hset(  # type: ignore[misc]
                keys.hooks_index, hook_id, hook_record.to_json()
            )
            await redis_client.zrem(keys.hooks_expiring, hook_id)
        elif hook_record.payload is None:
            # Recovery path: persist payload if the first attempt marked resolved
            # but crashed before storing the payload/session updates.
            hook_record.payload = payload
            hook_record.resolved_at = hook_record.resolved_at or now
            await redis_client.hset(  # type: ignore[misc]
                keys.hooks_index, hook_id, hook_record.to_json()
            )

        resolved_payload = hook_record.payload
        task_keys = RedisKeys.format(
            namespace=namespace,
            task_id=task_id,
            agent=hook_record.agent_name,
            owner_id=hook_record.owner_id,
        )
        task_resumed = False
        if not hook_record.session_id:
            raise ValueError(
                "resolve_hook requires a hook session. "
                "Legacy non-session hook completion paths are no longer supported."
            )
        if not hook_record.hook_param_name:
            raise ValueError(
                f"Hook '{hook_id}' is missing hook_param_name linkage metadata."
            )

        session = await _load_hook_session(
            redis_client,
            keys=task_keys,
            session_id=hook_record.session_id,
        )
        if session is None:
            raise ValueError(
                f"Hook session '{hook_record.session_id}' was not found for "
                f"hook '{hook_id}'."
            )
        if session.status == "expired":
            raise HookExpiredError(hook_id)
        if session.status in {"completed", "failed"}:
            raise HookAlreadyResolvedError(hook_id)

        if hook_record.hook_param_name not in session.nodes:
            raise ValueError(
                f"Hook parameter '{hook_record.hook_param_name}' was not found in "
                f"session '{session.session_id}'."
            )

        node = session.nodes[hook_record.hook_param_name]
        if node.status in {"expired", "failed"}:
            raise HookExpiredError(hook_id)
        if node.status != "resolved":
            node.status = "resolved"
            node.payload = resolved_payload
            node.resolved_at = hook_record.resolved_at or now
            node.hook_id = hook_record.hook_id
            session.updated_at = now
            await _save_hook_session(
                redis_client=redis_client,
                keys=task_keys,
                session=session,
            )

        if not any(current.status == "requested" for current in session.nodes.values()):
            wake_script = await create_hook_wake_script(redis_client)
            task_resumed, _ = await wake_script.execute(
                queue_main_key=task_keys.queue_main,
                queue_pending_key=task_keys.queue_pending,
                queue_orphaned_key=task_keys.queue_orphaned,
                task_statuses_key=task_keys.task_status,
                task_agents_key=task_keys.task_agent,
                task_payloads_key=task_keys.task_payload,
                task_pickups_key=task_keys.task_pickups,
                task_retries_key=task_keys.task_retries,
                task_metas_key=task_keys.task_meta,
                hook_runtime_ready_key=task_keys.hook_runtime_ready,
                task_id=task_id,
                tool_call_id=tool_call_id,
                session_id=session.session_id,
            )

        if (
            hook_resolution_key is not None
            and hook_request_hash is not None
            and hook_resolve_script is not None
        ):
            hook_resolution_ttl_s = max(1, int(hook_record.expires_at - time.time()))
            finalize_result = await hook_resolve_script.finalize(
                hook_resolution_key=hook_resolution_key,
                request_hash=hook_request_hash,
                ttl_seconds=hook_resolution_ttl_s,
                hook_id=hook_id,
                task_id=task_id,
                tool_call_id=tool_call_id,
                task_resumed=task_resumed,
            )
            if finalize_result.decision == "conflict":
                raise ValueError(
                    "resolve_hook idempotency_key conflict: key was reused "
                    "with a different request payload."
                )
            hook_resolution_finalized = True

        return HookResolutionResult(
            hook_id=hook_id,
            task_id=task_id,
            tool_call_id=tool_call_id,
            status="resolved",
            task_resumed=task_resumed,
        )
    except Exception:
        if (
            hook_resolution_key is not None
            and hook_resolution_claimed
            and not hook_resolution_finalized
        ):
            try:
                await redis_client.delete(hook_resolution_key)
            except Exception as cleanup_exc:
                logger.warning(
                    "resolve_hook failed to clear idempotency claim for hook %s",
                    hook_id,
                    exc_info=cleanup_exc,
                )
        raise


async def rotate_hook_token(
    redis_client: redis.Redis,
    namespace: str,
    hook_id: str,
    *,
    revoke_previous: bool = True,
) -> str:
    """Rotate token for a pending hook and return the new token."""
    keys = RedisKeys.format(namespace=namespace)
    record_json = await redis_client.hget(keys.hooks_index, hook_id)  # type: ignore[misc]
    if not record_json:
        raise HookNotFoundError(hook_id)

    hook_record = HookRecord.from_json(record_json)
    if hook_record.status == "resolved":
        raise HookAlreadyResolvedError(hook_id)

    now = time.time()
    if hook_record.expires_at <= now:
        hook_record.status = "expired"
        await redis_client.hset(  # type: ignore[misc]
            keys.hooks_index, hook_id, hook_record.to_json()
        )
        await redis_client.zrem(keys.hooks_expiring, hook_id)
        raise HookExpiredError(hook_id)

    next_token = secrets.token_urlsafe(32)
    next_hash = _hash_hook_token(next_token)

    if revoke_previous:
        hook_record.token_hashes = [next_hash]
    else:
        existing_hashes = [
            h for h in hook_record.token_hashes if isinstance(h, str)
        ]
        hook_record.token_hashes = list(dict.fromkeys([*existing_hashes, next_hash]))

    hook_record.token_version = int(hook_record.token_version) + 1
    await redis_client.hset(  # type: ignore[misc]
        keys.hooks_index, hook_id, hook_record.to_json()
    )

    return next_token


async def expire_pending_hooks(
    *,
    redis_client: redis.Redis,
    namespace: str,
    max_cleanup_batch: int,
) -> int:
    """Expire hook records past TTL and wake affected pending tool calls."""
    if max_cleanup_batch <= 0:
        raise ValueError("max_cleanup_batch must be greater than 0")

    now = time.time()
    keys = RedisKeys.format(namespace=namespace)
    expired_hook_ids_raw = cast(
        list[str | bytes],
        await redis_client.zrangebyscore(
            keys.hooks_expiring,
            "-inf",
            now,
            start=0,
            num=max_cleanup_batch,
        ),  # type: ignore[arg-type]
    )
    if not expired_hook_ids_raw:
        return 0

    wake_script = await create_hook_wake_script(redis_client)
    expired_count = 0

    for raw_hook_id in expired_hook_ids_raw:
        hook_id = decode(raw_hook_id)
        record_json = await redis_client.hget(keys.hooks_index, hook_id)  # type: ignore[misc]
        if not record_json:
            await redis_client.zrem(keys.hooks_expiring, hook_id)
            continue

        hook_record = HookRecord.from_json(record_json)
        if hook_record.status in {"resolved", "expired", "failed"}:
            await redis_client.zrem(keys.hooks_expiring, hook_id)
            continue
        if hook_record.expires_at > now:
            continue

        hook_record.status = "expired"
        await redis_client.hset(keys.hooks_index, hook_id, hook_record.to_json())  # type: ignore[misc]
        await redis_client.zrem(keys.hooks_expiring, hook_id)
        expired_count += 1

        if not hook_record.session_id:
            continue

        task_keys = RedisKeys.format(
            namespace=namespace,
            task_id=hook_record.task_id,
            agent=hook_record.agent_name,
            owner_id=hook_record.owner_id,
        )
        session = await _load_hook_session(
            redis_client,
            keys=task_keys,
            session_id=hook_record.session_id,
        )
        if session is not None:
            if hook_record.hook_param_name in session.nodes:
                node = session.nodes[hook_record.hook_param_name]
                if node.status not in {"resolved", "expired", "failed"}:
                    node.status = "expired"
                    node.resolved_at = now
            if session.status == "active":
                session.status = "expired"
            session.updated_at = now
            await _save_hook_session(
                redis_client=redis_client,
                keys=task_keys,
                session=session,
            )
            session_id_for_wake = session.session_id
        else:
            session_id_for_wake = hook_record.session_id

        try:
            await wake_script.execute(
                queue_main_key=task_keys.queue_main,
                queue_pending_key=task_keys.queue_pending,
                queue_orphaned_key=task_keys.queue_orphaned,
                task_statuses_key=task_keys.task_status,
                task_agents_key=task_keys.task_agent,
                task_payloads_key=task_keys.task_payload,
                task_pickups_key=task_keys.task_pickups,
                task_retries_key=task_keys.task_retries,
                task_metas_key=task_keys.task_meta,
                hook_runtime_ready_key=task_keys.hook_runtime_ready,
                task_id=hook_record.task_id,
                tool_call_id=hook_record.tool_call_id,
                session_id=session_id_for_wake,
            )
        except Exception as exc:
            logger.error(
                "Failed to wake task %s after hook expiry %s",
                hook_record.task_id,
                hook_id,
                exc_info=exc,
            )

    return expired_count


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


async def cancel_batch(
    redis_client: redis.Redis,
    namespace: str,
    batch_id: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    metrics_retention_duration: int,
) -> Batch:
    """Cancel all tasks in a batch.

    * Concurrently cancel all tasks in a batch.
    * Silently ignores cases where the task is already finished / cancelled.
    """

    batch = await get_batch_data(redis_client, namespace, batch_id)
    keys = RedisKeys.format(namespace=namespace)

    async def _safe_cancel(tid: str) -> None:
        try:
            await cancel_task(
                redis_client=redis_client,
                namespace=namespace,
                task_id=tid,
                agents_by_name=agents_by_name,
                metrics_retention_duration=metrics_retention_duration,
            )
        except (InactiveTaskError, TaskNotFoundError):
            # Task already in terminal state – ignore
            return
        except Exception as e:
            logger.error(f"Failed to cancel task {tid} in batch {batch_id}: {e}")

    # Run cancellations in parallel (bounded to avoid overwhelming redis)
    sem = asyncio.Semaphore(50)

    async def _bounded_cancel(tid: str) -> None:
        async with sem:
            await _safe_cancel(tid)

    await asyncio.gather(*[_bounded_cancel(tid) for tid in batch.task_ids])
    # Refresh batch stats (some tasks may have been cancelled immediately)
    batch = await get_batch_data(redis_client, namespace, batch_id)

    batch.metadata.status = "cancelled"

    pipe = redis_client.pipeline(transaction=True)
    pipe.hset(keys.batch_meta, batch_id, batch.metadata.to_json())  # type: ignore[arg-type]
    pipe.zadd(keys.batch_completed, {batch_id: time.time()})  # type: ignore[arg-type]
    await pipe.execute()

    owner_id = batch.metadata.owner_id
    if owner_id:
        publisher = EventPublisher(
            redis_client,
            RedisKeys.format(namespace=namespace, owner_id=owner_id).updates_channel,
        )
        await publisher.publish_event(
            BatchEvent(
                event_type="batch_cancelled",
                batch_id=batch_id,
                owner_id=owner_id,
                status="cancelled",
                progress=batch.progress,
                completed_tasks=len(batch.task_ids) - len(batch.remaining_task_ids),
                total_tasks=len(batch.task_ids),
            )
        )

    return batch


async def cancel_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    metrics_retention_duration: int,
) -> None:
    agent_name = await get_task_agent(redis_client, namespace, task_id)
    keys = RedisKeys.format(
        namespace=namespace,
        agent=agent_name,
        task_id=task_id,
    )

    cancel_script = await create_cancel_task_script(redis_client)
    result: CancelTaskScriptResult = await cancel_script.execute(
        queue_cancelled_key=keys.queue_cancelled,
        queue_backoff_key=keys.queue_backoff,
        queue_orphaned_key=keys.queue_orphaned,
        queue_pending_key=keys.queue_pending,
        pending_cancellations_key=keys.task_cancellations,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        pending_tool_results_key=keys.pending_tool_results,
        pending_child_task_results_key=keys.pending_child_task_results,
        agent_metrics_bucket_key=keys.agent_metrics_bucket,
        global_metrics_bucket_key=keys.global_metrics_bucket,
        task_id=task_id,
        metrics_ttl=metrics_retention_duration,
        queue_scheduled_key=keys.queue_scheduled,
        scheduled_wait_meta_key=keys.scheduled_wait_meta,
        pending_child_wait_ids_key=keys.pending_child_wait_ids,
    )

    if not result.success:
        if not result.current_status:
            raise TaskNotFoundError(task_id)
        elif result.current_status in [
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        ]:
            raise InactiveTaskError(task_id)
        else:
            raise Exception(result.message)

    # If the task was cancelled immediately by the script (e.g. it was in backoff or
    # pending_tool_results), the worker loop will never see it.  The Lua script returns
    # owner_id in this case so we can emit the run_cancelled event right here.
    if result.owner_id is not None:
        await run_agent_cancellation(
            redis_client=redis_client,
            namespace=namespace,
            agent=agents_by_name[agent_name],
            task_id=task_id,
        )


async def steer_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    messages: list[dict[str, Any]],
) -> None:
    """Steer a task"""
    task_status = await get_task_status(redis_client, namespace, task_id)
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        raise InactiveTaskError(task_id)

    keys = RedisKeys.format(namespace=namespace, task_id=task_id)
    # message id format: {timestamp_ms}_{random_hex} e.g. 1717234200000_a3f4b5c6
    message_mapping = {
        f"{int(time.time() * 1000)}_{secrets.token_hex(3)}": json.dumps(message)
        for message in messages
    }
    await redis_client.hset(keys.task_steering, mapping=message_mapping)  # type: ignore


async def resume_if_no_remaining_child_tasks(
    redis_client: redis.Redis,
    namespace: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    task_id: str,
) -> bool:
    """Resume the parent task if there are no remaining child tasks.

    Returns:
        * True if the task was resumed, False if cancelled or has child tasks
    """
    task_data = await get_task_data(redis_client, namespace, task_id)
    task_status = TaskStatus(task_data["status"])
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        return False

    agent_name = task_data["agent"]
    agent = agents_by_name[agent_name]
    keys = RedisKeys.format(namespace=namespace, task_id=task_id, agent=agent_name)

    try:
        task: Task = Task.from_dict(task_data, context_class=agent.context_class)  # type: ignore
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid", exc_info=e
        )
        return False

    wait_child_ids_raw = cast(
        set[str | bytes],
        await redis_client.smembers(keys.pending_child_wait_ids),  # type: ignore
    )
    if not wait_child_ids_raw:
        return False
    wait_child_ids = sorted(decode(child_id) for child_id in wait_child_ids_raw)

    # Read only the child results for the currently awaited wait-set.
    result_values = cast(
        list[str | bytes | None],
        await redis_client.hmget(keys.pending_child_task_results, wait_child_ids),  # type: ignore[arg-type,misc]
    )

    completed_results: list[tuple[str, Any]] = []
    for child_task_id, result_json in zip(wait_child_ids, result_values, strict=True):
        if result_json is None:
            return False
        result_str = decode(result_json)
        if result_str == PENDING_SENTINEL:
            return False
        completed_results.append((child_task_id, json.loads(result_str)))

    # Update the task context with the completed results
    updated_context = agent.process_child_task_results(
        task.payload, completed_results
    ).context

    # Move task back to queue
    child_task_completion_script = await create_child_task_completion_script(
        redis_client
    )
    success, message = await child_task_completion_script.execute(
        queue_main_key=keys.queue_main,
        queue_orphaned_key=keys.queue_orphaned,
        queue_pending_key=keys.queue_pending,
        pending_child_task_results_key=keys.pending_child_task_results,
        pending_child_wait_ids_key=keys.pending_child_wait_ids,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        task_id=task.id,
        updated_task_context_json=updated_context.to_json(),
    )

    return success


async def run_agent_cancellation(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    task_id: str,
) -> None:
    """Run task cancellation"""
    task_data = await get_task_data(redis_client, namespace, task_id)
    if not task_data:
        logger.error(
            f"Failed to complete task cancellation for {task_id}: Task data not found"
        )
        return

    try:
        task: Task[Any] = Task.from_dict(task_data, context_class=agent.context_class)
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid", exc_info=e
        )
        return

    keys = RedisKeys.format(namespace=namespace, owner_id=task.metadata.owner_id)
    event_publisher = EventPublisher(
        redis_client=redis_client,
        channel=keys.updates_channel,
    )

    try:
        execution_ctx = ExecutionContext(
            task_id=task.id,
            owner_id=task.metadata.owner_id,
            retries=task.retries,
            iterations=task.payload.turn,
            events=event_publisher,
        )
        # Lifecycle callback – run cancelled
        await agent._safe_call(
            agent.on_run_cancelled,
            task.payload,
            execution_ctx,
        )
        logger.info(f"🚫 Task cancelled {colored(f'[{task.id}]', 'dim')}")

        await event_publisher.publish_event(
            AgentEvent(
                event_type="run_cancelled",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
            )
        )
    except Exception as e:
        logger.error(f"Error sending cancellation event for {task.id}", exc_info=e)


async def process_cancelled_tasks(
    redis_client: redis.Redis,
    namespace: str,
    cancelled_task_ids: list[str],
    agent: BaseAgent[Any],
) -> None:
    """Process cancelled tasks with agent-specific logic and publish events."""
    tasks = [
        run_agent_cancellation(redis_client, namespace, agent, task_id)
        for task_id in cancelled_task_ids
    ]
    await asyncio.gather(*tasks)


async def get_task_batch(
    batch_script: BatchPickupScript,
    namespace: str,
    agent: BaseAgent[Any],
    batch_size: int,
    metrics_ttl: int,
) -> tuple[list[str], list[str]]:
    """
    Get batch of tasks atomically

    Returns:
        * List of task ids to process
        * List of task ids to cancel
    """
    keys = RedisKeys.format(
        namespace=namespace,
        agent=agent.name,
    )

    try:
        result: BatchPickupScriptResult = await batch_script.execute(
            queue_main_key=keys.queue_main,
            queue_cancelled_key=keys.queue_cancelled,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_cancellations_key=keys.task_cancellations,
            processing_heartbeats_key=keys.processing_heartbeats,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_size=batch_size,
            metrics_ttl=metrics_ttl,
        )
        if result.orphaned_task_ids:
            logger.warning(
                f"⚠️ Found {len(result.orphaned_task_ids)} orphaned tasks: "
                f"{result.orphaned_task_ids}"
            )
        if result.corrupted_task_ids:
            logger.warning(
                f"⚠️ Found {len(result.corrupted_task_ids)} corrupted tasks: "
                f"{result.corrupted_task_ids}"
            )

        return result.tasks_to_process_ids, result.tasks_to_cancel_ids

    except Exception as e:
        logger.error(f"Failed to get task batch: {e}")
        await asyncio.sleep(0.15 + random.random() * 0.1)  # backoff with jitter
        return [], []
