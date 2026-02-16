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
from factorial.context import execution_context
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
    create_hook_wake_script,
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
from factorial.tools import ToolResult
from factorial.utils import decode, is_valid_task_id

logger = get_logger(__name__)


def _hash_hook_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


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
    session_json = await redis_client.hget(keys.hook_sessions, session_id)
    if not session_json:
        return None
    return HookSessionRecord.from_json(session_json)


async def _save_hook_session(
    redis_client: redis.Redis,
    *,
    keys: RedisKeys,
    session: HookSessionRecord,
) -> None:
    await redis_client.hset(keys.hook_sessions, session.session_id, session.to_json())


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

    existing_session_id = await redis_client.hget(
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

    existing_json = await redis_client.hget(task_keys.hooks_index, pending_hook.hook_id)
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
) -> Any:
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

    if isinstance(continuation_result, ToolResult):
        return continuation_result.output_data
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
        session = await _load_hook_session(
            redis_client,
            keys=task_keys,
            session_id=session_id,
        )
        if (
            session is None
            or session.status != "active"
            or session.tool_call_id != tool_call_id
        ):
            await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)
            continue

        tool_def = next(
            (tool for tool in agent.tools if tool.name == session.tool_name),
            None,
        )
        hook_plan = tool_def.hook_plan if tool_def else None
        if hook_plan is None:
            await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)
            continue

        requested_unresolved = [
            node for node in session.nodes.values() if node.status == "requested"
        ]
        if requested_unresolved:
            # Another callback is still pending; drop this wake marker.
            await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)
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
            await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)
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

            session.status = "completed"
            session.updated_at = time.time()
            await _save_hook_session(
                redis_client=redis_client,
                keys=task_keys,
                session=session,
            )

            pipe = redis_client.pipeline(transaction=True)
            pipe.hdel(task_keys.pending_tool_results, tool_call_id)
            pipe.hdel(task_keys.hook_runtime_ready, tool_call_id)
            pipe.hdel(task_keys.hook_session_by_tool_call, tool_call_id)
            await pipe.execute()
            continue

        await redis_client.hdel(task_keys.hook_runtime_ready, tool_call_id)

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
    agents_by_name: dict[str, BaseAgent[Any]],
    hook_id: str,
    payload: Any,
    token: str,
    *,
    idempotency_key: str | None = None,
) -> HookResolutionResult:
    """Resolve a pending hook and wake the parent task when ready."""
    keys = RedisKeys.format(namespace=namespace)
    record_json = await redis_client.hget(keys.hooks_index, hook_id)
    if not record_json:
        raise HookNotFoundError(hook_id)

    hook_record = HookRecord.from_json(record_json)
    task_id = hook_record.task_id
    tool_call_id = hook_record.tool_call_id

    now = time.time()
    if hook_record.expires_at <= now:
        hook_record.status = "expired"
        await redis_client.hset(
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

    if idempotency_key:
        idem_key = keys.hook_idempotency(hook_id, idempotency_key)
        existing_idem = await redis_client.get(idem_key)  # type: ignore[assignment]
        if existing_idem:
            idem_data = json.loads(decode(existing_idem))
            return HookResolutionResult(
                hook_id=hook_id,
                task_id=task_id,
                tool_call_id=tool_call_id,
                status="idempotent",
                task_resumed=bool(idem_data.get("task_resumed", False)),
            )

    already_resolved = hook_record.status == "resolved"
    if already_resolved and not idempotency_key:
        raise HookAlreadyResolvedError(hook_id)
    if hook_record.status == "expired":
        raise HookExpiredError(hook_id)

    if not already_resolved:
        hook_record.status = "resolved"
        hook_record.resolved_at = now
        hook_record.payload = payload

        await redis_client.hset(
            keys.hooks_index, hook_id, hook_record.to_json()
        )
        await redis_client.zrem(keys.hooks_expiring, hook_id)

    resolved_payload = hook_record.payload
    task_keys = RedisKeys.format(
        namespace=namespace,
        task_id=task_id,
        agent=hook_record.agent_name,
        owner_id=hook_record.owner_id,
    )
    task_resumed = False
    session_handled = False
    if hook_record.session_id:
        session = await _load_hook_session(
            redis_client,
            keys=task_keys,
            session_id=hook_record.session_id,
        )
        if session and hook_record.hook_param_name in session.nodes:
            session_handled = True
            node = session.nodes[hook_record.hook_param_name]
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

            if not any(
                current.status == "requested" for current in session.nodes.values()
            ):
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

    if not session_handled:
        raise ValueError(
            "resolve_hook requires a hook session. "
            "Legacy non-session hook completion paths are no longer supported."
        )

    if idempotency_key:
        idem_key = keys.hook_idempotency(hook_id, idempotency_key)
        ttl_s = max(1, int(hook_record.expires_at - now))
        idem_value = {
            "hook_id": hook_id,
            "task_id": task_id,
            "tool_call_id": tool_call_id,
            "task_resumed": task_resumed,
        }
        await redis_client.set(  # type: ignore[arg-type]
            idem_key, json.dumps(idem_value, default=str), ex=ttl_s
        )

    return HookResolutionResult(
        hook_id=hook_id,
        task_id=task_id,
        tool_call_id=tool_call_id,
        status="resolved",
        task_resumed=task_resumed,
    )


async def rotate_hook_token(
    redis_client: redis.Redis,
    namespace: str,
    hook_id: str,
    *,
    revoke_previous: bool = True,
) -> str:
    """Rotate token for a pending hook and return the new token."""
    keys = RedisKeys.format(namespace=namespace)
    record_json = await redis_client.hget(keys.hooks_index, hook_id)
    if not record_json:
        raise HookNotFoundError(hook_id)

    hook_record = HookRecord.from_json(record_json)
    if hook_record.status == "resolved":
        raise HookAlreadyResolvedError(hook_id)

    now = time.time()
    if hook_record.expires_at <= now:
        hook_record.status = "expired"
        await redis_client.hset(
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
    await redis_client.hset(
        keys.hooks_index, hook_id, hook_record.to_json()
    )

    return next_token


async def enqueue_task(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    task: Task[ContextType],
) -> str:
    keys = RedisKeys.format(namespace=namespace, agent=agent.name)

    if not is_valid_task_id(task.id):
        raise InvalidTaskIdError(task.id)

    enqueue_script = await create_enqueue_task_script(redis_client)
    await enqueue_script.execute(
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
    )

    return task.id


async def create_batch_and_enqueue(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    payloads: list[ContextType],
    owner_id: str,
    parent_id: str | None = None,
) -> Batch:
    """Atomically enqueue a batch of tasks via batch_enqueue.lua.

    Returns the *batch_id* created.
    """
    batch_id = str(uuid.uuid4())
    created_at = time.time()

    task_objs: list[Task[ContextType]] = []
    for payload in payloads:
        t: Task[ContextType] = Task.create(
            owner_id=owner_id, agent=agent.name, payload=payload, batch_id=batch_id
        )
        if parent_id is not None:
            t.metadata.parent_id = parent_id
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
    )
    task_ids = result.task_ids

    return Batch(
        id=batch_id,
        metadata=BatchMetadata.from_dict(batch_meta),
        task_ids=task_ids,
        remaining_task_ids=task_ids,
        progress=0.0,
    )


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

    # Get all results and check if any are still pending
    all_results = cast(
        dict[str | bytes, str | bytes],
        await redis_client.hgetall(keys.pending_child_task_results),  # type: ignore
    )
    if not all_results:
        return False

    # Check if any results are still pending (sentinel value)
    completed_results: list[tuple[str, Any]] = []

    for child_task_id, result_json in all_results.items():
        child_task_id_str = decode(child_task_id)
        result_str = decode(result_json)

        if result_str == PENDING_SENTINEL:
            return False
        else:
            completed_results.append((child_task_id_str, json.loads(result_str)))

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
