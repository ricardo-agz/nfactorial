"""Integration coverage for hook registration, resolution, and runtime flow."""

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Literal

import httpx
import pytest
import redis.asyncio as redis
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)

from factorial import Agent, AgentContext, hook, tool
from factorial.context import ExecutionContext, execution_context
from factorial.exceptions import HookExpiredError, HookTokenValidationError
from factorial.hooks import (
    Hook,
    HookRequestContext,
    HookSessionNode,
    HookSessionRecord,
    PendingHook,
)
from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.operations import (
    enqueue_task,
    expire_pending_hooks,
    persist_hook_runtime_payload,
    process_hook_runtime_wake_requests,
    register_pending_hook,
    resolve_hook,
    rotate_hook_token,
)
from factorial.queue.task import Task, TaskStatus, get_task_data, get_task_status
from factorial.queue.worker import CompletionAction, process_task
from factorial.tools import _ToolResultInternal
from factorial.utils import decode

from .conftest import SimpleTestAgent


class ApprovalHook(Hook):
    approved: bool


class _NoopEvents:
    async def publish_event(self, event):
        return None


def _make_pending_hook(
    *,
    task_id: str = "task",
    owner_id: str = "owner",
    agent_name: str = "agent",
    tool_call_id: str = "tool_call",
    hook_id: str | None = None,
    token: str | None = None,
    hook_type: type[Hook] = ApprovalHook,
    metadata: dict[str, str | bool | int] | None = None,
) -> PendingHook[Any]:
    ctx = HookRequestContext(
        task_id=task_id,
        owner_id=owner_id,
        agent_name=agent_name,
        tool_name="request_approval",
        tool_call_id=tool_call_id,
    )
    return hook_type.pending(
        ctx=ctx,
        hook_id=hook_id,
        token=token,
        title="Approve action",
        timeout_s=300,
        metadata=metadata or {"source": "integration-test"},
    )


@pytest.mark.asyncio
class TestHookResolutionFlow:
    async def _setup_processing_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        agent_name: str,
        task,
        pickup_script,
    ) -> str:
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=agent_name,
            task_id=task.id,
        )
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=SimpleTestAgent(name=agent_name),
            task=task,
        )
        await pickup_script.execute(
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
            batch_size=1,
            metrics_ttl=3600,
        )
        return task_id

    async def _setup_pending_tool_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script,
        completion_script,
        tool_call_id: str,
    ) -> str:
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        await pickup_script.execute(
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
            batch_size=1,
            metrics_ttl=3600,
        )

        await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.PENDING_TOOL.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_tool_call_ids_json=json.dumps([tool_call_id]),
        )

        return task_id

    async def _persist_requested_hook_session(
        self,
        *,
        redis_client: redis.Redis,
        namespace: str,
        task_id: str,
        owner_id: str,
        agent_name: str,
        tool_call_id: str,
        requested_nodes: list[
            tuple[
                str,
                PendingHook[Any],
                tuple[str, ...],
                Literal["requires", "awaits"],
                str,
            ]
        ],
        tool_name: str = "hook_test_tool",
        tool_args: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> str:
        now = datetime.now(timezone.utc).timestamp()
        runtime_session_id = session_id or f"session:{task_id}:{tool_call_id}"
        nodes: dict[str, HookSessionNode] = {}
        requested_hooks: list[dict[str, Any]] = []

        for param_name, pending, depends_on, mode, hook_type_name in requested_nodes:
            nodes[param_name] = HookSessionNode(
                param_name=param_name,
                mode=mode,
                hook_type=hook_type_name,
                depends_on=depends_on,
                status="requested",
                hook_id=pending.hook_id,
                requested_at=now,
            )
            requested_hooks.append(
                {
                    "param_name": param_name,
                    "mode": mode,
                    "hook_type": hook_type_name,
                    "depends_on": list(depends_on),
                    "hook_id": pending.hook_id,
                    "submit_url": pending.submit_url,
                    "token": pending.token,
                    "expires_at": pending.expires_at.timestamp(),
                    "title": pending.title,
                    "metadata": pending.metadata,
                }
            )

        session = HookSessionRecord(
            session_id=runtime_session_id,
            task_id=task_id,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            tool_args=tool_args or {},
            nodes=nodes,
            status="active",
            created_at=now,
            updated_at=now,
        )
        await persist_hook_runtime_payload(
            redis_client=redis_client,
            namespace=namespace,
            task_id=task_id,
            runtime_payload={
                "kind": "hook_session_init",
                "session": session.to_dict(),
                "requested_hooks": requested_hooks,
            },
        )
        return runtime_session_id

    async def test_register_pending_hook_persists_token_hash(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_approval_1"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )

        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )

        created = await register_pending_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            tool_call_id=tool_call_id,
            pending_hook=pending,
            session_id=f"session:{task_id}:{tool_call_id}",
            tool_name="hook_test_tool",
            hook_param_name="approval",
        )
        assert created is True

        hooks_index_key = f"{test_namespace}:hooks:index"
        record_json = await redis_client.hget(hooks_index_key, pending.hook_id)
        assert record_json is not None
        record = json.loads(record_json)
        assert record["task_id"] == task_id
        assert record["tool_call_id"] == tool_call_id
        assert record["status"] == "requested"
        assert record["token_hashes"] and pending.token not in record["token_hashes"]

        created_again = await register_pending_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            tool_call_id=tool_call_id,
            pending_hook=pending,
            session_id=f"session:{task_id}:{tool_call_id}",
            tool_name="hook_test_tool",
            hook_param_name="approval",
        )
        assert created_again is False

    async def test_resolve_hook_with_valid_token_requeues_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        agents_by_name,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_approval_2"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )

        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval", pending, (), "requires", pending.hook_type.__name__)
            ],
        )

        result = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            payload={"approved": True},
            token=pending.token,
        )

        assert result.status == "resolved"
        assert result.task_resumed is True

        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.ACTIVE

        queue_length = await redis_client.llen(keys.queue_main)
        assert queue_length == 1
        assert await redis_client.zscore(keys.queue_pending, task_id) is None
        assert await redis_client.hget(keys.pending_tool_results, tool_call_id) == (
            PENDING_SENTINEL
        )
        assert (
            await redis_client.hget(keys.hook_runtime_ready, tool_call_id)
        ) is not None

        record_json = await redis_client.hget(keys.hooks_index, pending.hook_id)
        assert record_json is not None
        record = json.loads(record_json)
        assert record["status"] == "resolved"
        assert record["payload"] == {"approved": True}
        assert await redis_client.zscore(keys.hooks_expiring, pending.hook_id) is None

    async def test_resolve_hook_rejects_invalid_token(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        agents_by_name,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_approval_3"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )
        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval", pending, (), "requires", pending.hook_type.__name__)
            ],
        )

        with pytest.raises(HookTokenValidationError):
            await resolve_hook(
                redis_client=redis_client,
                namespace=test_namespace,
                hook_id=pending.hook_id,
                payload={"approved": True},
                token="wrong-token",
            )

        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )
        pending_value = await redis_client.hget(keys.pending_tool_results, tool_call_id)
        assert pending_value == PENDING_SENTINEL

    async def test_resolve_hook_honors_idempotency_key(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        agents_by_name,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_approval_4"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )
        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval", pending, (), "requires", pending.hook_type.__name__)
            ],
        )

        first = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            payload={"approved": True},
            token=pending.token,
            idempotency_key="evt-approval-1",
        )
        second = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            payload={"approved": True},
            token=pending.token,
            idempotency_key="evt-approval-1",
        )

        assert first.status == "resolved"
        assert second.status == "idempotent"
        assert second.task_resumed is True

        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)
        assert await redis_client.llen(keys.queue_main) == 1

    async def test_resolve_hook_recovers_from_partial_resolve_write(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_approval_crash_recovery"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )
        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        session_id = await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval", pending, (), "requires", pending.hook_type.__name__)
            ],
        )

        # Simulate a crash window where hook_record was persisted as resolved
        # but session node/wake updates did not happen yet.
        record_json = await redis_client.hget(keys.hooks_index, pending.hook_id)
        assert record_json is not None
        record = json.loads(record_json)
        record["status"] = "resolved"
        record["payload"] = {"approved": True}
        record["resolved_at"] = datetime.now(timezone.utc).timestamp()
        await redis_client.hset(keys.hooks_index, pending.hook_id, json.dumps(record))

        session_json = await redis_client.hget(keys.hook_sessions, session_id)
        assert session_json is not None
        session_before = HookSessionRecord.from_json(session_json)
        assert session_before.nodes["approval"].status == "requested"

        recovered = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            payload={"approved": True},
            token=pending.token,
        )
        assert recovered.status == "resolved"
        assert recovered.task_resumed is True
        assert await get_task_status(redis_client, test_namespace, task_id) == (
            TaskStatus.ACTIVE
        )

        session_json_after = await redis_client.hget(keys.hook_sessions, session_id)
        assert session_json_after is not None
        session_after = HookSessionRecord.from_json(session_json_after)
        assert session_after.nodes["approval"].status == "resolved"
        assert await redis_client.llen(keys.queue_main) == 1

    async def test_rotate_hook_token_replaces_previous_token(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        agents_by_name,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_approval_5"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )
        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval", pending, (), "requires", pending.hook_type.__name__)
            ],
        )

        rotated_token = await rotate_hook_token(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            revoke_previous=True,
        )

        with pytest.raises(HookTokenValidationError):
            await resolve_hook(
                redis_client=redis_client,
                namespace=test_namespace,
                hook_id=pending.hook_id,
                payload={"approved": True},
                token=pending.token,
            )

        resolved = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            payload={"approved": True},
            token=rotated_token,
        )
        assert resolved.status == "resolved"

    async def test_rotate_without_revocation_accepts_existing_token(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_rotate_keep_old"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )
        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval", pending, (), "requires", pending.hook_type.__name__)
            ],
        )

        await rotate_hook_token(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            revoke_previous=False,
        )
        resolved = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            payload={"approved": True},
            token=pending.token,
        )
        assert resolved.status == "resolved"

    async def test_rotate_without_revocation_accepts_new_token(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_rotate_use_new"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )
        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval", pending, (), "requires", pending.hook_type.__name__)
            ],
        )

        rotated = await rotate_hook_token(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            revoke_previous=False,
        )
        resolved = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending.hook_id,
            payload={"approved": True},
            token=rotated,
        )
        assert resolved.status == "resolved"

    async def test_register_pending_hook_rejects_non_pending_tool_state(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
    ) -> None:
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )
        tool_call_id = "tool_call_approval_6"
        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )

        with pytest.raises(
            ValueError, match="requires task status in .*pending_tool_results"
        ):
            await register_pending_hook(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=task_id,
                tool_call_id=tool_call_id,
                pending_hook=pending,
                session_id=f"session:{task_id}:{tool_call_id}",
                tool_name="hook_test_tool",
                hook_param_name="approval",
            )

    async def test_resolve_expired_hook_does_not_requeue_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        agents_by_name,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_approval_7"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )
        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        pending.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval", pending, (), "requires", pending.hook_type.__name__)
            ],
        )

        with pytest.raises(HookExpiredError):
            await resolve_hook(
                redis_client=redis_client,
                namespace=test_namespace,
                hook_id=pending.hook_id,
                payload={"approved": True},
                token=pending.token,
            )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PENDING_TOOL_RESULTS
        assert await redis_client.zscore(keys.queue_pending, task_id) is not None
        assert await redis_client.hget(keys.pending_tool_results, tool_call_id) == (
            PENDING_SENTINEL
        )

        record_json = await redis_client.hget(keys.hooks_index, pending.hook_id)
        assert record_json is not None
        record = json.loads(record_json)
        assert record["status"] == "expired"

    async def test_resolve_waits_for_all_hooks_on_same_tool_call(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        agents_by_name,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_approval_8"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )
        pending_a = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        pending_b = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval_a", pending_a, (), "requires", pending_a.hook_type.__name__),
                ("approval_b", pending_b, (), "requires", pending_b.hook_type.__name__),
            ],
        )

        first = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending_a.hook_id,
            payload={"approved": True, "step": "a"},
            token=pending_a.token,
        )
        assert first.task_resumed is False
        assert await get_task_status(redis_client, test_namespace, task_id) == (
            TaskStatus.PENDING_TOOL_RESULTS
        )
        assert await redis_client.zscore(keys.queue_pending, task_id) is not None
        assert await redis_client.hget(keys.pending_tool_results, tool_call_id) == (
            PENDING_SENTINEL
        )

        second = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=pending_b.hook_id,
            payload={"approved": True, "step": "b"},
            token=pending_b.token,
        )
        assert second.task_resumed is True
        assert await get_task_status(redis_client, test_namespace, task_id) == (
            TaskStatus.ACTIVE
        )
        assert await redis_client.llen(keys.queue_main) == 1

    async def test_dag_stage_progression_and_final_continuation(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        pickup_script,
        completion_script,
        agents_by_name,
    ) -> None:
        class ManagerApproval(Hook):
            approved: bool

        class FinanceApproval(Hook):
            approved: bool

        def request_manager(
            ctx: HookRequestContext, amount: int
        ) -> PendingHook[ManagerApproval]:
            return _make_pending_hook(
                hook_id="hook-manager-stage-1",
                token="manager-token",
                hook_type=ManagerApproval,
                metadata={"amount": amount, "task_id": ctx.task_id},
            )

        def request_finance(
            ctx: HookRequestContext,
            amount: int,
            manager: ManagerApproval,
        ) -> PendingHook[FinanceApproval]:
            return _make_pending_hook(
                hook_id="hook-finance-stage-2",
                token="finance-token",
                hook_type=FinanceApproval,
                metadata={
                    "amount": amount,
                    "manager_approved": manager.approved,
                    "task_id": ctx.task_id,
                },
            )

        @tool(name="wire_transfer")
        def wire_transfer(
            amount: int,
            manager: Annotated[ManagerApproval, hook.requires(request_manager)],
            finance: Annotated[FinanceApproval, hook.requires(request_finance)],
        ) -> str:
            return (
                f"transfer-approved:{amount}:"
                f"{manager.approved}:{finance.approved}"
            )

        hook_agent = Agent(
            name="hook_runtime_agent",
            instructions="test",
            tools=[wire_transfer],
            context_class=AgentContext,
            http_client=httpx.AsyncClient(verify=False, trust_env=False),
        )

        task = Task.create(
            owner_id=test_owner_id,
            agent=hook_agent.name,
            payload=AgentContext(query="approve transfer"),
        )
        task_id = await self._setup_processing_task(
            redis_client=redis_client,
            test_namespace=test_namespace,
            agent_name=hook_agent.name,
            task=task,
            pickup_script=pickup_script,
        )

        tool_call_id = "tool_call_dag_1"
        synthetic_tool_call = ChatCompletionMessageFunctionToolCall(
            id=tool_call_id,
            type="function",
            function=ToolCallFunction(
                name="wire_transfer",
                arguments=json.dumps({"amount": 42}),
            ),
        )
        exec_ctx = ExecutionContext(
            task_id=task_id,
            owner_id=test_owner_id,
            retries=0,
            iterations=0,
            events=_NoopEvents(),  # type: ignore[arg-type]
            persist_hook_runtime=lambda payload: persist_hook_runtime_payload(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=task_id,
                runtime_payload=payload,
            ),
        )
        token = execution_context.set(exec_ctx)
        try:
            initial_result = await hook_agent.tool_action(
                synthetic_tool_call,
                task.payload,
            )
        finally:
            execution_context.reset(token)

        assert initial_result.pending_result is True

        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=hook_agent.name,
            task_id=task_id,
        )
        await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.PENDING_TOOL.value,
            updated_task_payload_json=task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_tool_call_ids_json=json.dumps([tool_call_id]),
        )

        hook_ids = await redis_client.smembers(keys.hooks_by_task)
        assert len(hook_ids) == 1  # manager only for stage 1
        manager_hook_id = decode(next(iter(hook_ids)))
        manager_record = json.loads(
            decode(await redis_client.hget(keys.hooks_index, manager_hook_id))
        )
        assert manager_record["hook_param_name"] == "manager"

        first = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=manager_hook_id,
            payload={"approved": True},
            token="manager-token",
        )
        assert first.task_resumed is True
        assert await get_task_status(redis_client, test_namespace, task_id) == (
            TaskStatus.ACTIVE
        )
        assert await redis_client.llen(keys.queue_main) == 1
        assert (
            await redis_client.hget(keys.hook_runtime_ready, tool_call_id)
        ) is not None

        # Simulate worker pickup + hook runtime tick (stage 2 request path).
        await pickup_script.execute(
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
            batch_size=1,
            metrics_ttl=3600,
        )
        task_data = await get_task_data(redis_client, test_namespace, task_id)
        picked_task = Task.from_dict(task_data, context_class=hook_agent.context_class)
        exec_ctx_tick = ExecutionContext(
            task_id=task_id,
            owner_id=test_owner_id,
            retries=picked_task.retries,
            iterations=picked_task.payload.turn,
            events=_NoopEvents(),  # type: ignore[arg-type]
            persist_hook_runtime=lambda payload: persist_hook_runtime_payload(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=task_id,
                runtime_payload=payload,
            ),
        )
        tick_1 = await process_hook_runtime_wake_requests(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=hook_agent,
            task=picked_task,
            execution_ctx=exec_ctx_tick,
        )
        assert tick_1.had_wake_requests is True
        assert tick_1.should_repark is True
        assert tick_1.pending_tool_call_ids == [tool_call_id]
        assert tick_1.completed_results == []

        await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.PENDING_TOOL.value,
            updated_task_payload_json=picked_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=picked_task.payload.turn,
            pending_tool_call_ids_json=json.dumps(tick_1.pending_tool_call_ids),
        )
        assert await get_task_status(redis_client, test_namespace, task_id) == (
            TaskStatus.PENDING_TOOL_RESULTS
        )

        # locate finance hook record created by worker runtime stage progression
        hook_ids = await redis_client.smembers(keys.hooks_by_task)
        finance_hook_id = None
        finance_record = None
        for raw_hook_id in hook_ids:
            hook_id = decode(raw_hook_id)
            record_json = await redis_client.hget(keys.hooks_index, hook_id)
            if not record_json:
                continue
            record = json.loads(decode(record_json))
            if record.get("hook_param_name") == "finance":
                finance_hook_id = hook_id
                finance_record = record
                break
        assert finance_hook_id is not None
        assert finance_record is not None
        assert finance_record["status"] == "requested"

        second = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=finance_hook_id,
            payload={"approved": True},
            token="finance-token",
        )
        assert second.task_resumed is True
        assert await get_task_status(redis_client, test_namespace, task_id) == (
            TaskStatus.ACTIVE
        )
        assert await redis_client.llen(keys.queue_main) == 1

        # Simulate worker pickup + hook runtime tick (final continuation path).
        await pickup_script.execute(
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
            batch_size=1,
            metrics_ttl=3600,
        )
        task_data = await get_task_data(redis_client, test_namespace, task_id)
        picked_task = Task.from_dict(task_data, context_class=hook_agent.context_class)
        exec_ctx_tick_2 = ExecutionContext(
            task_id=task_id,
            owner_id=test_owner_id,
            retries=picked_task.retries,
            iterations=picked_task.payload.turn,
            events=_NoopEvents(),  # type: ignore[arg-type]
            persist_hook_runtime=lambda payload: persist_hook_runtime_payload(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=task_id,
                runtime_payload=payload,
            ),
        )
        tick_2 = await process_hook_runtime_wake_requests(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=hook_agent,
            task=picked_task,
            execution_ctx=exec_ctx_tick_2,
        )
        assert tick_2.had_wake_requests is True
        assert tick_2.should_repark is False
        assert tick_2.pending_tool_call_ids == []
        assert len(tick_2.completed_results) == 1
        assert tick_2.completed_results[0][0] == tool_call_id
        assert isinstance(tick_2.completed_results[0][1], _ToolResultInternal)
        assert (
            tick_2.completed_results[0][1].client_output
            == "transfer-approved:42:True:True"
        )

        await hook_agent.http_client.aclose()

    async def test_process_hook_runtime_missing_plan_unblocks_pending_tool(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script,
    ) -> None:
        task_id = await self._setup_processing_task(
            redis_client=redis_client,
            test_namespace=test_namespace,
            agent_name=test_agent.name,
            task=sample_task,
            pickup_script=pickup_script,
        )
        tool_call_id = "tool_call_missing_plan"
        session_id = f"session:{task_id}:{tool_call_id}"
        now = datetime.now(timezone.utc).timestamp()
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )
        session = HookSessionRecord(
            session_id=session_id,
            task_id=task_id,
            tool_call_id=tool_call_id,
            tool_name="nonexistent_tool",
            tool_args={},
            nodes={
                "approval": HookSessionNode(
                    param_name="approval",
                    mode="requires",
                    hook_type="ApprovalHook",
                    depends_on=(),
                    status="resolved",
                    hook_id="hook_missing_plan",
                    payload={"approved": True},
                    resolved_at=now,
                )
            },
            status="active",
            created_at=now,
            updated_at=now,
        )

        await redis_client.hset(keys.hook_sessions, session_id, session.to_json())
        await redis_client.hset(
            keys.hook_session_by_tool_call, tool_call_id, session_id
        )
        await redis_client.hset(
            keys.pending_tool_results, tool_call_id, PENDING_SENTINEL
        )
        await redis_client.hset(keys.hook_runtime_ready, tool_call_id, session_id)

        task_data = await get_task_data(redis_client, test_namespace, task_id)
        task = Task.from_dict(task_data, context_class=test_agent.context_class)
        exec_ctx = ExecutionContext(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            retries=task.retries,
            iterations=task.payload.turn,
            events=_NoopEvents(),  # type: ignore[arg-type]
        )
        tick = await process_hook_runtime_wake_requests(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=task,
            execution_ctx=exec_ctx,
        )
        assert tick.had_wake_requests is True
        assert tick.should_repark is False
        assert tick.pending_tool_call_ids == []
        assert len(tick.completed_results) == 1
        assert tick.completed_results[0][0] == tool_call_id
        assert isinstance(tick.completed_results[0][1], Exception)
        assert "no hook plan" in str(tick.completed_results[0][1]).lower()
        assert await redis_client.hget(keys.pending_tool_results, tool_call_id) is None
        assert (
            await redis_client.hget(keys.hook_session_by_tool_call, tool_call_id)
            is None
        )

        session_after_json = await redis_client.hget(keys.hook_sessions, session_id)
        assert session_after_json is not None
        session_after = HookSessionRecord.from_json(session_after_json)
        assert session_after.status == "failed"

    async def test_expire_pending_hooks_wakes_and_unblocks_expired_session(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script,
        completion_script,
    ) -> None:
        tool_call_id = "tool_call_expire_sweep"
        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            tool_call_id,
        )
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )
        pending = _make_pending_hook(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
        )
        pending.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        session_id = await self._persist_requested_hook_session(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            agent_name=test_agent.name,
            tool_call_id=tool_call_id,
            requested_nodes=[
                ("approval", pending, (), "requires", pending.hook_type.__name__)
            ],
        )

        expired_count = await expire_pending_hooks(
            redis_client=redis_client,
            namespace=test_namespace,
            max_cleanup_batch=10,
        )
        assert expired_count == 1
        assert await get_task_status(redis_client, test_namespace, task_id) == (
            TaskStatus.ACTIVE
        )
        assert (
            await redis_client.hget(keys.hook_runtime_ready, tool_call_id)
        ) is not None

        await pickup_script.execute(
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
            batch_size=1,
            metrics_ttl=3600,
        )
        task_data = await get_task_data(redis_client, test_namespace, task_id)
        task = Task.from_dict(task_data, context_class=test_agent.context_class)
        exec_ctx = ExecutionContext(
            task_id=task_id,
            owner_id=sample_task.metadata.owner_id,
            retries=task.retries,
            iterations=task.payload.turn,
            events=_NoopEvents(),  # type: ignore[arg-type]
        )
        tick = await process_hook_runtime_wake_requests(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=task,
            execution_ctx=exec_ctx,
        )
        assert tick.had_wake_requests is True
        assert tick.should_repark is False
        assert tick.pending_tool_call_ids == []
        assert len(tick.completed_results) == 1
        assert isinstance(tick.completed_results[0][1], Exception)
        assert "terminal state 'expired'" in str(tick.completed_results[0][1])
        assert await redis_client.hget(keys.pending_tool_results, tool_call_id) is None

        session_json_after = await redis_client.hget(keys.hook_sessions, session_id)
        assert session_json_after is not None
        session_after = HookSessionRecord.from_json(session_json_after)
        assert session_after.status == "expired"
        assert session_after.nodes["approval"].status == "expired"

    async def test_process_task_parks_child_results_from_hook_continuation(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        pickup_script,
        completion_script,
        steering_script,
    ) -> None:
        class Approval(Hook):
            approved: bool

        def request_approval(ctx: HookRequestContext) -> PendingHook[Approval]:
            return _make_pending_hook(
                hook_id="hook-child-continuation-stage-1",
                token="child-continuation-token",
                hook_type=Approval,
                metadata={"task_id": ctx.task_id},
            )

        child_ids = [str(uuid.uuid4()), str(uuid.uuid4())]

        @tool(name="spawn_after_approval")
        def spawn_after_approval(
            approval: Annotated[Approval, hook.requires(request_approval)],
        ) -> _ToolResultInternal:
            return _ToolResultInternal(
                model_output="spawned child tasks",
                client_output={
                    "child_count": len(child_ids),
                    "approved": approval.approved,
                },
                pending_child_task_ids=child_ids,
            )

        hook_agent = Agent(
            name="hook_child_continuation_agent",
            instructions="test",
            tools=[spawn_after_approval],
            context_class=AgentContext,
            http_client=httpx.AsyncClient(verify=False, trust_env=False),
        )
        task = Task.create(
            owner_id=test_owner_id,
            agent=hook_agent.name,
            payload=AgentContext(query="spawn children after approval"),
        )
        task_id = await self._setup_processing_task(
            redis_client=redis_client,
            test_namespace=test_namespace,
            agent_name=hook_agent.name,
            task=task,
            pickup_script=pickup_script,
        )
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=hook_agent.name,
            task_id=task_id,
        )
        tool_call_id = "tool_call_child_continuation"
        synthetic_tool_call = ChatCompletionMessageFunctionToolCall(
            id=tool_call_id,
            type="function",
            function=ToolCallFunction(
                name="spawn_after_approval",
                arguments=json.dumps({}),
            ),
        )
        exec_ctx = ExecutionContext(
            task_id=task_id,
            owner_id=test_owner_id,
            retries=0,
            iterations=0,
            events=_NoopEvents(),  # type: ignore[arg-type]
            persist_hook_runtime=lambda payload: persist_hook_runtime_payload(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=task_id,
                runtime_payload=payload,
            ),
        )
        token = execution_context.set(exec_ctx)
        try:
            initial_result = await hook_agent.tool_action(
                synthetic_tool_call,
                task.payload,
            )
        finally:
            execution_context.reset(token)
        assert initial_result.pending_result is True

        await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.PENDING_TOOL.value,
            updated_task_payload_json=task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_tool_call_ids_json=json.dumps([tool_call_id]),
        )

        hook_ids = await redis_client.smembers(keys.hooks_by_task)
        assert len(hook_ids) == 1
        approval_hook_id = decode(next(iter(hook_ids)))
        resolved = await resolve_hook(
            redis_client=redis_client,
            namespace=test_namespace,
            hook_id=approval_hook_id,
            payload={"approved": True},
            token="child-continuation-token",
        )
        assert resolved.task_resumed is True

        await pickup_script.execute(
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
            batch_size=1,
            metrics_ttl=3600,
        )

        await process_task(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            completion_script=completion_script,
            steering_script=steering_script,
            agent=hook_agent,
            agents_by_name={hook_agent.name: hook_agent},
            max_retries=3,
            heartbeat_interval=1,
            task_timeout=30,
            metrics_retention_duration=3600,
        )
        assert await get_task_status(redis_client, test_namespace, task_id) == (
            TaskStatus.PENDING_CHILD_TASKS
        )
        pending_children = await redis_client.hgetall(keys.pending_child_task_results)
        assert set(pending_children.keys()) == set(child_ids)
        assert all(value == PENDING_SENTINEL for value in pending_children.values())
        assert await redis_client.hget(keys.pending_tool_results, tool_call_id) is None

        await hook_agent.http_client.aclose()
