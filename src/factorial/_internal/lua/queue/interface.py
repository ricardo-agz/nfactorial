import json
from dataclasses import dataclass, field
from typing import Any

import redis.asyncio as redis

from factorial._internal.queue.keys import PENDING_SENTINEL, RedisKeys

from ._runtime import (
    CancelTaskScript,
    CancelTaskScriptResult,
    TaskCompletionScript,
    TaskCompletionScriptResult,
    create_cancel_task_script,
    create_task_completion_script,
)


@dataclass(frozen=True, slots=True)
class TaskCompletionInput:
    """Semantic inputs for task completion transitions."""

    task_id: str
    action: str
    updated_task_payload_json: str
    current_turn: int
    parent_task_id: str | None = None
    pending_tool_call_ids: list[str] | None = None
    pending_child_task_ids: list[str] | None = None
    final_output: dict[str, Any] | str | None = None

    def __post_init__(self) -> None:
        if not self.task_id:
            raise ValueError("TaskCompletionInput.task_id must be a non-empty string")
        if not self.action:
            raise ValueError("TaskCompletionInput.action must be a non-empty string")


@dataclass(slots=True)
class QueueScripts:
    """Facade around Lua scripts with namespace/agent pre-bound."""

    redis_client: redis.Redis
    namespace: str
    agent_name: str
    metrics_ttl: int
    _cancel_script: CancelTaskScript | None = None
    _completion_script: TaskCompletionScript | None = None
    _keys: RedisKeys = field(init=False, repr=False)
    _queue_templates: RedisKeys = field(init=False, repr=False)
    _task_steering_key_template: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._keys = RedisKeys.format(namespace=self.namespace, agent=self.agent_name)
        self._queue_templates = RedisKeys.format(
            namespace=self.namespace,
            agent="{agent}",
        )
        self._task_steering_key_template = RedisKeys.format(
            namespace=self.namespace,
            task_id="{task_id}",
        ).task_steering

    @classmethod
    def for_agent(
        cls,
        *,
        redis_client: redis.Redis,
        namespace: str,
        agent_name: str,
        metrics_ttl: int,
        cancel_script: CancelTaskScript | None = None,
        completion_script: TaskCompletionScript | None = None,
    ) -> "QueueScripts":
        return cls(
            redis_client=redis_client,
            namespace=namespace,
            agent_name=agent_name,
            metrics_ttl=metrics_ttl,
            _cancel_script=cancel_script,
            _completion_script=completion_script,
        )

    async def _get_cancel_script(self) -> CancelTaskScript:
        if self._cancel_script is None:
            self._cancel_script = await create_cancel_task_script(self.redis_client)
        return self._cancel_script

    async def _get_completion_script(self) -> TaskCompletionScript:
        if self._completion_script is None:
            self._completion_script = await create_task_completion_script(
                self.redis_client
            )
        return self._completion_script

    def _task_keys(self, task_id: str) -> RedisKeys:
        return RedisKeys.format(
            namespace=self.namespace,
            agent=self.agent_name,
            task_id=task_id,
        )

    async def cancel_task(self, *, task_id: str) -> CancelTaskScriptResult:
        task_keys = self._task_keys(task_id)
        cancel_script = await self._get_cancel_script()
        return await cancel_script.execute(
            queue_cancelled_key=self._keys.queue_cancelled,
            queue_backoff_key=self._keys.queue_backoff,
            queue_orphaned_key=self._keys.queue_orphaned,
            queue_pending_key=self._keys.queue_pending,
            pending_cancellations_key=self._keys.task_cancellations,
            task_statuses_key=self._keys.task_status,
            task_agents_key=self._keys.task_agent,
            task_payloads_key=self._keys.task_payload,
            task_pickups_key=self._keys.task_pickups,
            task_retries_key=self._keys.task_retries,
            task_metas_key=self._keys.task_meta,
            pending_tool_results_key=task_keys.pending_tool_results,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            agent_metrics_bucket_key=self._keys.agent_metrics_bucket,
            global_metrics_bucket_key=self._keys.global_metrics_bucket,
            task_id=task_id,
            metrics_ttl=self.metrics_ttl,
            queue_scheduled_key=self._keys.queue_scheduled,
            scheduled_wait_meta_key=self._keys.scheduled_wait_meta,
            pending_child_wait_ids_key=task_keys.pending_child_wait_ids,
            activity_wait_meta_key=self._keys.activity_wait_meta,
            queue_main_key_template=self._queue_templates.queue_main,
            queue_pending_key_template=self._queue_templates.queue_pending,
            queue_scheduled_key_template=self._queue_templates.queue_scheduled,
            task_steering_key_template=self._task_steering_key_template,
            message_seq_key=self._keys.messaging_message_seq,
            signal_wait_meta_key=self._keys.signal_wait_meta,
            signal_wake_meta_key=self._keys.signal_wake_meta,
            task_signals_key=task_keys.task_signals,
        )

    async def complete_task(
        self,
        request: TaskCompletionInput,
    ) -> TaskCompletionScriptResult:
        completion_script = await self._get_completion_script()
        task_keys = self._task_keys(request.task_id)
        parent_keys = (
            RedisKeys.format(namespace=self.namespace, task_id=request.parent_task_id)
            if request.parent_task_id
            else None
        )
        return await completion_script.execute(
            queue_main_key=self._keys.queue_main,
            queue_completions_key=self._keys.queue_completions,
            queue_failed_key=self._keys.queue_failed,
            queue_backoff_key=self._keys.queue_backoff,
            queue_orphaned_key=self._keys.queue_orphaned,
            queue_pending_key=self._keys.queue_pending,
            task_statuses_key=self._keys.task_status,
            task_agents_key=self._keys.task_agent,
            task_payloads_key=self._keys.task_payload,
            task_pickups_key=self._keys.task_pickups,
            task_retries_key=self._keys.task_retries,
            task_metas_key=self._keys.task_meta,
            processing_heartbeats_key=self._keys.processing_heartbeats,
            pending_tool_results_key=task_keys.pending_tool_results,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            agent_metrics_bucket_key=self._keys.agent_metrics_bucket,
            global_metrics_bucket_key=self._keys.global_metrics_bucket,
            batch_meta_key=self._keys.batch_meta,
            batch_progress_key=self._keys.batch_progress,
            batch_remaining_tasks_key=self._keys.batch_remaining_tasks,
            batch_completed_key=self._keys.batch_completed,
            activity_wait_meta_key=self._keys.activity_wait_meta,
            task_steering_key_template=self._task_steering_key_template,
            message_seq_key=self._keys.messaging_message_seq,
            queue_main_key_template=self._queue_templates.queue_main,
            queue_pending_key_template=self._queue_templates.queue_pending,
            queue_scheduled_key_template=self._queue_templates.queue_scheduled,
            scheduled_wait_meta_key=self._keys.scheduled_wait_meta,
            current_turn=request.current_turn,
            pending_child_wait_ids_key=task_keys.pending_child_wait_ids,
            parent_pending_child_task_results_key=(
                parent_keys.pending_child_task_results if parent_keys else None
            ),
            parent_pending_child_wait_ids_key=(
                parent_keys.pending_child_wait_ids if parent_keys else None
            ),
            task_id=request.task_id,
            action=request.action,
            updated_task_payload_json=request.updated_task_payload_json,
            metrics_ttl=self.metrics_ttl,
            pending_sentinel=PENDING_SENTINEL,
            pending_tool_call_ids_json=(
                json.dumps(request.pending_tool_call_ids)
                if request.pending_tool_call_ids
                else None
            ),
            pending_child_task_ids_json=(
                json.dumps(request.pending_child_task_ids)
                if request.pending_child_task_ids
                else None
            ),
            final_output_json=json.dumps(request.final_output),
        )
