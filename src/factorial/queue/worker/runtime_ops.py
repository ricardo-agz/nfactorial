import asyncio
import json
from dataclasses import dataclass
from typing import Any, cast

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.core.events import AgentEvent, BatchEvent, EventPublisher
from factorial.core.utils import serialize_data
from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    ActivityWaitScript,
    QueueScripts,
    SignalWaitScript,
    TaskCompletionInput,
    WaitScheduleScript,
)
from factorial.queue.operations import (
    cancel_task,
    create_batch_and_enqueue,
    enqueue_task,
    messaging_groups_add_members,
    messaging_groups_create,
    messaging_groups_find,
    messaging_groups_get,
    messaging_groups_leave,
    messaging_groups_list,
    messaging_groups_remove_members,
    messaging_groups_send,
    messaging_inbox_direct_mark_read,
    messaging_inbox_direct_peek,
    messaging_inbox_group_mark_read,
    messaging_inbox_group_peek,
    messaging_inbox_receipts_mark_read,
    messaging_inbox_receipts_peek,
    messaging_send_direct,
    persist_hook_runtime_payload,
    resume_if_no_remaining_child_tasks,
    signal_task as queue_signal_task,
)
from factorial.queue.task import (
    Batch,
    Task,
    effective_team_id,
    get_batch_data,
    get_task_data,
)

from .common import CompletionAction, logger


@dataclass
class TaskRuntimeOps:
    """Per-task runtime operations."""

    redis_client: redis.Redis
    namespace: str
    agent: BaseAgent[Any]
    agents_by_name: dict[str, BaseAgent[Any]]
    task: Task[Any]
    keys: RedisKeys
    event_publisher: EventPublisher
    queue_scripts: QueueScripts
    wait_schedule_script: WaitScheduleScript
    activity_wait_script: ActivityWaitScript
    signal_wait_script: SignalWaitScript
    metrics_retention_duration: int

    async def complete(
        self,
        *,
        action: CompletionAction,
        pending_tool_call_ids: list[str] | None,
        pending_child_task_ids: list[str] | None,
        final_output: dict[str, Any] | str | None,
    ) -> None:
        try:
            result = await self.queue_scripts.complete_task(
                TaskCompletionInput(
                    task_id=self.task.id,
                    action=action.value,
                    updated_task_payload_json=self.task.payload.to_json(),
                    current_turn=self.task.payload.turn_number,
                    parent_task_id=self.task.metadata.parent_id,
                    pending_tool_call_ids=pending_tool_call_ids,
                    pending_child_task_ids=pending_child_task_ids,
                    final_output=final_output,
                )
            )

            if result.batch_completed:
                await self.event_publisher.publish_event(
                    BatchEvent(
                        event_type="batch_completed",
                        batch_id=self.task.metadata.batch_id,
                        owner_id=self.task.metadata.owner_id,
                    )
                )
            if not result.success:
                raise RuntimeError(
                    "Task completion script rejected transition "
                    f"action='{action.value}' task_id='{self.task.id}'"
                )
        except Exception as e:
            logger.error(f"Error completing task: {e}", exc_info=e)
            raise

    async def park_scheduled_wait(
        self,
        *,
        wait_kind: str,
        wake_timestamp: float,
        source_tool_call_ids: list[str],
        data: Any = None,
        cron_expression: str | None = None,
        cron_timezone: str | None = None,
    ) -> None:
        wait_metadata = {
            "kind": wait_kind,
            "wake_timestamp": wake_timestamp,
            "source_tool_call_ids": source_tool_call_ids,
        }
        if data is not None:
            wait_metadata["data"] = serialize_data(data)
        if cron_expression is not None:
            wait_metadata["cron"] = cron_expression
        if cron_timezone is not None:
            wait_metadata["timezone"] = cron_timezone

        schedule_result = await self.wait_schedule_script.execute(
            queue_scheduled_key=self.keys.queue_scheduled,
            queue_pending_key=self.keys.queue_pending,
            queue_orphaned_key=self.keys.queue_orphaned,
            processing_heartbeats_key=self.keys.processing_heartbeats,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            scheduled_wait_meta_key=self.keys.scheduled_wait_meta,
            task_id=self.task.id,
            updated_task_payload_json=self.task.payload.to_json(),
            wake_timestamp=wake_timestamp,
            wait_metadata_json=json.dumps(wait_metadata),
        )
        if not schedule_result.success:
            raise RuntimeError(
                "Failed to schedule wait for task "
                f"{self.task.id}: {schedule_result.message}"
            )

    async def park_activity_wait(
        self,
        *,
        source_tool_call_ids: list[str],
        data: Any = None,
        timeout_wake_timestamp: float | None = None,
        timeout_kind: str | None = None,
        timeout_cron_expression: str | None = None,
        timeout_cron_timezone: str | None = None,
    ) -> None:
        steering_key_template = RedisKeys.format(
            namespace=self.namespace,
            task_id="{task_id}",
        ).task_steering
        queue_templates = RedisKeys.format(
            namespace=self.namespace,
            agent="{agent}",
        )
        wait_metadata: dict[str, Any] = {
            "kind": "activity",
            "source_tool_call_ids": source_tool_call_ids,
        }
        if data is not None:
            wait_metadata["data"] = serialize_data(data)
        scheduled_wait_metadata_json: str | None = None
        if timeout_wake_timestamp is not None:
            timeout_metadata: dict[str, Any] = {
                "kind": timeout_kind,
                "wake_timestamp": timeout_wake_timestamp,
            }
            if timeout_cron_expression is not None:
                timeout_metadata["cron"] = timeout_cron_expression
            if timeout_cron_timezone is not None:
                timeout_metadata["timezone"] = timeout_cron_timezone
            wait_metadata["timeout"] = timeout_metadata

            scheduled_wait_metadata: dict[str, Any] = {
                "kind": "activity_timeout",
                "wake_timestamp": timeout_wake_timestamp,
                "source_tool_call_ids": source_tool_call_ids,
                "timeout_kind": timeout_kind,
            }
            if timeout_cron_expression is not None:
                scheduled_wait_metadata["cron"] = timeout_cron_expression
            if timeout_cron_timezone is not None:
                scheduled_wait_metadata["timezone"] = timeout_cron_timezone
            if data is not None:
                scheduled_wait_metadata["data"] = serialize_data(data)
            scheduled_wait_metadata_json = json.dumps(scheduled_wait_metadata)

        wait_result = await self.activity_wait_script.execute(
            queue_pending_key=self.keys.queue_pending,
            queue_orphaned_key=self.keys.queue_orphaned,
            processing_heartbeats_key=self.keys.processing_heartbeats,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            activity_wait_meta_key=self.keys.activity_wait_meta,
            message_seq_key=self.keys.messaging_message_seq,
            task_id=self.task.id,
            updated_task_payload_json=self.task.payload.to_json(),
            wait_metadata_json=json.dumps(wait_metadata),
            task_steering_key_template=steering_key_template,
            task_children_key_template=self.keys.task_children("{parent_task_id}"),
            queue_main_key_template=queue_templates.queue_main,
            queue_pending_key_template=queue_templates.queue_pending,
            queue_scheduled_key_template=queue_templates.queue_scheduled,
            scheduled_wait_meta_key=self.keys.scheduled_wait_meta,
            timeout_wake_timestamp=timeout_wake_timestamp,
            scheduled_wait_metadata_json=scheduled_wait_metadata_json,
        )
        if not wait_result.success:
            raise RuntimeError(
                f"Failed to park activity wait for task {self.task.id}: "
                f"{wait_result.message}"
            )

    async def park_signal_wait(
        self,
        *,
        signal_id: str,
        source_tool_call_ids: list[str],
        data: Any = None,
        timeout_wake_timestamp: float | None = None,
        timeout_kind: str | None = None,
        timeout_cron_expression: str | None = None,
        timeout_cron_timezone: str | None = None,
    ) -> bool:
        wait_metadata: dict[str, Any] = {
            "kind": "signal",
            "signal_id": signal_id,
            "source_tool_call_ids": source_tool_call_ids,
        }
        if data is not None:
            wait_metadata["data"] = serialize_data(data)
        scheduled_wait_metadata_json: str | None = None
        if timeout_wake_timestamp is not None:
            timeout_metadata: dict[str, Any] = {
                "kind": timeout_kind,
                "wake_timestamp": timeout_wake_timestamp,
            }
            if timeout_cron_expression is not None:
                timeout_metadata["cron"] = timeout_cron_expression
            if timeout_cron_timezone is not None:
                timeout_metadata["timezone"] = timeout_cron_timezone
            wait_metadata["timeout"] = timeout_metadata
            scheduled_wait_metadata: dict[str, Any] = {
                "kind": "signal_timeout",
                "signal_id": signal_id,
                "wake_timestamp": timeout_wake_timestamp,
                "source_tool_call_ids": source_tool_call_ids,
                "timeout_kind": timeout_kind,
            }
            if timeout_cron_expression is not None:
                scheduled_wait_metadata["cron"] = timeout_cron_expression
            if timeout_cron_timezone is not None:
                scheduled_wait_metadata["timezone"] = timeout_cron_timezone
            if data is not None:
                scheduled_wait_metadata["data"] = serialize_data(data)
            scheduled_wait_metadata_json = json.dumps(scheduled_wait_metadata)

        wait_result = await self.signal_wait_script.execute(
            queue_main_key=self.keys.queue_main,
            queue_pending_key=self.keys.queue_pending,
            queue_orphaned_key=self.keys.queue_orphaned,
            processing_heartbeats_key=self.keys.processing_heartbeats,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            signal_wait_meta_key=self.keys.signal_wait_meta,
            signal_wake_meta_key=self.keys.signal_wake_meta,
            task_signals_key=self.keys.task_signals,
            queue_scheduled_key=self.keys.queue_scheduled,
            scheduled_wait_meta_key=self.keys.scheduled_wait_meta,
            task_id=self.task.id,
            signal_id=signal_id,
            updated_task_payload_json=self.task.payload.to_json(),
            wait_metadata_json=json.dumps(wait_metadata),
            timeout_wake_timestamp=timeout_wake_timestamp,
            scheduled_wait_metadata_json=scheduled_wait_metadata_json,
        )
        if not wait_result.success:
            raise RuntimeError(
                f"Failed to park signal wait for task {self.task.id}: "
                f"{wait_result.message}"
            )
        return wait_result.woken_immediately

    async def pop_signal_wake_context(self) -> dict[str, Any] | None:
        raw_value = await self.redis_client.hget(
            self.keys.signal_wake_meta,
            self.task.id,
        )
        if raw_value is None:
            return None
        await self.redis_client.hdel(self.keys.signal_wake_meta, self.task.id)
        try:
            decoded_value = (
                raw_value.decode("utf-8")
                if isinstance(raw_value, bytes)
                else str(raw_value)
            )
            parsed = json.loads(decoded_value)
        except Exception:
            return None
        if not isinstance(parsed, dict):
            return None
        return cast(dict[str, Any], parsed)

    async def park_or_resume_child_wait(
        self,
        *,
        child_task_ids: list[str],
        event_data: dict[str, Any] | None,
    ) -> None:
        deduped_child_task_ids = list(dict.fromkeys(child_task_ids))
        if not deduped_child_task_ids:
            raise ValueError("Expected at least one child task ID for child wait.")

        raw_results = await self.redis_client.hmget(
            self.keys.pending_child_task_results,
            deduped_child_task_ids,
        )  # type: ignore[arg-type,misc]
        completed_results: list[tuple[str, Any]] = []
        all_ready = True
        for child_task_id, raw_result in zip(
            deduped_child_task_ids,
            raw_results,
            strict=True,
        ):
            if raw_result is None:
                all_ready = False
                break
            result_str = (
                raw_result.decode("utf-8")
                if isinstance(raw_result, bytes)
                else str(raw_result)
            )
            if result_str == PENDING_SENTINEL:
                all_ready = False
                break
            completed_results.append((child_task_id, json.loads(result_str)))

        if all_ready and completed_results:
            self.task.payload = self.agent.process_child_task_results(
                self.task.payload,
                completed_results,
            ).context
            await self.complete(
                action=CompletionAction.CONTINUE,
                pending_tool_call_ids=None,
                pending_child_task_ids=None,
                final_output=None,
            )
            try:
                await cast(
                    Any,
                    self.redis_client.hdel(
                        self.keys.pending_child_task_results,
                        *deduped_child_task_ids,
                    ),
                )
            except Exception as cleanup_exc:
                logger.warning(
                    "Failed to clean fast-path child results for task %s",
                    self.task.id,
                    exc_info=cleanup_exc,
                )
            return

        await self.complete(
            action=CompletionAction.PENDING_CHILD,
            pending_tool_call_ids=None,
            pending_child_task_ids=deduped_child_task_ids,
            final_output=None,
        )

        resumed = await resume_if_no_remaining_child_tasks(
            redis_client=self.redis_client,
            namespace=self.namespace,
            agents_by_name=self.agents_by_name,
            task_id=self.task.id,
        )
        if resumed:
            return

        await self.event_publisher.publish_event(
            AgentEvent(
                event_type="task_pending_child_task_results",
                task_id=self.task.id,
                owner_id=self.task.metadata.owner_id,
                agent_name=self.agent.name,
                turn=self.task.payload.turn_number,
                data=event_data,
            )
        )

    async def enqueue_child_task(
        self,
        child_agent: BaseAgent[Any],
        child_payload: Any,
        task_id: str | None = None,
    ) -> str:
        child_task: Task[Any] = Task.create(
            owner_id=self.task.metadata.owner_id,
            agent=child_agent.name,
            payload=child_payload,
        )
        if task_id is not None:
            child_task.id = task_id

        child_task.metadata.parent_id = self.task.id
        child_task.metadata.team_id = self.task.metadata.team_id or self.task.id

        await enqueue_task(
            redis_client=self.redis_client,
            namespace=self.namespace,
            agent=child_agent,
            task=child_task,
        )
        return child_task.id

    async def enqueue_batch(
        self,
        agent: BaseAgent[Any],
        payloads: list[Any],
        task_ids: list[str] | None = None,
        batch_id: str | None = None,
    ) -> Batch:
        return await create_batch_and_enqueue(
            redis_client=self.redis_client,
            namespace=self.namespace,
            agent=agent,
            payloads=payloads,
            owner_id=self.task.metadata.owner_id,
            parent_id=self.task.id,
            team_id=self.task.metadata.team_id or self.task.id,
            task_ids=task_ids,
            batch_id=batch_id,
        )

    async def cancel_child_task(self, child_task_id: str) -> None:
        await self._validate_direct_child_scope(
            child_task_id,
            operation="subagents.cancel",
        )
        await cancel_task(
            redis_client=self.redis_client,
            namespace=self.namespace,
            task_id=child_task_id,
            agents_by_name=self.agents_by_name,
            metrics_retention_duration=self.metrics_retention_duration,
        )

    async def cancel_child_tasks(self, child_task_ids: list[str]) -> None:
        if not child_task_ids:
            return
        deduped_child_task_ids = list(dict.fromkeys(child_task_ids))
        for child_task_id in deduped_child_task_ids:
            await self._validate_direct_child_scope(
                child_task_id,
                operation="subagents.cancel",
            )

        await asyncio.gather(
            *[
                cancel_task(
                    redis_client=self.redis_client,
                    namespace=self.namespace,
                    task_id=child_task_id,
                    agents_by_name=self.agents_by_name,
                    metrics_retention_duration=self.metrics_retention_duration,
                )
                for child_task_id in deduped_child_task_ids
            ]
        )

    async def signal_child_task(
        self,
        child_task_id: str,
        signal_id: str,
        payload: Any = None,
    ) -> dict[str, Any]:
        await self._validate_direct_child_scope(
            child_task_id,
            operation="subagents.signal",
        )
        try:
            return await queue_signal_task(
                self.redis_client,
                self.namespace,
                sender_task_id=self.task.id,
                task_id=child_task_id,
                signal_id=signal_id,
                payload=payload,
            )
        except Exception:
            return {
                "signal_id": signal_id,
                "signal_seq": None,
                "target_task_ids": [child_task_id],
                "signaled_task_ids": [],
                "woken_task_ids": [],
                "skipped_inactive_task_ids": [],
                "failed_task_ids": [child_task_id],
            }

    async def signal_child_tasks(
        self,
        child_task_ids: list[str],
        signal_id: str,
        payload: Any = None,
    ) -> dict[str, Any]:
        deduped_child_task_ids = list(dict.fromkeys(child_task_ids))
        if not deduped_child_task_ids:
            return {
                "signal_id": signal_id,
                "signal_seq": None,
                "target_task_ids": [],
                "signaled_task_ids": [],
                "woken_task_ids": [],
                "skipped_inactive_task_ids": [],
                "failed_task_ids": [],
            }
        validated_child_task_ids: list[str] = []
        failed_validation_task_ids: list[str] = []
        for child_task_id in deduped_child_task_ids:
            try:
                await self._validate_direct_child_scope(
                    child_task_id,
                    operation="subagents.signal",
                )
                validated_child_task_ids.append(child_task_id)
            except Exception:
                failed_validation_task_ids.append(child_task_id)

        if not validated_child_task_ids:
            return {
                "signal_id": signal_id,
                "signal_seq": None,
                "target_task_ids": list(deduped_child_task_ids),
                "signaled_task_ids": [],
                "woken_task_ids": [],
                "skipped_inactive_task_ids": [],
                "failed_task_ids": failed_validation_task_ids,
            }

        results = await asyncio.gather(
            *[
                queue_signal_task(
                    self.redis_client,
                    self.namespace,
                    sender_task_id=self.task.id,
                    task_id=child_task_id,
                    signal_id=signal_id,
                    payload=payload,
                )
                for child_task_id in validated_child_task_ids
            ],
            return_exceptions=True,
        )
        aggregate: dict[str, Any] = {
            "signal_id": signal_id,
            "signal_seq": None,
            "target_task_ids": list(deduped_child_task_ids),
            "signaled_task_ids": [],
            "woken_task_ids": [],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": list(dict.fromkeys(failed_validation_task_ids)),
        }
        signaled_set: set[str] = set()
        woken_set: set[str] = set()
        skipped_set: set[str] = set()
        failed_set: set[str] = set(aggregate["failed_task_ids"])
        for child_task_id, result in zip(
            validated_child_task_ids,
            results,
            strict=True,
        ):
            if isinstance(result, BaseException):
                if child_task_id not in failed_set:
                    failed_set.add(child_task_id)
                    aggregate["failed_task_ids"].append(child_task_id)
                continue
            if aggregate["signal_seq"] is None and result.get("signal_seq") is not None:
                aggregate["signal_seq"] = result.get("signal_seq")
            for value in result.get("signaled_task_ids", []):
                if isinstance(value, str) and value not in signaled_set:
                    signaled_set.add(value)
                    aggregate["signaled_task_ids"].append(value)
            for value in result.get("woken_task_ids", []):
                if isinstance(value, str) and value not in woken_set:
                    woken_set.add(value)
                    aggregate["woken_task_ids"].append(value)
            for value in result.get("skipped_inactive_task_ids", []):
                if isinstance(value, str) and value not in skipped_set:
                    skipped_set.add(value)
                    aggregate["skipped_inactive_task_ids"].append(value)
            for value in result.get("failed_task_ids", []):
                if isinstance(value, str) and value not in failed_set:
                    failed_set.add(value)
                    aggregate["failed_task_ids"].append(value)
        return aggregate

    async def _validate_direct_child_scope(
        self,
        child_task_id: str,
        *,
        operation: str,
    ) -> None:
        if not isinstance(child_task_id, str) or not child_task_id:
            raise ValueError(f"{operation} requires a non-empty task_id")

        child_task_data = await get_task_data(
            self.redis_client,
            self.namespace,
            child_task_id,
        )
        child_metadata = cast(dict[str, Any], child_task_data["metadata"])
        child_owner_id = child_metadata.get("owner_id")
        if (
            not isinstance(child_owner_id, str)
            or child_owner_id != self.task.metadata.owner_id
        ):
            raise PermissionError(
                f"{operation} can only target child tasks in the same owner scope"
            )

        child_parent_id = child_metadata.get("parent_id")
        if child_parent_id != self.task.id:
            raise PermissionError(
                f"{operation} can only target direct child tasks "
                "of the current task"
            )

        parent_team_id = self.task.metadata.team_id or self.task.id
        child_team_id = effective_team_id(
            task_id=child_task_id,
            metadata=child_metadata,
        )
        if child_team_id != parent_team_id:
            raise PermissionError(
                f"{operation} can only target child tasks in the same team scope"
            )

    async def publish_batch_progress(self, batch_id: str) -> None:
        batch = await get_batch_data(self.redis_client, self.namespace, batch_id)
        if not batch or batch.metadata.status != "active":
            return

        await self.event_publisher.publish_event(
            BatchEvent(
                event_type="batch_progress",
                batch_id=batch_id,
                owner_id=batch.metadata.owner_id,
                progress=batch.progress,
                completed_tasks=batch.metadata.total_tasks
                - len(batch.remaining_task_ids),
                total_tasks=batch.metadata.total_tasks,
                status=batch.metadata.status,
            )
        )

    async def persist_hook_runtime(self, runtime_payload: dict[str, Any]) -> None:
        await persist_hook_runtime_payload(
            redis_client=self.redis_client,
            namespace=self.namespace,
            task_id=self.task.id,
            runtime_payload=runtime_payload,
        )

    async def messaging_create_group(
        self,
        group_name: str,
        member_task_ids: list[str] | None,
    ) -> dict[str, Any]:
        return await messaging_groups_create(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            group_name=group_name,
            member_task_ids=member_task_ids,
        )

    async def messaging_get_group(self, group_name: str) -> dict[str, Any]:
        return await messaging_groups_get(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            group_name=group_name,
        )

    async def messaging_list_groups(self) -> list[dict[str, Any]]:
        return await messaging_groups_list(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
        )

    async def messaging_find_groups(self, group_name: str) -> list[dict[str, Any]]:
        return await messaging_groups_find(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            group_name=group_name,
        )

    async def messaging_add_group_members(
        self,
        group_name: str,
        member_task_ids: list[str],
    ) -> list[str]:
        return await messaging_groups_add_members(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            group_name=group_name,
            member_task_ids=member_task_ids,
        )

    async def messaging_remove_group_members(
        self,
        group_name: str,
        member_task_ids: list[str],
    ) -> list[str]:
        return await messaging_groups_remove_members(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            group_name=group_name,
            member_task_ids=member_task_ids,
        )

    async def messaging_leave_group(self, group_name: str) -> bool:
        return await messaging_groups_leave(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            group_name=group_name,
        )

    async def messaging_send_group(
        self,
        group_name: str,
        content: str,
        data: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await messaging_groups_send(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            group_name=group_name,
            content=content,
            data=data,
            metadata=metadata,
        )

    async def messaging_send_direct(
        self,
        to_task_id: str,
        content: str,
        data: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await messaging_send_direct(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            to_task_id=to_task_id,
            content=content,
            data=data,
            metadata=metadata,
        )

    async def inbox_direct_peek(
        self,
        unread_only: bool,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        return await messaging_inbox_direct_peek(
            redis_client=self.redis_client,
            namespace=self.namespace,
            task_id=self.task.id,
            unread_only=unread_only,
            limit=limit,
            cursor=cursor,
        )

    async def inbox_direct_mark_read(
        self,
        message_ids: list[str],
        notify_sender: bool,
        data: Any = None,
    ) -> dict[str, Any]:
        return await messaging_inbox_direct_mark_read(
            redis_client=self.redis_client,
            namespace=self.namespace,
            task_id=self.task.id,
            message_ids=message_ids,
            notify_sender=notify_sender,
            data=data,
        )

    async def inbox_group_peek(
        self,
        group_name: str,
        unread_only: bool,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        return await messaging_inbox_group_peek(
            redis_client=self.redis_client,
            namespace=self.namespace,
            task_id=self.task.id,
            group_name=group_name,
            unread_only=unread_only,
            limit=limit,
            cursor=cursor,
        )

    async def inbox_group_mark_read(
        self,
        group_name: str,
        message_ids: list[str],
        notify_sender: bool,
        data: Any = None,
    ) -> dict[str, Any]:
        return await messaging_inbox_group_mark_read(
            redis_client=self.redis_client,
            namespace=self.namespace,
            task_id=self.task.id,
            group_name=group_name,
            message_ids=message_ids,
            notify_sender=notify_sender,
            data=data,
        )

    async def inbox_receipts_peek(
        self,
        unread_only: bool,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        return await messaging_inbox_receipts_peek(
            redis_client=self.redis_client,
            namespace=self.namespace,
            task_id=self.task.id,
            unread_only=unread_only,
            limit=limit,
            cursor=cursor,
        )

    async def inbox_receipts_mark_read(
        self,
        receipt_ids: list[str],
    ) -> dict[str, Any]:
        return await messaging_inbox_receipts_mark_read(
            redis_client=self.redis_client,
            namespace=self.namespace,
            task_id=self.task.id,
            receipt_ids=receipt_ids,
        )

