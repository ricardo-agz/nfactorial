import json
from dataclasses import dataclass
from typing import Any, cast

import redis.asyncio as redis

from factorial.agent import BaseAgent, serialize_data
from factorial.events import AgentEvent, BatchEvent, EventPublisher
from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    ActivityWaitScript,
    QueueScripts,
    TaskCompletionInput,
    WaitScheduleScript,
)
from factorial.queue.operations import (
    create_batch_and_enqueue,
    enqueue_task,
    messaging_groups_add_members,
    messaging_groups_create,
    messaging_groups_find,
    messaging_groups_get,
    messaging_groups_list,
    messaging_groups_send,
    messaging_send_direct,
    persist_hook_runtime_payload,
    resume_if_no_remaining_child_tasks,
)
from factorial.queue.task import Batch, Task, get_batch_data

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
                    current_turn=self.task.payload.turn,
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
    ) -> None:
        steering_key_template = RedisKeys.format(
            namespace=self.namespace,
            task_id="{task_id}",
        ).task_steering
        wait_metadata = {
            "kind": "activity",
            "source_tool_call_ids": source_tool_call_ids,
        }
        if data is not None:
            wait_metadata["data"] = serialize_data(data)
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
            queue_main_key_template=RedisKeys.format(
                namespace=self.namespace,
                agent="{agent}",
            ).queue_main,
            queue_pending_key_template=RedisKeys.format(
                namespace=self.namespace,
                agent="{agent}",
            ).queue_pending,
        )
        if not wait_result.success:
            raise RuntimeError(
                f"Failed to park activity wait for task {self.task.id}: "
                f"{wait_result.message}"
            )

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
                turn=self.task.payload.turn,
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

    async def messaging_send_group(
        self,
        group_name: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await messaging_groups_send(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            group_name=group_name,
            content=content,
            metadata=metadata,
        )

    async def messaging_send_direct(
        self,
        to_task_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await messaging_send_direct(
            redis_client=self.redis_client,
            namespace=self.namespace,
            sender_task_id=self.task.id,
            to_task_id=to_task_id,
            content=content,
            metadata=metadata,
        )

