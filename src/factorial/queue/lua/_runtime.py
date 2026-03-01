import json
from dataclasses import dataclass

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from factorial.core.utils import decode

from ._core import LuaScriptContract, _execute_contract, get_cached_script


@dataclass
class BatchPickupScriptResult:
    tasks_to_process_ids: list[str]
    tasks_to_cancel_ids: list[str]
    orphaned_task_ids: list[str]
    corrupted_task_ids: list[str]


class BatchPickupScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="BatchPickupScript.execute",
        key_fields=(
            "queue_main_key",
            "queue_cancelled_key",
            "queue_orphaned_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "task_cancellations_key",
            "processing_heartbeats_key",
            "agent_metrics_bucket_key",
            "global_metrics_bucket_key",
            "activity_wait_meta_key",
            "queue_pending_key_template",
            "queue_main_key_template",
            "task_steering_key_template",
            "message_seq_key",
            "queue_scheduled_key_template",
            "scheduled_wait_meta_key",
        ),
        arg_fields=("batch_size", "metrics_ttl"),
        optional_key_fields=frozenset(
            {
                "queue_scheduled_key_template",
                "scheduled_wait_meta_key",
            }
        ),
    )

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_cancelled_key: str,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        task_cancellations_key: str,
        processing_heartbeats_key: str,
        agent_metrics_bucket_key: str,
        global_metrics_bucket_key: str,
        batch_size: int,
        metrics_ttl: int,
        activity_wait_meta_key: str = "",
        queue_pending_key_template: str = "",
        queue_main_key_template: str = "",
        task_steering_key_template: str = "",
        message_seq_key: str = "",
        queue_scheduled_key_template: str | None = None,
        scheduled_wait_meta_key: str | None = None,
    ) -> BatchPickupScriptResult:
        result: tuple[list[str], list[str], list[str], list[str]] = (
            await _execute_contract(self, self._CONTRACT, locals())
        )
        return BatchPickupScriptResult(
            tasks_to_process_ids=[decode(task_id) for task_id in result[0]],
            tasks_to_cancel_ids=[decode(task_id) for task_id in result[1]],
            orphaned_task_ids=[decode(task_id) for task_id in result[2]],
            corrupted_task_ids=[decode(task_id) for task_id in result[3]],
        )


async def create_batch_pickup_script(redis_client: redis.Redis) -> BatchPickupScript:
    return get_cached_script(redis_client, "pickup", BatchPickupScript)


@dataclass
class TaskSteeringScriptResult:
    success: bool
    status: str


class TaskSteeringScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="TaskSteeringScript.execute",
        key_fields=(
            "queue_orphaned_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "steering_messages_key",
        ),
        arg_fields=(
            "task_id",
            "steering_message_ids_json",
            "updated_task_payload_json",
        ),
    )

    async def execute(
        self,
        *,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        steering_messages_key: str,
        task_id: str,
        steering_message_ids: list[str],
        updated_task_payload_json: str,
    ) -> TaskSteeringScriptResult:
        steering_message_ids_json = json.dumps(steering_message_ids)
        execution_values = dict(locals())
        execution_values.pop("steering_message_ids", None)
        result: tuple[bool, str | bytes] = await _execute_contract(
            self, self._CONTRACT, execution_values
        )
        return TaskSteeringScriptResult(
            success=bool(result[0]),
            status=decode(result[1]),
        )


async def create_task_steering_script(redis_client: redis.Redis) -> TaskSteeringScript:
    return get_cached_script(redis_client, "steering", TaskSteeringScript)


@dataclass
class SteeringEnqueueScriptResult:
    success: bool
    status: str
    woken: bool


class SteeringEnqueueScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="SteeringEnqueueScript.execute",
        key_fields=(
            "queue_main_key",
            "queue_orphaned_key",
            "queue_pending_key",
            "queue_scheduled_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "steering_messages_key",
            "activity_wait_meta_key",
            "scheduled_wait_meta_key",
            "message_seq_key",
        ),
        arg_fields=("task_id", "messages_json"),
    )

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_orphaned_key: str,
        queue_pending_key: str,
        queue_scheduled_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        steering_messages_key: str,
        activity_wait_meta_key: str,
        scheduled_wait_meta_key: str,
        message_seq_key: str,
        task_id: str,
        messages_json: str,
    ) -> SteeringEnqueueScriptResult:
        result: tuple[bool, str | bytes, int] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return SteeringEnqueueScriptResult(
            success=bool(result[0]),
            status=decode(result[1]),
            woken=bool(result[2]),
        )


async def create_steering_enqueue_script(
    redis_client: redis.Redis,
) -> SteeringEnqueueScript:
    return get_cached_script(redis_client, "steering_enqueue", SteeringEnqueueScript)


@dataclass
class TaskCompletionScriptResult:
    success: bool
    batch_completed: bool


class TaskCompletionScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="TaskCompletionScript.execute",
        key_fields=(
            "queue_main_key",
            "queue_completions_key",
            "queue_failed_key",
            "queue_backoff_key",
            "queue_orphaned_key",
            "queue_pending_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "batch_meta_key",
            "batch_progress_key",
            "batch_remaining_tasks_key",
            "batch_completed_key",
            "processing_heartbeats_key",
            "pending_tool_results_key",
            "pending_child_task_results_key",
            "agent_metrics_bucket_key",
            "global_metrics_bucket_key",
            "pending_child_wait_ids_key",
            "parent_pending_child_task_results_key",
            "parent_pending_child_wait_ids_key",
            "activity_wait_meta_key",
            "task_steering_key_template",
            "message_seq_key",
            "queue_main_key_template",
            "queue_pending_key_template",
            "queue_scheduled_key_template",
            "scheduled_wait_meta_key",
        ),
        arg_fields=(
            "task_id",
            "action",
            "updated_task_payload_json",
            "current_turn",
            "metrics_ttl",
            "pending_sentinel",
            "pending_tool_call_ids_json",
            "pending_child_task_ids_json",
            "final_output_json",
        ),
        optional_key_fields=frozenset(
            {
                "pending_child_wait_ids_key",
                "parent_pending_child_task_results_key",
                "parent_pending_child_wait_ids_key",
                "queue_scheduled_key_template",
                "scheduled_wait_meta_key",
            }
        ),
        optional_arg_fields=frozenset(
            {
                "pending_tool_call_ids_json",
                "pending_child_task_ids_json",
                "final_output_json",
            }
        ),
    )

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_completions_key: str,
        queue_failed_key: str,
        queue_backoff_key: str,
        queue_orphaned_key: str,
        queue_pending_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        processing_heartbeats_key: str,
        pending_tool_results_key: str,
        pending_child_task_results_key: str,
        agent_metrics_bucket_key: str,
        global_metrics_bucket_key: str,
        batch_meta_key: str,
        batch_progress_key: str,
        batch_remaining_tasks_key: str,
        batch_completed_key: str,
        task_id: str,
        action: str,
        updated_task_payload_json: str,
        metrics_ttl: int,
        pending_sentinel: str,
        current_turn: int,
        pending_child_wait_ids_key: str | None = None,
        parent_pending_child_task_results_key: str | None = None,
        parent_pending_child_wait_ids_key: str | None = None,
        pending_tool_call_ids_json: str | None = None,
        pending_child_task_ids_json: str | None = None,
        final_output_json: str | None = None,
        activity_wait_meta_key: str = "",
        task_steering_key_template: str = "",
        message_seq_key: str = "",
        queue_main_key_template: str = "",
        queue_pending_key_template: str = "",
        queue_scheduled_key_template: str | None = None,
        scheduled_wait_meta_key: str | None = None,
    ) -> TaskCompletionScriptResult:
        result: tuple[bool, bool] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return TaskCompletionScriptResult(
            success=bool(result[0]),
            batch_completed=bool(result[1]),
        )


async def create_task_completion_script(
    redis_client: redis.Redis,
) -> TaskCompletionScript:
    return get_cached_script(redis_client, "completion", TaskCompletionScript)


class ToolCompletionScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="ToolCompletionScript.execute",
        key_fields=(
            "queue_main_key",
            "queue_orphaned_key",
            "queue_pending_key",
            "pending_tool_results_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
        ),
        arg_fields=("task_id", "updated_task_context_json"),
    )

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_orphaned_key: str,
        queue_pending_key: str,
        pending_tool_results_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        task_id: str,
        updated_task_context_json: str,
    ) -> tuple[bool, str]:
        result: tuple[int, str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return bool(result[0]), decode(result[1])


async def create_tool_completion_script(
    redis_client: redis.Redis,
) -> ToolCompletionScript:
    return get_cached_script(redis_client, "tool_completion", ToolCompletionScript)


class ChildTaskCompletionScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="ChildTaskCompletionScript.execute",
        key_fields=(
            "queue_main_key",
            "queue_orphaned_key",
            "queue_pending_key",
            "pending_child_task_results_key",
            "pending_child_wait_ids_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
        ),
        arg_fields=("task_id", "updated_task_context_json"),
        optional_key_fields=frozenset({"pending_child_wait_ids_key"}),
    )

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_orphaned_key: str,
        queue_pending_key: str,
        pending_child_task_results_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        task_id: str,
        updated_task_context_json: str,
        pending_child_wait_ids_key: str | None = None,
    ) -> tuple[bool, str]:
        result: tuple[int, str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return bool(result[0]), decode(result[1])


async def create_child_task_completion_script(
    redis_client: redis.Redis,
) -> ChildTaskCompletionScript:
    return get_cached_script(
        redis_client, "child_completion", ChildTaskCompletionScript
    )


@dataclass
class ActivityWaitScriptResult:
    success: bool
    message: str
    parent_woken: bool


class ActivityWaitScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="ActivityWaitScript.execute",
        key_fields=(
            "queue_pending_key",
            "queue_orphaned_key",
            "processing_heartbeats_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "activity_wait_meta_key",
            "message_seq_key",
            "queue_scheduled_key_template",
            "scheduled_wait_meta_key",
        ),
        arg_fields=(
            "task_id",
            "updated_task_payload_json",
            "wait_metadata_json",
            "task_steering_key_template",
            "task_children_key_template",
            "queue_main_key_template",
            "queue_pending_key_template",
            "timeout_wake_timestamp",
            "scheduled_wait_metadata_json",
        ),
        optional_key_fields=frozenset(
            {
                "queue_scheduled_key_template",
                "scheduled_wait_meta_key",
            }
        ),
        optional_arg_fields=frozenset(
            {
                "timeout_wake_timestamp",
                "scheduled_wait_metadata_json",
            }
        ),
    )

    async def execute(
        self,
        *,
        queue_pending_key: str,
        queue_orphaned_key: str,
        processing_heartbeats_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        activity_wait_meta_key: str,
        message_seq_key: str,
        task_id: str,
        updated_task_payload_json: str,
        wait_metadata_json: str,
        task_steering_key_template: str,
        task_children_key_template: str,
        queue_main_key_template: str,
        queue_pending_key_template: str,
        queue_scheduled_key_template: str | None = None,
        scheduled_wait_meta_key: str | None = None,
        timeout_wake_timestamp: float | None = None,
        scheduled_wait_metadata_json: str | None = None,
    ) -> ActivityWaitScriptResult:
        result: tuple[bool, str | bytes, int] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return ActivityWaitScriptResult(
            success=bool(result[0]),
            message=decode(result[1]),
            parent_woken=bool(result[2]),
        )


async def create_activity_wait_script(redis_client: redis.Redis) -> ActivityWaitScript:
    return get_cached_script(redis_client, "activity_wait", ActivityWaitScript)


@dataclass
class WaitScheduleScriptResult:
    success: bool
    message: str


class WaitScheduleScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="WaitScheduleScript.execute",
        key_fields=(
            "queue_scheduled_key",
            "queue_pending_key",
            "queue_orphaned_key",
            "processing_heartbeats_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "scheduled_wait_meta_key",
        ),
        arg_fields=(
            "task_id",
            "updated_task_payload_json",
            "wake_timestamp",
            "wait_metadata_json",
        ),
    )

    async def execute(
        self,
        *,
        queue_scheduled_key: str,
        queue_pending_key: str,
        queue_orphaned_key: str,
        processing_heartbeats_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        scheduled_wait_meta_key: str,
        task_id: str,
        updated_task_payload_json: str,
        wake_timestamp: float,
        wait_metadata_json: str,
    ) -> WaitScheduleScriptResult:
        result: tuple[bool, str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return WaitScheduleScriptResult(
            success=bool(result[0]),
            message=decode(result[1]),
        )


async def create_wait_schedule_script(redis_client: redis.Redis) -> WaitScheduleScript:
    return get_cached_script(redis_client, "schedule_wait", WaitScheduleScript)


@dataclass
class CancelTaskScriptResult:
    success: bool
    current_status: str | None
    message: str
    owner_id: str | None


class CancelTaskScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="CancelTaskScript.execute",
        key_fields=(
            "queue_cancelled_key",
            "queue_backoff_key",
            "queue_orphaned_key",
            "queue_pending_key",
            "pending_cancellations_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "pending_tool_results_key",
            "pending_child_task_results_key",
            "agent_metrics_bucket_key",
            "global_metrics_bucket_key",
            "queue_scheduled_key",
            "scheduled_wait_meta_key",
            "pending_child_wait_ids_key",
            "activity_wait_meta_key",
            "queue_main_key_template",
            "queue_pending_key_template",
            "queue_scheduled_key_template",
            "task_steering_key_template",
            "message_seq_key",
        ),
        arg_fields=("task_id", "metrics_ttl"),
        optional_key_fields=frozenset(
            {
                "queue_scheduled_key",
                "scheduled_wait_meta_key",
                "pending_child_wait_ids_key",
                "activity_wait_meta_key",
                "queue_main_key_template",
                "queue_pending_key_template",
                "queue_scheduled_key_template",
                "task_steering_key_template",
                "message_seq_key",
            }
        ),
    )

    async def execute(
        self,
        *,
        queue_cancelled_key: str,
        queue_backoff_key: str,
        queue_orphaned_key: str,
        queue_pending_key: str,
        pending_cancellations_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        pending_tool_results_key: str,
        pending_child_task_results_key: str,
        agent_metrics_bucket_key: str,
        global_metrics_bucket_key: str,
        task_id: str,
        metrics_ttl: int,
        queue_scheduled_key: str | None = None,
        scheduled_wait_meta_key: str | None = None,
        pending_child_wait_ids_key: str | None = None,
        activity_wait_meta_key: str | None = None,
        queue_main_key_template: str | None = None,
        queue_pending_key_template: str | None = None,
        queue_scheduled_key_template: str | None = None,
        task_steering_key_template: str | None = None,
        message_seq_key: str | None = None,
    ) -> CancelTaskScriptResult:
        result: tuple[bool, str | bytes | None, str | bytes, str | bytes | None] = (
            await _execute_contract(self, self._CONTRACT, locals())
        )
        return CancelTaskScriptResult(
            success=bool(result[0]),
            current_status=decode(result[1]) if result[1] else None,
            message=decode(result[2]),
            owner_id=decode(result[3]) if result[3] else None,
        )


async def create_cancel_task_script(redis_client: redis.Redis) -> CancelTaskScript:
    return get_cached_script(redis_client, "cancellation", CancelTaskScript)
