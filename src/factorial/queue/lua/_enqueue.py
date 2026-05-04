from dataclasses import dataclass
from typing import Literal

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from factorial.core.utils import decode
from factorial.queue.lua_core import (
    LuaScriptContract,
    _decode_json_string_list,
    _execute_contract,
    get_cached_script,
)


@dataclass
class EnqueueTaskScriptResult:
    decision: Literal["enqueued", "replay", "conflict"]
    task_id: str
    request_hash: str


class EnqueueTaskScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="EnqueueTaskScript.execute",
        key_fields=(
            "agent_queue_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "enqueue_idempotency_key",
            "task_children_key_template",
        ),
        arg_fields=(
            "task_id",
            "task_agent",
            "task_payload_json",
            "task_pickups",
            "task_retries",
            "task_meta_json",
            "request_hash",
            "ttl_seconds",
            "idempotency_enabled_flag",
        ),
    )

    async def execute(
        self,
        *,
        agent_queue_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        task_id: str,
        task_agent: str,
        task_payload_json: str,
        task_pickups: int,
        task_retries: int,
        task_meta_json: str,
        enqueue_idempotency_key: str = "",
        task_children_key_template: str = "",
        request_hash: str = "",
        ttl_seconds: int = 1,
        idempotency_enabled: bool = False,
    ) -> EnqueueTaskScriptResult:
        idempotency_enabled_flag = "1" if idempotency_enabled else "0"
        execution_values = dict(locals())
        execution_values.pop("idempotency_enabled", None)
        result: tuple[str | bytes, str | bytes, str | bytes] = await _execute_contract(
            self, self._CONTRACT, execution_values
        )
        decision = decode(result[0])
        if decision not in {"enqueued", "replay", "conflict"}:
            raise ValueError(
                f"enqueue script returned unexpected decision '{decision}'"
            )
        return EnqueueTaskScriptResult(
            decision=decision,  # type: ignore[arg-type]
            task_id=decode(result[1]),
            request_hash=decode(result[2]),
        )


async def create_enqueue_task_script(redis_client: redis.Redis) -> EnqueueTaskScript:
    return get_cached_script(redis_client, "enqueue", EnqueueTaskScript)


@dataclass
class ResumeEnqueueScriptResult:
    decision: Literal["enqueued", "replay", "conflict"]
    resumed_task_id: str
    request_hash: str


class ResumeEnqueueScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="ResumeEnqueueScript.execute",
        key_fields=(
            "agent_queue_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "resume_idempotency_key",
            "task_children_key_template",
        ),
        arg_fields=(
            "task_id",
            "task_agent",
            "task_payload_json",
            "task_pickups",
            "task_retries",
            "task_meta_json",
            "request_hash",
            "source_task_id",
            "ttl_seconds",
            "idempotency_enabled_flag",
        ),
    )

    async def execute(
        self,
        *,
        agent_queue_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        resume_idempotency_key: str,
        task_id: str,
        task_agent: str,
        task_payload_json: str,
        task_pickups: int,
        task_retries: int,
        task_meta_json: str,
        request_hash: str,
        source_task_id: str,
        ttl_seconds: int,
        idempotency_enabled: bool,
        task_children_key_template: str = "",
    ) -> ResumeEnqueueScriptResult:
        idempotency_enabled_flag = "1" if idempotency_enabled else "0"
        execution_values = dict(locals())
        execution_values.pop("idempotency_enabled", None)
        result: tuple[str | bytes, str | bytes, str | bytes] = await _execute_contract(
            self, self._CONTRACT, execution_values
        )
        decision = decode(result[0])
        if decision not in {"enqueued", "replay", "conflict"}:
            raise ValueError(
                f"resume_enqueue script returned unexpected decision '{decision}'"
            )
        return ResumeEnqueueScriptResult(
            decision=decision,  # type: ignore[arg-type]
            resumed_task_id=decode(result[1]),
            request_hash=decode(result[2]),
        )


async def create_resume_enqueue_script(
    redis_client: redis.Redis,
) -> ResumeEnqueueScript:
    return get_cached_script(redis_client, "resume_enqueue", ResumeEnqueueScript)


@dataclass
class EnqueueBatchScriptResult:
    decision: Literal["enqueued", "replay", "conflict"]
    batch_id: str
    task_ids: list[str]
    request_hash: str


class EnqueueBatchScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="EnqueueBatchScript.execute",
        key_fields=(
            "agent_queue_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "batch_tasks_key",
            "batch_meta_key",
            "batch_remaining_tasks_key",
            "batch_progress_key",
            "batch_enqueue_idempotency_key",
            "task_children_key_template",
        ),
        arg_fields=(
            "batch_id",
            "owner_id",
            "created_at",
            "agent_name",
            "tasks_json",
            "base_task_meta_json",
            "batch_meta_json",
            "request_hash",
            "ttl_seconds",
            "idempotency_enabled_flag",
        ),
    )

    async def execute(
        self,
        *,
        agent_queue_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        batch_tasks_key: str,
        batch_meta_key: str,
        batch_id: str,
        owner_id: str,
        created_at: float,
        agent_name: str,
        tasks_json: str,
        base_task_meta_json: str,
        batch_meta_json: str,
        batch_remaining_tasks_key: str,
        batch_progress_key: str,
        batch_enqueue_idempotency_key: str = "",
        task_children_key_template: str = "",
        request_hash: str = "",
        ttl_seconds: int = 1,
        idempotency_enabled: bool = False,
    ) -> EnqueueBatchScriptResult:
        idempotency_enabled_flag = "1" if idempotency_enabled else "0"
        execution_values = dict(locals())
        execution_values.pop("idempotency_enabled", None)
        result: tuple[str | bytes, str | bytes, str | bytes, str | bytes] = (
            await _execute_contract(self, self._CONTRACT, execution_values)
        )
        decision = decode(result[0])
        if decision not in {"enqueued", "replay", "conflict"}:
            raise ValueError(
                f"enqueue_batch script returned unexpected decision '{decision}'"
            )
        return EnqueueBatchScriptResult(
            decision=decision,  # type: ignore[arg-type]
            batch_id=decode(result[1]),
            task_ids=_decode_json_string_list(result[2]),
            request_hash=decode(result[3]),
        )


async def create_enqueue_batch_script(redis_client: redis.Redis) -> EnqueueBatchScript:
    return get_cached_script(redis_client, "enqueue_batch", EnqueueBatchScript)
