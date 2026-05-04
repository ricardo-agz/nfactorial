import asyncio
import json
from dataclasses import replace
from enum import Enum
from typing import Any

import redis.asyncio as redis

from factorial._internal.lua.queue import TaskSteeringScript
from factorial._internal.queue.keys import RedisKeys
from factorial._internal.queue.task_store import get_task_steering_messages
from factorial.agent import BaseAgent
from factorial.agent.context import ContextType
from factorial.core.events import AgentEvent, EventPublisher
from factorial.core.exceptions import RETRYABLE_EXCEPTIONS, FatalAgentError
from factorial.core.logging import get_logger
from factorial.execution.context import ExecutionContext
from factorial.execution.waits import WaitInstruction
from factorial.queue.task import Task

logger = get_logger("factorial.queue")


class CompletionAction(str, Enum):
    """Canonical action names accepted by the Lua *task completion* script."""

    CONTINUE = "continue"
    PENDING_TOOL = "pending_tool_call_results"
    PENDING_CHILD = "pending_child_task_results"
    COMPLETE = "complete"
    RETRY = "retry"
    BACKOFF = "backoff"
    FAIL = "fail"


def steering_message_sort_key(message_id: str) -> tuple[int, int, str]:
    """Sort by timestamp_ms, then sequence for deterministic steering order."""
    ts_part, sep, seq_part = message_id.partition("_")
    try:
        timestamp_ms = int(ts_part)
    except ValueError:
        timestamp_ms = 0
    try:
        sequence = int(seq_part) if sep else 0
    except ValueError:
        sequence = 0
    return (timestamp_ms, sequence, message_id)


async def apply_steering_if_available(
    *,
    redis_client: redis.Redis,
    task: Task[ContextType],
    agent: BaseAgent[ContextType],
    execution_ctx: ExecutionContext,
    steering_script: TaskSteeringScript,
    namespace: str,
    event_publisher: EventPublisher,
) -> Task[ContextType]:
    """Apply queued steering messages and persist updated task context."""
    steering_messages_data = await get_task_steering_messages(
        redis_client=redis_client,
        namespace=namespace,
        task_id=task.id,
    )
    if not steering_messages_data:
        return task

    steering_messages_data.sort(
        key=lambda message_tuple: steering_message_sort_key(message_tuple[0])
    )
    steering_messages = [msg for _, msg in steering_messages_data]
    steering_message_ids = [mid for mid, _ in steering_messages_data]

    steered_task = replace(task)
    steered_task.payload = await agent.steer(
        messages=steering_messages,
        agent_ctx=task.payload,
        execution_ctx=execution_ctx,
    )

    keys = RedisKeys.format(namespace=namespace, agent=agent.name, task_id=task.id)

    try:
        steering_result = await steering_script.execute(
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            steering_messages_key=keys.task_steering,
            task_id=task.id,
            steering_message_ids=steering_message_ids,
            updated_task_payload_json=steered_task.payload.to_json(),
        )
        if not steering_result.success:
            raise RuntimeError(
                f"Task steering update failed for task {task.id}: "
                f"{steering_result.status}"
            )

        await event_publisher.publish_event(
            AgentEvent(
                event_type="run_steering_applied",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
            )
        )
        return steered_task
    except Exception as e:
        logger.error(f"Error updating task with steering messages: {e}")
        await event_publisher.publish_event(
            AgentEvent(
                event_type="run_steering_failed",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                error=str(e),
            )
        )
        return task


def classify_failure(
    exc: BaseException,
    retries: int,
    max_retries: int,
) -> tuple[CompletionAction, str | None]:
    """
    Map an exception -> (action, output_json)

    - asyncio.TimeoutError  -> RETRY unless max retries hit
    - RETRYABLE_EXCEPTIONS  -> BACKOFF unless max retries hit
    - everything else       -> RETRY unless max retries hit
    - max retries reached   -> FAIL with a JSON error message
    """
    if isinstance(exc, FatalAgentError):
        return CompletionAction.FAIL, json.dumps({"error": str(exc)})

    if isinstance(exc, asyncio.TimeoutError):
        base_action = CompletionAction.RETRY
        msg = {"error": f"Task timed out: {exc}"}
    elif isinstance(exc, tuple(RETRYABLE_EXCEPTIONS)):
        base_action = CompletionAction.BACKOFF
        msg = {"error": str(exc)}
    else:
        base_action = CompletionAction.RETRY
        msg = {"error": str(exc)}

    if retries >= max_retries:
        return CompletionAction.FAIL, json.dumps(msg)
    return base_action, None


def extract_wait_instructions(
    tool_call_results: list[tuple[Any, Any | Exception]],
) -> list[tuple[str, WaitInstruction]]:
    waits: list[tuple[str, WaitInstruction]] = []
    for tool_call, result in tool_call_results:
        if isinstance(result, WaitInstruction):
            waits.append((tool_call.id, result))
    return waits
