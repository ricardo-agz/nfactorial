from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any

from factorial.agent.context import AgentContext
from factorial.core.utils import serialize_data
from factorial.execution.context import ExecutionContext
from factorial.execution.waits import WaitInstruction, wait

# Readable UUID namespace tree for deterministic spawn IDs.
# The leaf labels (`task-id.v1`, `batch-id.v1`) must be stable once shipped.
_NAMESPACE_ROOT = uuid.uuid5(uuid.NAMESPACE_DNS, "factorial.sh")
_SUBAGENT_NAMESPACE_ROOT = uuid.uuid5(_NAMESPACE_ROOT, "subagents.spawn")
_SUBAGENT_SPAWN_NAMESPACE = uuid.uuid5(_SUBAGENT_NAMESPACE_ROOT, "task-id.v1")
_SUBAGENT_BATCH_NAMESPACE = uuid.uuid5(_SUBAGENT_NAMESPACE_ROOT, "batch-id.v1")


@dataclass(frozen=True)
class JobRef:
    """Reference to a spawned child task.

    `key` is metadata for caller-side grouping/debugging only; runtime join
    semantics are driven by `task_id`/`parent_task_id`.
    """

    task_id: str
    agent_name: str
    parent_task_id: str
    key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "agent_name": self.agent_name,
            "parent_task_id": self.parent_task_id,
            "key": self.key,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> JobRef:
        return cls(
            task_id=str(data["task_id"]),
            agent_name=str(data["agent_name"]),
            parent_task_id=str(data["parent_task_id"]),
            key=str(data["key"]) if data.get("key") is not None else None,
        )


@dataclass(frozen=True)
class SignalDeliveryReport:
    signal_id: str
    target_task_ids: list[str]
    signaled_task_ids: list[str]
    woken_task_ids: list[str]
    skipped_inactive_task_ids: list[str]
    failed_task_ids: list[str]


def _coerce_inputs_for_agent(agent: Any, inputs: list[Any]) -> list[AgentContext]:
    context_from_dict = getattr(agent, "context_from_dict", None)
    build_context = getattr(agent, "build_context", None)
    if not callable(context_from_dict) or not callable(build_context):
        raise TypeError(
            f"Subagent '{getattr(agent, 'name', '<unknown>')}' "
            "cannot coerce agent inputs."
        )

    coerced_inputs: list[AgentContext] = []
    for input_item in inputs:
        if isinstance(input_item, AgentContext):
            coerced_inputs.append(input_item)
            continue

        if isinstance(input_item, str):
            coerced_inputs.append(build_context(input_item))
            continue

        if isinstance(input_item, list) and all(
            isinstance(message, dict) for message in input_item
        ):
            coerced_inputs.append(build_context(input_item))
            continue

        if isinstance(input_item, dict):
            if isinstance(input_item.get("role"), str):
                coerced_inputs.append(build_context([input_item]))
            else:
                coerced_inputs.append(context_from_dict(input_item))
            continue

        model_dump = getattr(input_item, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, dict):
                if isinstance(dumped.get("role"), str):
                    coerced_inputs.append(build_context([dumped]))
                else:
                    coerced_inputs.append(context_from_dict(dumped))
                continue

        raise TypeError(
            "subagents.spawn inputs must be context instances, strings, message "
            f"lists, dicts, or pydantic models. Got {type(input_item).__name__}."
        )

    return coerced_inputs


def _coerce_task_id(task_or_ref: Any) -> str:
    if isinstance(task_or_ref, str) and task_or_ref:
        return task_or_ref

    if isinstance(task_or_ref, dict):
        candidate = task_or_ref.get("task_id")
        if isinstance(candidate, str) and candidate:
            return candidate

    candidate = getattr(task_or_ref, "task_id", None)
    if isinstance(candidate, str) and candidate:
        return candidate

    raise TypeError(
        "subagents expects a task_id string or JobRef-like object with task_id"
    )


def _normalize_signal_id(signal_id: str) -> str:
    if not isinstance(signal_id, str) or not signal_id.strip():
        raise ValueError("subagents.signal requires a non-empty signal_id")
    return signal_id.strip()


def _signal_delivery_from_dict(
    data: dict[str, Any],
    *,
    signal_id: str,
    fallback_target_task_ids: list[str],
) -> SignalDeliveryReport:
    def _as_string_list(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [item for item in value if isinstance(item, str)]

    target_task_ids = _as_string_list(data.get("target_task_ids"))
    return SignalDeliveryReport(
        signal_id=str(data.get("signal_id", signal_id)),
        target_task_ids=target_task_ids or list(fallback_target_task_ids),
        signaled_task_ids=_as_string_list(data.get("signaled_task_ids")),
        woken_task_ids=_as_string_list(data.get("woken_task_ids")),
        skipped_inactive_task_ids=_as_string_list(
            data.get("skipped_inactive_task_ids")
        ),
        failed_task_ids=_as_string_list(data.get("failed_task_ids")),
    )


def _deterministic_child_task_id(
    *,
    parent_task_id: str,
    key: str,
    agent_name: str,
    input_index: int,
    payload: AgentContext,
) -> str:
    """Build a stable child task ID for idempotent spawn semantics.

    Same (parent_task_id, key, agent_name, input_index, payload) => same task ID.
    """
    payload_json = json.dumps(
        serialize_data(payload.to_dict()),
        sort_keys=True,
        separators=(",", ":"),
    )
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    seed = f"{parent_task_id}:{key}:{agent_name}:{input_index}:{payload_hash}"
    return str(uuid.uuid5(_SUBAGENT_SPAWN_NAMESPACE, seed))


def _deterministic_spawn_batch_id(
    *,
    parent_task_id: str,
    key: str,
    agent_name: str,
    task_ids: list[str],
) -> str:
    """Build a stable batch ID for deterministic batched spawn replays."""
    ids_fingerprint = hashlib.sha256(",".join(task_ids).encode("utf-8")).hexdigest()
    seed = f"{parent_task_id}:{key}:{agent_name}:{ids_fingerprint}"
    return str(uuid.uuid5(_SUBAGENT_BATCH_NAMESPACE, seed))


class SubagentsNamespace:
    """Imperative subagent orchestration helpers."""

    async def spawn(
        self,
        *,
        agent: Any,
        inputs: list[Any],
        key: str,
    ) -> list[JobRef]:
        """Spawn child tasks immediately and return stable job refs.

        The `key` defines idempotency scope for this parent task. Reusing the same
        key + agent + inputs yields the same child task IDs (replay-safe).

        To intentionally run duplicate experiments:
        - include duplicate payloads in one spawn call (input_index differentiates), or
        - use distinct keys across separate spawn calls.
        """
        if not inputs:
            return []

        agent_name = getattr(agent, "name", None)
        if not isinstance(agent_name, str) or not agent_name:
            raise ValueError("subagents.spawn requires an agent with a non-empty name")
        if not isinstance(key, str) or not key.strip():
            raise ValueError("subagents.spawn requires a non-empty key")

        execution_ctx = ExecutionContext.current()
        normalized_key = key.strip()
        coerced_inputs = _coerce_inputs_for_agent(agent, inputs)
        deterministic_task_ids = [
            _deterministic_child_task_id(
                parent_task_id=execution_ctx.task_id,
                key=normalized_key,
                agent_name=agent_name,
                input_index=index,
                payload=payload,
            )
            for index, payload in enumerate(coerced_inputs)
        ]

        task_ids: list[str]
        if execution_ctx.subagents.has_enqueue_batch:
            batch_id = _deterministic_spawn_batch_id(
                parent_task_id=execution_ctx.task_id,
                key=normalized_key,
                agent_name=agent_name,
                task_ids=deterministic_task_ids,
            )
            batch = await execution_ctx.subagents.enqueue_batch(
                agent,
                coerced_inputs,
                task_ids=deterministic_task_ids,
                batch_id=batch_id,
            )
            task_ids = list(batch.task_ids)
        else:
            task_ids = []
            for index, payload in enumerate(coerced_inputs):
                task_ids.append(
                    await execution_ctx.subagents.enqueue(
                        agent,
                        payload,
                        task_id=deterministic_task_ids[index],
                    )
                )

        return [
            JobRef(
                task_id=task_id,
                agent_name=agent_name,
                parent_task_id=execution_ctx.task_id,
                key=normalized_key,
            )
            for task_id in task_ids
        ]

    async def run(
        self,
        *,
        agent: Any,
        inputs: list[Any],
        key: str,
        data: Any = None,
    ) -> WaitInstruction:
        jobs = await self.spawn(agent=agent, inputs=inputs, key=key)
        return wait.jobs(jobs, data=data)

    async def cancel(self, task_or_refs: Any) -> str | list[str]:
        """Cancel one or more previously spawned direct child tasks."""
        execution_ctx = ExecutionContext.current()
        if isinstance(task_or_refs, list):
            if not task_or_refs:
                return []
            task_ids = [_coerce_task_id(task_or_ref) for task_or_ref in task_or_refs]
            deduped_task_ids = list(dict.fromkeys(task_ids))
            await execution_ctx.subagents.cancel_many(deduped_task_ids)
            return deduped_task_ids

        task_id = _coerce_task_id(task_or_refs)
        await execution_ctx.subagents.cancel(task_id)
        return task_id

    async def signal(
        self,
        task_or_refs: Any | list[Any],
        *,
        signal_id: str,
        payload: Any = None,
    ) -> SignalDeliveryReport:
        """Signal one or more direct child tasks to resume from wait.until_signal."""
        execution_ctx = ExecutionContext.current()
        normalized_signal_id = _normalize_signal_id(signal_id)
        if isinstance(task_or_refs, list):
            if not task_or_refs:
                return SignalDeliveryReport(
                    signal_id=normalized_signal_id,
                    target_task_ids=[],
                    signaled_task_ids=[],
                    woken_task_ids=[],
                    skipped_inactive_task_ids=[],
                    failed_task_ids=[],
                )
            task_ids = [_coerce_task_id(task_or_ref) for task_or_ref in task_or_refs]
            deduped_task_ids = list(dict.fromkeys(task_ids))
            result = await execution_ctx.subagents.signal_many(
                deduped_task_ids,
                normalized_signal_id,
                payload,
            )
            return _signal_delivery_from_dict(
                result,
                signal_id=normalized_signal_id,
                fallback_target_task_ids=deduped_task_ids,
            )

        task_id = _coerce_task_id(task_or_refs)
        result = await execution_ctx.subagents.signal(
            task_id,
            normalized_signal_id,
            payload,
        )
        return _signal_delivery_from_dict(
            result,
            signal_id=normalized_signal_id,
            fallback_target_task_ids=[task_id],
        )


subagents = SubagentsNamespace()

