from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any

from factorial.context import AgentContext, ExecutionContext
from factorial.utils import serialize_data
from factorial.waits import WaitInstruction, wait

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


def _coerce_inputs_for_agent(agent: Any, inputs: list[Any]) -> list[AgentContext]:
    context_class = getattr(agent, "context_class", None)
    if (
        not isinstance(context_class, type)
        or not issubclass(context_class, AgentContext)
    ):
        raise TypeError(
            f"Subagent '{getattr(agent, 'name', '<unknown>')}' has invalid "
            f"context_class '{context_class}'."
        )

    coerced_inputs: list[AgentContext] = []
    for input_item in inputs:
        if isinstance(input_item, context_class):
            coerced_inputs.append(input_item)
            continue

        if isinstance(input_item, dict):
            coerced_inputs.append(context_class.from_dict(input_item))
            continue

        model_dump = getattr(input_item, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, dict):
                coerced_inputs.append(context_class.from_dict(dumped))
                continue

        raise TypeError(
            "subagents.spawn inputs must be context instances, dicts, "
            f"or pydantic models. Got {type(input_item).__name__}."
        )

    return coerced_inputs


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
        if execution_ctx.enqueue_batch is not None:
            batch_id = _deterministic_spawn_batch_id(
                parent_task_id=execution_ctx.task_id,
                key=normalized_key,
                agent_name=agent_name,
                task_ids=deterministic_task_ids,
            )
            batch = await execution_ctx.spawn_child_tasks(
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
                    await execution_ctx.spawn_child_task(
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


subagents = SubagentsNamespace()

