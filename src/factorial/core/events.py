from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypeAlias, TypedDict, cast

from redis.asyncio import Redis

from factorial.core.run_types import RunError, RunStatus, UsageSummary
from factorial.core.utils import serialize_data


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(kw_only=True)
class BaseEvent:
    event_type: str
    task_id: str | None = None
    owner_id: str | None = None
    timestamp: datetime = field(default_factory=_utcnow)
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = serialize_data(self)
        if not isinstance(payload, dict):
            raise TypeError("Event serialization must produce a mapping payload")
        payload["timestamp"] = self.timestamp.isoformat()
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))


@dataclass(kw_only=True)
class QueueEvent(BaseEvent):
    agent_name: str | None = None
    worker_id: str | None = None
    batch_id: str | None = None
    error: str | None = None


@dataclass(kw_only=True)
class AgentEvent(BaseEvent):
    agent_name: str | None = None
    turn: int | None = None
    data: Any = None
    error: str | None = None


@dataclass(kw_only=True)
class BatchEvent(BaseEvent):
    batch_id: str | None = None
    progress: float | None = None
    completed_tasks: int | None = None
    total_tasks: int | None = None
    status: str | None = None


@dataclass(kw_only=True)
class StartEvent(AgentEvent):
    event_type: str = "start"


@dataclass(kw_only=True)
class TurnStartEvent(AgentEvent):
    event_type: str = "turn_start"


@dataclass(kw_only=True)
class ModelStartEvent(AgentEvent):
    event_type: str = "model_start"
    model_name: str | None = None


@dataclass(kw_only=True)
class ModelFinishEvent(AgentEvent):
    event_type: str = "model_finish"
    model_name: str | None = None
    finish_reason: str | None = None
    usage: UsageSummary = field(default_factory=UsageSummary.zero)


@dataclass(kw_only=True)
class ToolStartEvent(AgentEvent):
    event_type: str = "tool_start"
    tool_name: str | None = None
    tool_call_id: str | None = None


@dataclass(kw_only=True)
class ToolFinishEvent(AgentEvent):
    event_type: str = "tool_finish"
    tool_name: str | None = None
    tool_call_id: str | None = None
    output: Any = None
    is_error: bool = False


@dataclass(kw_only=True)
class TurnFinishEvent(AgentEvent):
    event_type: str = "turn_finish"
    finish_reason: str | None = None
    output: Any = None
    pending_tool_call_ids: tuple[str, ...] = ()
    pending_child_task_ids: tuple[str, ...] = ()
    usage: UsageSummary = field(default_factory=UsageSummary.zero)


@dataclass(kw_only=True)
class WaitEvent(AgentEvent):
    event_type: str = "wait"
    wait_kind: str | None = None
    wake_at: str | None = None
    signal_id: str | None = None
    source_tool_call_ids: tuple[str, ...] = ()
    pending_child_task_ids: tuple[str, ...] = ()


@dataclass(kw_only=True)
class FinishEvent(AgentEvent):
    event_type: str = "finish"
    status: RunStatus = RunStatus.COMPLETED
    output: Any = None
    run_error: RunError | None = None
    turn_count: int | None = None
    usage: UsageSummary = field(default_factory=UsageSummary.zero)


TypedAgentEvent: TypeAlias = (
    StartEvent
    | TurnStartEvent
    | ModelStartEvent
    | ModelFinishEvent
    | ToolStartEvent
    | ToolFinishEvent
    | TurnFinishEvent
    | WaitEvent
    | FinishEvent
)


class _AgentEventCommonKwargs(TypedDict):
    task_id: str | None
    owner_id: str | None
    agent_name: str | None
    turn: int | None
    metadata: dict[str, Any] | None
    timestamp: datetime


def _parse_usage(value: Any) -> UsageSummary:
    if isinstance(value, UsageSummary):
        return value
    if isinstance(value, dict):
        return UsageSummary(
            input_tokens=int(value.get("input_tokens", 0)),
            output_tokens=int(value.get("output_tokens", 0)),
            total_tokens=int(value.get("total_tokens", 0)),
        )
    return UsageSummary.zero()


def _parse_run_error(value: Any) -> RunError | None:
    if value is None:
        return None
    if isinstance(value, RunError):
        return value
    if isinstance(value, dict):
        return RunError(
            type=str(value.get("type", "Error")),
            message=str(value.get("message", "")),
            traceback=(
                str(value["traceback"]) if value.get("traceback") is not None else None
            ),
        )
    return RunError(type="Error", message=str(value))


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return _utcnow()
    return _utcnow()


def _maybe_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _maybe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _maybe_dict(value: Any) -> dict[str, Any] | None:
    return cast(dict[str, Any], value) if isinstance(value, dict) else None


def _tuple_of_strings(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple, set)):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _common_agent_event_kwargs(payload: dict[str, Any]) -> _AgentEventCommonKwargs:
    return {
        "task_id": _maybe_str(payload.get("task_id")),
        "owner_id": _maybe_str(payload.get("owner_id")),
        "agent_name": _maybe_str(payload.get("agent_name")),
        "turn": _maybe_int(
            payload.get("turn")
            if payload.get("turn") is not None
            else payload.get("turn_number")
        ),
        "metadata": _maybe_dict(payload.get("metadata")),
        "timestamp": _parse_timestamp(payload.get("timestamp")),
    }


def parse_event(payload: dict[str, Any]) -> BaseEvent:
    event_type = str(payload.get("event_type", "update"))
    common_kwargs = _common_agent_event_kwargs(payload)

    if event_type in {"start", "run_started"}:
        return StartEvent(**common_kwargs)
    if event_type in {"turn_start"}:
        return TurnStartEvent(**common_kwargs)
    if event_type in {"model_start"}:
        return ModelStartEvent(
            **common_kwargs,
            model_name=cast(str | None, payload.get("model_name")),
        )
    if event_type in {"model_finish"}:
        return ModelFinishEvent(
            **common_kwargs,
            model_name=cast(str | None, payload.get("model_name")),
            finish_reason=cast(str | None, payload.get("finish_reason")),
            usage=_parse_usage(payload.get("usage")),
        )
    if event_type in {"tool_start"}:
        return ToolStartEvent(
            **common_kwargs,
            tool_name=cast(str | None, payload.get("tool_name")),
            tool_call_id=cast(str | None, payload.get("tool_call_id")),
        )
    if event_type in {"tool_finish"}:
        return ToolFinishEvent(
            **common_kwargs,
            tool_name=cast(str | None, payload.get("tool_name")),
            tool_call_id=cast(str | None, payload.get("tool_call_id")),
            output=payload.get("output"),
            is_error=bool(payload.get("is_error", False)),
        )
    if event_type in {"turn_finish"}:
        return TurnFinishEvent(
            **common_kwargs,
            finish_reason=cast(str | None, payload.get("finish_reason")),
            output=payload.get("output"),
            pending_tool_call_ids=tuple(payload.get("pending_tool_call_ids", ()) or ()),
            pending_child_task_ids=tuple(
                payload.get("pending_child_task_ids", ()) or ()
            ),
            usage=_parse_usage(payload.get("usage")),
        )
    if event_type in {
        "wait",
        "task_paused",
        "task_activity_waiting",
        "task_signal_waiting",
        "task_signal_wait_satisfied",
    }:
        data = _maybe_dict(payload.get("data"))
        wait_kind = _maybe_str(payload.get("wait_kind"))
        if wait_kind is None and data is not None:
            wait_kind = _maybe_str(data.get("wait_kind"))
        wake_at = payload.get("wake_at")
        if wake_at is None and data is not None:
            wake_at = data.get("wake_timestamp")
        if wake_at is not None:
            wake_at = str(wake_at)
        signal_id = _maybe_str(payload.get("signal_id"))
        if signal_id is None and data is not None:
            signal_id = _maybe_str(data.get("signal_id"))
        source_tool_call_ids = _tuple_of_strings(payload.get("source_tool_call_ids"))
        if not source_tool_call_ids and data is not None:
            source_tool_call_ids = _tuple_of_strings(data.get("source_tool_call_ids"))
        pending_child_task_ids = _tuple_of_strings(
            payload.get("pending_child_task_ids")
        )
        if not pending_child_task_ids and data is not None:
            pending_child_task_ids = _tuple_of_strings(
                data.get("pending_child_task_ids")
            )
        return WaitEvent(
            **common_kwargs,
            wait_kind=wait_kind,
            wake_at=cast(str | None, wake_at),
            signal_id=signal_id,
            source_tool_call_ids=source_tool_call_ids,
            pending_child_task_ids=pending_child_task_ids,
        )
    if event_type in {"finish", "run_completed", "run_failed", "run_cancelled"}:
        status = RunStatus.COMPLETED
        if event_type == "run_failed":
            status = RunStatus.FAILED
        elif event_type == "run_cancelled":
            status = RunStatus.CANCELLED
        elif payload.get("status") is not None:
            status = RunStatus(str(payload["status"]))
        return FinishEvent(
            **common_kwargs,
            status=status,
            output=payload.get("output") or payload.get("data"),
            run_error=_parse_run_error(
                payload.get("run_error") or payload.get("error")
            ),
            turn_count=cast(int | None, payload.get("turn_count")),
            usage=_parse_usage(payload.get("usage")),
        )

    if "batch_id" in payload:
        return BatchEvent(
            event_type=event_type,
            task_id=_maybe_str(payload.get("task_id")),
            owner_id=_maybe_str(payload.get("owner_id")),
            timestamp=_parse_timestamp(payload.get("timestamp")),
            metadata=_maybe_dict(payload.get("metadata")),
            batch_id=_maybe_str(payload.get("batch_id")),
            progress=cast(float | None, payload.get("progress")),
            completed_tasks=_maybe_int(payload.get("completed_tasks")),
            total_tasks=_maybe_int(payload.get("total_tasks")),
            status=_maybe_str(payload.get("status")),
        )

    if payload.get("agent_name") is not None:
        return AgentEvent(
            event_type=event_type,
            task_id=_maybe_str(payload.get("task_id")),
            owner_id=_maybe_str(payload.get("owner_id")),
            timestamp=_parse_timestamp(payload.get("timestamp")),
            metadata=_maybe_dict(payload.get("metadata")),
            agent_name=_maybe_str(payload.get("agent_name")),
            turn=_maybe_int(payload.get("turn")),
            data=payload.get("data"),
            error=_maybe_str(payload.get("error")),
        )

    return QueueEvent(
        event_type=event_type,
        task_id=_maybe_str(payload.get("task_id")),
        owner_id=_maybe_str(payload.get("owner_id")),
        timestamp=_parse_timestamp(payload.get("timestamp")),
        metadata=_maybe_dict(payload.get("metadata")),
        agent_name=_maybe_str(payload.get("agent_name")),
        worker_id=_maybe_str(payload.get("worker_id")),
        batch_id=_maybe_str(payload.get("batch_id")),
        error=_maybe_str(payload.get("error")),
    )


class EventPublisher:
    def __init__(self, redis_client: Redis, channel: str):
        self.redis_client = redis_client
        self.channel = channel

    async def publish_event(self, event: BaseEvent) -> None:
        await self.redis_client.publish(self.channel, event.to_json())


__all__ = [
    "AgentEvent",
    "BaseEvent",
    "BatchEvent",
    "EventPublisher",
    "FinishEvent",
    "ModelFinishEvent",
    "ModelStartEvent",
    "QueueEvent",
    "StartEvent",
    "ToolFinishEvent",
    "ToolStartEvent",
    "TurnFinishEvent",
    "TurnStartEvent",
    "TypedAgentEvent",
    "WaitEvent",
    "parse_event",
]
