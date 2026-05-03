from __future__ import annotations

import asyncio
import inspect
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, TypeAlias, TypeVar, cast

import httpx

from factorial import (
    HookCompletionStatus,
    HookMode,
    PendingHookSnapshot,
    RunError,
    RunResult,
    RunStatus,
    TaskSnapshot,
    TaskSnapshotStatus,
    TurnSummary,
    UsageSummary,
    VerificationSummary,
    WaitKind,
    WaitSnapshot,
    parse_event,
)
from factorial.ai.messages import Message
from factorial.core.events import BaseEvent

ProbeFunction: TypeAlias = Callable[["ProbeContext"], Awaitable[None] | None]
Predicate: TypeAlias = Callable[[Any], bool]
EventT = TypeVar("EventT", bound=BaseEvent)


@dataclass(frozen=True)
class ProbeDefinition:
    name: str
    func: ProbeFunction
    timeout_s: float


@dataclass(frozen=True)
class ProbeHookResolution:
    hook_id: str
    task_id: str
    tool_call_id: str
    status: HookCompletionStatus
    task_resumed: bool


@dataclass(frozen=True)
class ProbeMessageDelivery:
    team_id: str | None = None
    to_task_id: str | None = None
    group_id: str | None = None
    group_name: str | None = None
    thread_id: str | None = None
    thread_message_id: str | None = None
    global_message_id: str | None = None
    delivered_task_ids: tuple[str, ...] = ()
    skipped_inactive_task_ids: tuple[str, ...] = ()
    failed_task_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProbeEventRecord:
    cursor: int
    payload: dict[str, Any]
    event: BaseEvent


def probe(
    func: ProbeFunction | None = None,
    *,
    name: str | None = None,
    timeout_s: float = 30.0,
):
    def _decorate(inner: ProbeFunction) -> ProbeFunction:
        cast(Any, inner).__nfactorial_probe_definition__ = {
            "name": name or getattr(inner, "__name__", "probe"),
            "timeout_s": timeout_s,
        }
        return inner

    if func is None:
        return _decorate
    return _decorate(func)


def discover_probes(module: Any) -> list[ProbeDefinition]:
    probes: list[ProbeDefinition] = []
    for value in vars(module).values():
        metadata = getattr(value, "__nfactorial_probe_definition__", None)
        if metadata is None:
            continue
        probes.append(
            ProbeDefinition(
                name=str(metadata["name"]),
                func=cast(ProbeFunction, value),
                timeout_s=float(metadata["timeout_s"]),
            )
        )
    return probes


def _deep_get(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
            continue
        if isinstance(current, (list, tuple)) and part.isdigit():
            index = int(part)
            if 0 <= index < len(current):
                current = current[index]
                continue
        if hasattr(current, part):
            current = getattr(current, part)
            continue
        return None
    return current


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"Unsupported datetime value: {value!r}")


def _parse_usage_summary(data: Any) -> UsageSummary:
    if not isinstance(data, dict):
        return UsageSummary.zero()
    return UsageSummary(
        input_tokens=int(data.get("input_tokens", 0)),
        output_tokens=int(data.get("output_tokens", 0)),
        total_tokens=int(data.get("total_tokens", 0)),
    )


def _parse_turn_summary(data: Any) -> TurnSummary | None:
    if not isinstance(data, dict):
        return None
    return TurnSummary(
        turn_number=int(data["turn_number"]),
        finish_reason=str(data["finish_reason"]),
        status=str(data["status"]),
        output=data.get("output"),
        usage=_parse_usage_summary(data.get("usage")),
    )


def _parse_wait_snapshot(data: Any) -> WaitSnapshot | None:
    if not isinstance(data, dict):
        return None
    return WaitSnapshot(
        kind=WaitKind(str(data["kind"])),
        wake_at=_parse_datetime(data.get("wake_at")),
        signal_id=str(data["signal_id"]) if data.get("signal_id") is not None else None,
        source_tool_call_ids=tuple(data.get("source_tool_call_ids", ()) or ()),
        data=data.get("data"),
    )


def _parse_pending_hook_snapshot(data: Any) -> PendingHookSnapshot:
    if not isinstance(data, dict):
        raise TypeError(
            f"Pending hook payload must be a dict, got {type(data).__name__}"
        )
    expires_at = _parse_datetime(data.get("expires_at"))
    if expires_at is None:
        raise ValueError("Pending hook snapshot requires expires_at")
    return PendingHookSnapshot(
        id=str(data["id"]),
        hook_type=str(data["hook_type"]),
        mode=HookMode(str(data["mode"])),
        title=str(data["title"]) if data.get("title") is not None else None,
        tool_name=str(data["tool_name"]) if data.get("tool_name") is not None else None,
        param_name=(
            str(data["param_name"]) if data.get("param_name") is not None else None
        ),
        expires_at=expires_at,
        metadata=dict(data.get("metadata") or {}),
    )


def _parse_task_snapshot(data: dict[str, Any]) -> TaskSnapshot[Any, Any]:
    return TaskSnapshot(
        id=str(data["id"]),
        agent_name=str(data["agent_name"]),
        owner_id=str(data["owner_id"]),
        batch_id=str(data["batch_id"]) if data.get("batch_id") is not None else None,
        status=TaskSnapshotStatus(str(data["status"])),
        state=data.get("state"),
        metadata=data.get("metadata"),
        output=data.get("output"),
        retry_count=int(data.get("retry_count", 0)),
        turn_number=int(data.get("turn_number", 0)),
        last_turn=_parse_turn_summary(data.get("last_turn")),
        wait=_parse_wait_snapshot(data.get("wait")),
        pending_hooks=tuple(
            _parse_pending_hook_snapshot(item)
            for item in (data.get("pending_hooks", ()) or ())
        ),
        pending_child_task_ids=tuple(data.get("pending_child_task_ids", ()) or ()),
        backoff_until=_parse_datetime(data.get("backoff_until")),
    )


def _parse_run_error(data: Any) -> RunError | None:
    if not isinstance(data, dict):
        return None
    return RunError(
        type=str(data["type"]),
        message=str(data["message"]),
        traceback=str(data["traceback"]) if data.get("traceback") is not None else None,
    )


def _parse_verification_summary(data: Any) -> VerificationSummary[Any] | None:
    if not isinstance(data, dict):
        return None
    return VerificationSummary(
        status=str(data["status"]),
        attempts_used=int(data["attempts_used"]),
        code=str(data["code"]) if data.get("code") is not None else None,
        message=str(data["message"]) if data.get("message") is not None else None,
        metadata=data.get("metadata"),
    )


def _parse_run_result(data: dict[str, Any]) -> RunResult[Any, Any, Any]:
    started_at = _parse_datetime(data.get("started_at"))
    if started_at is None:
        raise ValueError("Run result requires started_at")
    return RunResult(
        run_id=str(data["run_id"]),
        task_id=str(data["task_id"]) if data.get("task_id") is not None else None,
        agent_name=str(data["agent_name"]),
        owner_id=str(data["owner_id"]) if data.get("owner_id") is not None else None,
        status=RunStatus(str(data["status"])),
        output=data.get("output"),
        state=data.get("state"),
        metadata=data.get("metadata"),
        messages=tuple(data.get("messages", ()) or ()),
        usage=_parse_usage_summary(data.get("usage")),
        turn_count=int(data.get("turn_count", 0)),
        last_turn=_parse_turn_summary(data.get("last_turn")),
        verification=_parse_verification_summary(data.get("verification")),
        started_at=started_at,
        finished_at=_parse_datetime(data.get("finished_at")),
        error=_parse_run_error(data.get("error")),
    )


def _parse_probe_event_record(data: dict[str, Any]) -> ProbeEventRecord:
    payload = cast(dict[str, Any], data["payload"])
    return ProbeEventRecord(
        cursor=int(data["cursor"]),
        payload=payload,
        event=parse_event(payload),
    )


def tool_output(
    messages: tuple[Message, ...],
    tool_name: str,
) -> dict[str, object]:
    """Return the output payload for a named tool message."""
    for message in messages:
        if message.get("role") != "tool" or message.get("tool_name") != tool_name:
            continue

        output = message.get("output")
        if isinstance(output, dict):
            return cast(dict[str, object], output)

    raise AssertionError(f"Could not find tool output for {tool_name!r}")


def tool_client_output(
    messages: tuple[Message, ...],
    tool_name: str,
) -> dict[str, object]:
    """Unwrap nested hook-continuation client output for a named tool."""
    for message in messages:
        if message.get("role") != "tool":
            continue

        output = message.get("output")
        if not isinstance(output, dict):
            continue
        output_payload = cast(dict[str, Any], output)

        tool_call = output_payload.get("tool_call")
        if not isinstance(tool_call, dict):
            continue

        function = tool_call.get("function")
        if not isinstance(function, dict):
            continue
        function_payload = cast(dict[str, Any], function)

        if function_payload.get("name") != tool_name:
            continue

        client_output = output_payload.get("client_output")
        if isinstance(client_output, dict):
            return cast(dict[str, object], client_output)

    raise AssertionError(f"Could not find client output for {tool_name!r}")


def _parse_hook_resolution(data: dict[str, Any]) -> ProbeHookResolution:
    return ProbeHookResolution(
        hook_id=str(data["hook_id"]),
        task_id=str(data["task_id"]),
        tool_call_id=str(data["tool_call_id"]),
        status=HookCompletionStatus(str(data["status"])),
        task_resumed=bool(data.get("task_resumed")),
    )


def _parse_message_delivery(data: dict[str, Any]) -> ProbeMessageDelivery:
    return ProbeMessageDelivery(
        team_id=str(data["team_id"]) if data.get("team_id") is not None else None,
        to_task_id=(
            str(data["to_task_id"]) if data.get("to_task_id") is not None else None
        ),
        group_id=str(data["group_id"]) if data.get("group_id") is not None else None,
        group_name=(
            str(data["group_name"]) if data.get("group_name") is not None else None
        ),
        thread_id=str(data["thread_id"]) if data.get("thread_id") is not None else None,
        thread_message_id=(
            str(data["thread_message_id"])
            if data.get("thread_message_id") is not None
            else None
        ),
        global_message_id=(
            str(data["global_message_id"])
            if data.get("global_message_id") is not None
            else None
        ),
        delivered_task_ids=tuple(data.get("delivered_task_ids", ()) or ()),
        skipped_inactive_task_ids=tuple(
            data.get("skipped_inactive_task_ids", ()) or ()
        ),
        failed_task_ids=tuple(data.get("failed_task_ids", ()) or ()),
    )


def _normalize_comparable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    return value


async def _poll_until(
    fetch: Callable[[], Awaitable[Any | None]],
    *,
    predicates: tuple[Predicate, ...],
    timeout_s: float,
    interval_s: float,
) -> Any:
    started_at = time.monotonic()
    last_value: Any | None = None

    while True:
        last_value = await fetch()
        if last_value is not None and all(
            predicate(last_value) for predicate in predicates
        ):
            return last_value

        if time.monotonic() - started_at >= timeout_s:
            raise AssertionError(
                f"Timed out after {timeout_s:.1f}s waiting for probe predicates. "
                f"Last value: {last_value!r}"
            )
        await asyncio.sleep(interval_s)


def _normalize_input(
    input_value: str | list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if isinstance(input_value, str):
        return [{"role": "user", "content": input_value}]
    return input_value


@dataclass
class ProbeHookHandle:
    run: RunHandle
    snapshot: PendingHookSnapshot
    _token: str | None = None

    async def resolve(
        self,
        payload: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> ProbeHookResolution:
        token = self._token
        if token is None:
            response = await self.run.ctx.client.post(
                f"/__probe/hooks/{self.snapshot.id}/token"
            )
            response.raise_for_status()
            token = response.json()["token"]
            self._token = token

        resolved = await self.run.ctx.client.post(
            f"/api/hooks/{self.snapshot.id}/resolve",
            json={
                "token": token,
                "payload": payload,
                "idempotency_key": idempotency_key,
            },
        )
        resolved.raise_for_status()
        return _parse_hook_resolution(cast(dict[str, Any], resolved.json()))


@dataclass
class RunHandle:
    ctx: ProbeContext
    task_id: str
    agent_name: str
    owner_id: str
    _event_cursor: int = 0

    async def snapshot(self) -> TaskSnapshot[Any, Any]:
        response = await self.ctx.client.get(f"/__probe/tasks/{self.task_id}")
        response.raise_for_status()
        body = cast(dict[str, Any], response.json())
        return _parse_task_snapshot(cast(dict[str, Any], body["task"]))

    async def result(self) -> RunResult[Any, Any, Any]:
        response = await self.ctx.client.get(f"/__probe/tasks/{self.task_id}/result")
        response.raise_for_status()
        body = cast(dict[str, Any], response.json())
        return _parse_run_result(cast(dict[str, Any], body["result"]))

    async def _maybe_result(self) -> RunResult[Any, Any, Any] | None:
        response = await self.ctx.client.get(f"/__probe/tasks/{self.task_id}/result")
        if response.status_code == 409:
            return None
        response.raise_for_status()
        body = cast(dict[str, Any], response.json())
        return _parse_run_result(cast(dict[str, Any], body["result"]))

    async def poll_events(
        self,
        *,
        timeout_s: float = 0.0,
        limit: int = 100,
    ) -> tuple[ProbeEventRecord, ...]:
        response = await self.ctx.client.get(
            f"/__probe/events/{self.owner_id}",
            params={
                "after": self._event_cursor,
                "limit": limit,
                "timeout_s": timeout_s,
            },
        )
        response.raise_for_status()
        body = cast(dict[str, Any], response.json())

        if body.get("truncated"):
            first_cursor = body.get("first_cursor")
            raise AssertionError(
                "Probe event journal truncated before the requested cursor "
                f"{self._event_cursor}. First available cursor: {first_cursor!r}"
            )

        records = tuple(
            _parse_probe_event_record(item)
            for item in body.get("events", [])
            if isinstance(item, dict)
        )
        if records:
            self._event_cursor = records[-1].cursor
        else:
            next_cursor = body.get("next_cursor")
            if isinstance(next_cursor, int):
                self._event_cursor = max(self._event_cursor, next_cursor)

        return tuple(
            record for record in records if record.event.task_id == self.task_id
        )

    async def wait_for(
        self,
        *predicates: Predicate,
        timeout_s: float = 30.0,
        interval_s: float = 0.25,
    ) -> TaskSnapshot[Any, Any]:
        return await _poll_until(
            self.snapshot,
            predicates=predicates,
            timeout_s=timeout_s,
            interval_s=interval_s,
        )

    async def wait_for_result(
        self,
        *predicates: Predicate,
        timeout_s: float = 30.0,
        interval_s: float = 0.25,
    ) -> RunResult[Any, Any, Any]:
        return await _poll_until(
            self._maybe_result,
            predicates=predicates,
            timeout_s=timeout_s,
            interval_s=interval_s,
        )

    async def _wait_for_any_event(
        self,
        *predicates: Predicate,
        timeout_s: float = 30.0,
        poll_timeout_s: float = 5.0,
        limit: int = 100,
    ) -> BaseEvent:
        deadline = time.monotonic() + timeout_s
        last_event: BaseEvent | None = None

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(
                    f"Timed out after {timeout_s:.1f}s waiting for event predicates. "
                    f"Last event: {last_event!r}"
                )

            records = await self.poll_events(
                timeout_s=min(poll_timeout_s, remaining),
                limit=limit,
            )
            for record in records:
                event = record.event
                last_event = event
                if all(predicate(event) for predicate in predicates):
                    return event

    async def wait_for_event(
        self,
        event_type: type[EventT],
        *predicates: Predicate,
        timeout_s: float = 30.0,
        poll_timeout_s: float = 5.0,
        limit: int = 100,
    ) -> EventT:
        event = await self._wait_for_any_event(
            lambda candidate: isinstance(candidate, event_type),
            *predicates,
            timeout_s=timeout_s,
            poll_timeout_s=poll_timeout_s,
            limit=limit,
        )
        return cast(EventT, event)

    async def wait_for_hook(
        self,
        *,
        tool_name: str | None = None,
        param_name: str | None = None,
        timeout_s: float = 30.0,
        interval_s: float = 0.25,
    ) -> ProbeHookHandle:
        def _has_matching_hook(task: TaskSnapshot[Any, Any]) -> bool:
            hooks = task.pending_hooks
            return any(
                (tool_name is None or hook.tool_name == tool_name)
                and (param_name is None or hook.param_name == param_name)
                for hook in hooks
            )

        task = await self.wait_for(
            status_is("waiting"),
            _has_matching_hook,
            timeout_s=timeout_s,
            interval_s=interval_s,
        )
        hooks = task.pending_hooks
        for hook in hooks:
            if (tool_name is None or hook.tool_name == tool_name) and (
                param_name is None or hook.param_name == param_name
            ):
                return ProbeHookHandle(run=self, snapshot=hook)
        raise AssertionError(
            "No pending hook matched "
            f"tool_name={tool_name!r}, param_name={param_name!r}"
        )

    async def message(
        self,
        content: str,
        *,
        data: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> ProbeMessageDelivery:
        response = await self.ctx.client.post(
            f"/api/tasks/{self.task_id}/message",
            json={
                "owner_id": self.owner_id,
                "content": content,
                "data": data,
                "metadata": metadata,
            },
        )
        response.raise_for_status()
        return _parse_message_delivery(cast(dict[str, Any], response.json()))

    async def steer(
        self,
        input: str | list[dict[str, Any]],
    ) -> dict[str, Any]:
        response = await self.ctx.client.post(
            f"/api/tasks/{self.task_id}/steer",
            json={"messages": _normalize_input(input)},
        )
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    async def cancel(self) -> dict[str, Any]:
        response = await self.ctx.client.post(f"/api/tasks/{self.task_id}/cancel")
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    async def wake(
        self,
        input: str | list[dict[str, Any]] | None = None,
    ) -> bool:
        response = await self.ctx.client.post(
            f"/__probe/tasks/{self.task_id}/wake",
            json={"input": input},
        )
        response.raise_for_status()
        body = cast(dict[str, Any], response.json())
        return bool(body.get("woke"))


@dataclass
class ProbeContext:
    fixture_name: str
    probe_name: str
    base_url: str
    client: httpx.AsyncClient
    owner_prefix: str | None = None

    def owner_id(self, label: str | None = None) -> str:
        prefix = self.owner_prefix or self.fixture_name
        suffix = uuid.uuid4().hex[:8]
        parts = [prefix, self.probe_name]
        if label:
            parts.append(label)
        parts.append(suffix)
        return "::".join(parts)

    def handle(
        self,
        task_id: str,
        *,
        agent_name: str,
        owner_id: str,
    ) -> RunHandle:
        return RunHandle(
            ctx=self,
            task_id=task_id,
            agent_name=agent_name,
            owner_id=owner_id,
        )

    async def run(
        self,
        agent_name: str,
        *,
        input: str | list[dict[str, Any]] | None = None,
        payload: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        owner_id: str | None = None,
    ) -> RunHandle:
        if payload is None and input is None:
            raise ValueError("ProbeContext.run requires either input= or payload=")

        request_payload = payload or {
            "messages": _normalize_input(cast(str | list[dict[str, Any]], input)),
            "turn_number": 1,
            "output": None,
            "attempt_number": 1,
            "state": state or {},
            "metadata": metadata or {},
        }
        resolved_owner_id = owner_id or self.owner_id(agent_name)

        response = await self.client.post(
            "/api/enqueue",
            json={
                "agent_name": agent_name,
                "owner_id": resolved_owner_id,
                "payload": request_payload,
            },
        )
        response.raise_for_status()
        body = cast(dict[str, Any], response.json())
        return RunHandle(
            ctx=self,
            task_id=str(body["task_id"]),
            agent_name=agent_name,
            owner_id=resolved_owner_id,
        )


def field_equals(path: str, expected: Any) -> Predicate:
    return lambda payload: _normalize_comparable(
        _deep_get(payload, path)
    ) == _normalize_comparable(expected)


def status_is(expected: str) -> Predicate:
    return field_equals("status", expected)


def event_type_is(expected: str) -> Predicate:
    return field_equals("event_type", expected)


def wait_kind_is(expected: str) -> Predicate:
    return field_equals("wait.kind", expected)


def pending_children(expected: int) -> Predicate:
    return (
        lambda payload: len(_deep_get(payload, "pending_child_task_ids") or [])
        == expected
    )


def pending_hooks(expected: int) -> Predicate:
    return lambda payload: len(_deep_get(payload, "pending_hooks") or []) == expected


def output_contains(expected: str) -> Predicate:
    def _match(payload: Any) -> bool:
        rendered = json.dumps(_deep_get(payload, "output"), sort_keys=True, default=str)
        return expected in rendered

    return _match


def output_field_equals(path: str, expected: Any) -> Predicate:
    return (
        lambda payload: _deep_get(_deep_get(payload, "output") or {}, path) == expected
    )


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
