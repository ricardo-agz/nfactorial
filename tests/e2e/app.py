from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException

from .loader import FixtureBundle, load_fixture_bundle, resolve_orchestrator


@dataclass(frozen=True)
class ProbeEventEntry:
    cursor: int
    payload: dict[str, Any]


class ProbeEventJournal:
    def __init__(self, *, max_events_per_owner: int = 1000) -> None:
        self._max_events_per_owner = max_events_per_owner
        self._events: dict[str, deque[ProbeEventEntry]] = defaultdict(deque)
        self._next_cursor: dict[str, int] = defaultdict(int)
        self._condition = asyncio.Condition()

    def _slice_entries(
        self,
        *,
        owner_id: str,
        after: int,
        limit: int,
    ) -> tuple[list[ProbeEventEntry], int, int | None]:
        entries = self._events.get(owner_id)
        next_cursor = self._next_cursor.get(owner_id, 0)
        if not entries:
            return [], next_cursor, None

        first_cursor = entries[0].cursor
        matching = [entry for entry in entries if entry.cursor > after]
        if limit >= 0:
            matching = matching[:limit]
        return matching, next_cursor, first_cursor

    async def append(self, *, owner_id: str, payload: dict[str, Any]) -> int:
        async with self._condition:
            cursor = self._next_cursor[owner_id] + 1
            self._next_cursor[owner_id] = cursor

            owner_entries = self._events[owner_id]
            owner_entries.append(ProbeEventEntry(cursor=cursor, payload=payload))
            while len(owner_entries) > self._max_events_per_owner:
                owner_entries.popleft()

            self._condition.notify_all()
            return cursor

    async def get_after(
        self,
        *,
        owner_id: str,
        after: int,
        limit: int,
    ) -> tuple[list[ProbeEventEntry], int, int | None]:
        async with self._condition:
            return self._slice_entries(owner_id=owner_id, after=after, limit=limit)

    async def wait_after(
        self,
        *,
        owner_id: str,
        after: int,
        limit: int,
        timeout_s: float,
    ) -> tuple[list[ProbeEventEntry], int, int | None]:
        deadline = time.monotonic() + max(timeout_s, 0.0)
        async with self._condition:
            while True:
                matching, next_cursor, first_cursor = self._slice_entries(
                    owner_id=owner_id,
                    after=after,
                    limit=limit,
                )
                if matching:
                    return matching, next_cursor, first_cursor

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return [], next_cursor, first_cursor

                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    next_cursor = self._next_cursor.get(owner_id, 0)
                    entries = self._events.get(owner_id)
                    first_cursor = entries[0].cursor if entries else None
                    return [], next_cursor, first_cursor


def _default_fixture_namespace(
    fixture: FixtureBundle,
    *,
    namespace: str | None,
) -> str:
    if namespace is not None and namespace.strip():
        return namespace.strip()

    env_namespace = os.getenv("NFACTORIAL_FIXTURE_NAMESPACE")
    if env_namespace and env_namespace.strip():
        return env_namespace.strip()

    return f"e2e_fixture.{fixture.name.replace('-', '_')}"


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return {key: _serialize(item) for key, item in asdict(value).items()}
    if isinstance(value, tuple):
        return [_serialize(item) for item in value]
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    return value


async def _collect_probe_events(
    *,
    orchestrator: Any,
    event_journal: ProbeEventJournal,
    stop_event: asyncio.Event,
) -> None:
    pattern = f"{orchestrator.namespace}:updates:*"
    async with orchestrator.redis_client_context() as redis_client:
        pubsub = redis_client.pubsub()
        await pubsub.psubscribe(pattern)
        try:
            while not stop_event.is_set():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=0.5,
                )
                if not message or message.get("type") != "pmessage":
                    continue

                raw_payload = message.get("data")
                raw_channel = message.get("channel")
                payload_text = (
                    raw_payload.decode("utf-8")
                    if isinstance(raw_payload, bytes)
                    else str(raw_payload)
                )
                channel_text = (
                    raw_channel.decode("utf-8")
                    if isinstance(raw_channel, bytes)
                    else str(raw_channel)
                )

                try:
                    payload = json.loads(payload_text)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, dict):
                    continue

                owner_id = payload.get("owner_id")
                if not isinstance(owner_id, str) or not owner_id:
                    owner_id = channel_text.rsplit(":", 1)[-1]
                if not owner_id:
                    continue

                await event_journal.append(owner_id=owner_id, payload=payload)
        finally:
            await pubsub.punsubscribe(pattern)
            await pubsub.aclose()


def build_probe_router(
    *,
    fixture: FixtureBundle,
    orchestrator: Any,
    event_journal: ProbeEventJournal,
) -> APIRouter:
    router = APIRouter(prefix="/__probe", tags=["fixture-probes"])

    @router.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "fixture": fixture.name,
            "namespace": orchestrator.namespace,
            "runtime_mode": orchestrator.runtime_mode,
            "agents": sorted(orchestrator.agents_by_name.keys()),
            "shutdown_requested": orchestrator.shutdown_event.is_set(),
        }

    @router.get("/tasks/{task_id}")
    async def task_snapshot(task_id: str) -> dict[str, Any]:
        try:
            snapshot = await orchestrator.snapshot_task(task_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True, "task": _serialize(snapshot)}

    @router.get("/tasks/{task_id}/result")
    async def task_result(task_id: str) -> dict[str, Any]:
        try:
            result = await orchestrator.task_result(task_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True, "result": _serialize(result)}

    @router.post("/tasks/{task_id}/wake")
    async def wake_task(
        task_id: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            woke = await orchestrator.wake_task(
                task_id=task_id,
                input=payload.get("input") if isinstance(payload, dict) else None,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, "task_id": task_id, "woke": woke}

    @router.get("/events/{owner_id}")
    async def owner_events(
        owner_id: str,
        after: int = 0,
        limit: int = 100,
        timeout_s: float = 0.0,
    ) -> dict[str, Any]:
        if timeout_s > 0:
            events, next_cursor, first_cursor = await event_journal.wait_after(
                owner_id=owner_id,
                after=after,
                limit=limit,
                timeout_s=timeout_s,
            )
        else:
            events, next_cursor, first_cursor = await event_journal.get_after(
                owner_id=owner_id,
                after=after,
                limit=limit,
            )

        truncated = first_cursor is not None and after < (first_cursor - 1)
        return {
            "ok": True,
            "events": [
                {"cursor": entry.cursor, "payload": entry.payload} for entry in events
            ],
            "next_cursor": next_cursor,
            "first_cursor": first_cursor,
            "truncated": truncated,
        }

    @router.get("/batches/{batch_id}")
    async def batch_snapshot(batch_id: str) -> dict[str, Any]:
        try:
            batch = await orchestrator.snapshot_batch(batch_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True, "batch": _serialize(batch)}

    @router.post("/hooks/{hook_id}/token")
    async def issue_hook_token(hook_id: str) -> dict[str, Any]:
        try:
            token = await orchestrator.rotate_hook_token(
                hook_id=hook_id,
                revoke_previous=False,
            )
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True, "token": token}

    return router


def _attach_fixture_lifespan(
    app: FastAPI,
    *,
    orchestrator: Any,
    event_journal: ProbeEventJournal,
    manage_workers: bool,
) -> None:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        stop_event = asyncio.Event()
        probe_event_task = asyncio.create_task(
            _collect_probe_events(
                orchestrator=orchestrator,
                event_journal=event_journal,
                stop_event=stop_event,
            ),
            name=f"{orchestrator.namespace}-probe-events",
        )
        worker_task: asyncio.Task[Any] | None = None
        if manage_workers:
            worker_task = asyncio.create_task(
                orchestrator.start_workers(orchestrator.shutdown_event),
                name=f"{orchestrator.namespace}-fixture-workers",
            )
        try:
            await asyncio.sleep(0)
            yield
        finally:
            stop_event.set()
            try:
                await asyncio.wait_for(probe_event_task, timeout=5.0)
            except asyncio.TimeoutError:
                probe_event_task.cancel()
                await asyncio.gather(probe_event_task, return_exceptions=True)

            if worker_task is not None:
                orchestrator.shutdown_event.set()
                try:
                    await asyncio.wait_for(worker_task, timeout=15.0)
                except asyncio.TimeoutError:
                    worker_task.cancel()
                    await asyncio.gather(worker_task, return_exceptions=True)

    app.router.lifespan_context = lifespan


def build_fixture_app(
    fixture: FixtureBundle | str | Path,
    *,
    orchestrator: Any | None = None,
    namespace: str | None = None,
    redis_pool: Any = None,
    manage_workers: bool = True,
) -> FastAPI:
    bundle = (
        fixture
        if isinstance(fixture, FixtureBundle)
        else load_fixture_bundle(Path(fixture))
    )
    resolved_namespace = _default_fixture_namespace(bundle, namespace=namespace)
    resolved_orchestrator = orchestrator or resolve_orchestrator(
        bundle,
        redis_pool=redis_pool,
        namespace=resolved_namespace,
    )
    event_journal = ProbeEventJournal()

    app = resolved_orchestrator.create_app(enable_ws=True, cors_origins=["*"])
    app.include_router(
        build_probe_router(
            fixture=bundle,
            orchestrator=resolved_orchestrator,
            event_journal=event_journal,
        )
    )
    app.state.fixture_name = bundle.name
    app.state.fixture_path = str(bundle.path)
    app.state.orchestrator = resolved_orchestrator
    app.state.probe_event_journal = event_journal
    _attach_fixture_lifespan(
        app,
        orchestrator=resolved_orchestrator,
        event_journal=event_journal,
        manage_workers=manage_workers,
    )

    return app
