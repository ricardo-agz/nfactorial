from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.websockets import WebSocket, WebSocketDisconnect

from factorial.exceptions import (
    MessagingGroupNotFoundError,
    MessagingScopeError,
    TaskNotFoundError,
)

from .models import (
    EnqueueRequest,
    EnqueueResponse,
    MessageGroupRequest,
    MessageTaskRequest,
    ResolveHookRequest,
    ResumeRequest,
    SteerRequest,
)

if TYPE_CHECKING:
    from factorial.orchestrator import Orchestrator


def register_control_plane_routes(
    *,
    app: FastAPI,
    orchestrator: Orchestrator,
    enable_ws: bool = False,
) -> None:
    @app.get("/")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "web",
            "runtime_mode": orchestrator.runtime_mode,
            "namespace": orchestrator.namespace,
        }

    @app.post("/api/enqueue", response_model=EnqueueResponse)
    async def enqueue_task_route(request: EnqueueRequest) -> EnqueueResponse:
        agent = orchestrator.get_agent(request.agent_name)
        if agent is None:
            raise HTTPException(
                status_code=404,
                detail=f"Agent '{request.agent_name}' is not registered",
            )
        try:
            context = cast(Any, agent.context_class).from_dict(request.payload)
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid payload for agent '{request.agent_name}': {exc}",
            ) from exc

        task = await orchestrator.create_agent_task(
            agent=agent,
            payload=context,
            owner_id=request.owner_id,
            idempotency_key=request.idempotency_key,
        )
        return EnqueueResponse(task_id=task.id)

    @app.get("/api/tasks/{task_id}")
    async def get_task_route(task_id: str) -> dict[str, Any]:
        task_data = await orchestrator.get_task_data(task_id)
        if task_data is None:
            raise HTTPException(status_code=404, detail="Task not found")
        return task_data

    @app.post("/api/tasks/{task_id}/cancel")
    async def cancel_task_route(task_id: str) -> dict[str, Any]:
        await orchestrator.cancel_task(task_id=task_id)
        return {"ok": True, "task_id": task_id}

    @app.post("/api/tasks/{task_id}/steer")
    async def steer_task_route(task_id: str, request: SteerRequest) -> dict[str, Any]:
        await orchestrator.steer_task(task_id=task_id, messages=request.messages)
        return {"ok": True, "task_id": task_id}

    @app.post("/api/tasks/{task_id}/message")
    async def message_task_route(
        task_id: str,
        request: MessageTaskRequest,
    ) -> dict[str, Any]:
        try:
            report = await orchestrator.message_task(
                task_id=task_id,
                owner_id=request.owner_id,
                content=request.content,
                metadata=request.metadata,
            )
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Task not found") from exc
        except (MessagingScopeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, **report}

    @app.post("/api/groups/message")
    async def message_group_route(request: MessageGroupRequest) -> dict[str, Any]:
        try:
            report = await orchestrator.message_group(
                owner_id=request.owner_id,
                content=request.content,
                group_id=request.group_id,
                group_name=request.group_name,
                task_id=request.task_id,
                team_id=request.team_id,
                metadata=request.metadata,
            )
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Task not found") from exc
        except MessagingGroupNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (MessagingScopeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, **report}

    @app.post("/api/tasks/{task_id}/resume")
    async def resume_task_route(task_id: str, request: ResumeRequest) -> dict[str, Any]:
        resumed = await orchestrator.resume_task(
            task_id=task_id,
            messages=request.messages,
            idempotency_key=request.idempotency_key,
        )
        return {
            "ok": True,
            "source_task_id": task_id,
            "resumed_task_id": resumed.id,
        }

    @app.post("/api/hooks/{hook_id}/resolve")
    async def resolve_hook_route(
        hook_id: str,
        request: ResolveHookRequest,
    ) -> dict[str, Any]:
        resolution = await orchestrator.resolve_hook(
            hook_id=hook_id,
            payload=request.payload,
            token=request.token,
            idempotency_key=request.idempotency_key,
        )
        return {
            "ok": True,
            "hook_id": resolution.hook_id,
            "task_id": resolution.task_id,
            "tool_call_id": resolution.tool_call_id,
            "status": resolution.status,
            "task_resumed": resolution.task_resumed,
        }

    @app.get("/events/{owner_id}")
    async def stream_events(owner_id: str, request: Request) -> StreamingResponse:
        async def event_stream():
            async for update in orchestrator.subscribe_to_updates(owner_id=owner_id):
                if await request.is_disconnected():
                    break
                event_type = str(update.get("event_type", "update"))
                payload = json.dumps(update, separators=(",", ":"))
                yield f"event: {event_type}\ndata: {payload}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    if enable_ws:

        @app.websocket("/ws/{owner_id}")
        async def websocket_updates(websocket: WebSocket, owner_id: str):
            await websocket.accept()
            try:
                async for update in orchestrator.subscribe_to_updates(
                    owner_id=owner_id
                ):
                    await websocket.send_text(json.dumps(update))
            except WebSocketDisconnect:
                return
