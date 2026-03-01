import importlib
import json
from collections.abc import Callable
from typing import Any

from agent import DemoContext, parent_agent
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from orchestrator import orchestrator
from pydantic import BaseModel
from starlette.requests import Request

app = FastAPI(root_path="/api")

app.add_middleware(
    CORSMiddleware,  # type: ignore[arg-type]
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _set_vercel_headers(headers: Any) -> None:
    try:
        module = importlib.import_module("vercel.headers")
    except Exception:
        return
    set_headers = getattr(module, "set_headers", None)
    if callable(set_headers):
        set_headers(headers)


@app.middleware("http")
async def vercel_context_middleware(request: Request, call_next: Callable):
    _set_vercel_headers(request.headers)
    return await call_next(request)


@app.get("/events/{user_id}")
@app.get("/api/events/{user_id}")
async def stream_updates(user_id: str):
    async def event_stream():
        async for update in orchestrator.subscribe_to_updates(owner_id=user_id):
            yield f"data: {json.dumps(update)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/")
def read_root() -> dict[str, str]:
    return {"status": "ok", "name": "group-chat-agent-demo"}


class EnqueueRequest(BaseModel):
    user_id: str
    message_history: list[dict[str, str]]
    query: str


class CancelRequest(BaseModel):
    user_id: str
    task_id: str


class SteerRequest(BaseModel):
    user_id: str
    task_id: str
    messages: list[dict[str, str]]


@app.post("/enqueue")
@app.post("/api/enqueue")
async def enqueue(request: EnqueueRequest):
    payload = DemoContext(
        messages=request.message_history,
        query=request.query,
        role_name="parent",
        phase="init",
    )
    task = await orchestrator.create_agent_task(
        agent=parent_agent,
        owner_id=request.user_id,
        payload=payload,
    )
    return {"task_id": task.id}


@app.post("/steer")
@app.post("/api/steer")
async def steer_task_endpoint(request: SteerRequest) -> dict[str, Any]:
    try:
        await orchestrator.steer_task(
            task_id=request.task_id,
            messages=request.messages,
        )
        return {
            "success": True,
            "message": f"Steering messages sent for task {request.task_id}",
        }
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to steer task {request.task_id}: {str(exc)}",
        ) from exc


@app.post("/cancel")
@app.post("/api/cancel")
async def cancel_task_endpoint(request: CancelRequest) -> dict[str, Any]:
    try:
        await orchestrator.cancel_task(task_id=request.task_id)
        return {
            "success": True,
            "message": f"Task {request.task_id} marked for cancellation",
        }
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel task {request.task_id}: {str(exc)}",
        ) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
