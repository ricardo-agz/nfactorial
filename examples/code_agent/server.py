import json
from typing import Any

from agent import IdeAgentContext, ide_agent, orchestrator
from fastapi import FastAPI
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.websockets import WebSocket, WebSocketDisconnect

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.websocket("/ws/{user_id}")
async def websocket_updates(websocket: WebSocket, user_id: str):
    await websocket.accept()

    try:
        async for update in orchestrator.subscribe_to_updates(owner_id=user_id):
            await websocket.send_text(json.dumps(update))
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for user_id={user_id}")


@app.get("/")
def read_root():
    return {"Hello": "IDE Agent"}


class EnqueueRequest(BaseModel):
    user_id: str
    message_history: list[dict[str, str]]
    query: str
    code: str


class CancelRequest(BaseModel):
    user_id: str
    task_id: str


@app.post("/api/enqueue")
async def enqueue(request: EnqueueRequest):
    payload = IdeAgentContext(
        messages=request.message_history,
        query=request.query,
        turn=0,
        code=request.code,
    )

    task = await orchestrator.create_agent_task(
        agent=ide_agent,
        owner_id=request.user_id,
        payload=payload,
    )

    return {"task_id": task.id}


@app.post("/api/cancel")
async def cancel_task_endpoint(request: CancelRequest) -> dict[str, Any]:
    try:
        await orchestrator.cancel_task(task_id=request.task_id)

        print(
            f"Task {request.task_id} marked for cancellation by user {request.user_id}"
        )
        return {
            "success": True,
            "message": f"Task {request.task_id} marked for cancellation",
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel task {request.task_id}: {str(e)}",
        ) from e


class ResolveHookRequest(BaseModel):
    hook_id: str
    token: str
    approved: bool
    idempotency_key: str | None = None


@app.post("/api/resolve_hook")
async def resolve_hook_endpoint(request: ResolveHookRequest):
    """Resolve a pending hook with approval payload."""
    try:
        resolution = await orchestrator.resolve_hook(
            hook_id=request.hook_id,
            payload={"approved": request.approved},
            token=request.token,
            idempotency_key=request.idempotency_key,
        )
        return {
            "success": True,
            "status": resolution.status,
            "task_id": resolution.task_id,
            "tool_call_id": resolution.tool_call_id,
            "task_resumed": resolution.task_resumed,
        }
    except Exception as e:
        print(f"Failed to resolve hook {request.hook_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
