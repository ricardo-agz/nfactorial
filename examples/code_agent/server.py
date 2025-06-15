from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException
from pydantic import BaseModel
import json
from typing import Any
from starlette.websockets import WebSocket, WebSocketDisconnect

from agent import ide_agent, orchestrator, IdeAgentContext


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
        )


class CompleteToolRequest(BaseModel):
    user_id: str
    task_id: str
    tool_call_id: str
    result: str


@app.post("/api/complete_tool")
async def complete_deferred_tool_endpoint(request: CompleteToolRequest):
    """Complete a deferred tool call with the provided result."""
    try:
        success = await orchestrator.complete_deferred_tool(
            task_id=request.task_id,
            tool_call_id=request.tool_call_id,
            result=request.result,
        )

        if success:
            return {"success": True}
        raise HTTPException(status_code=500, detail="Unable to complete deferred tool.")
    except Exception as e:
        print(f"Failed to complete deferred tool call {request.tool_call_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
