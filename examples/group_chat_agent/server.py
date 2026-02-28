from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as redis
from agent import DemoContext, parent_agent
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from orchestrator import orchestrator
from pydantic import BaseModel
from redis.asyncio.client import PubSub, Redis as RedisType
from starlette.websockets import WebSocket, WebSocketDisconnect

WS_REDIS_SUB_TIMEOUT = 5.0

redis_client: RedisType


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global redis_client
    redis_client = await orchestrator.get_redis_client()

    try:
        await redis_client.ping()  # type: ignore[misc]
        print("Connected to Redis successfully")
    except redis.ConnectionError:
        print("Failed to connect to Redis")
        raise

    yield

    if redis_client:
        await redis_client.close()
        print("Redis connection closed")


app = FastAPI(lifespan=lifespan)

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
    pubsub: PubSub = redis_client.pubsub()  # type: ignore[misc]
    channel = orchestrator.get_updates_channel(owner_id=user_id)
    await pubsub.subscribe(channel)  # type: ignore[misc]

    try:
        while True:
            msg: dict[str, Any] | None = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=WS_REDIS_SUB_TIMEOUT,
            )
            if msg and msg["type"] == "message":
                data = msg["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                await websocket.send_text(data)
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for user_id={user_id}")
    finally:
        await pubsub.unsubscribe(channel)  # type: ignore[misc]
        await pubsub.aclose()


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
