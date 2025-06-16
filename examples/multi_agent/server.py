from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
from redis.asyncio.client import Redis as RedisType, PubSub
from pydantic import BaseModel
from typing import Any
from contextlib import asynccontextmanager
from starlette.websockets import WebSocket, WebSocketDisconnect
from factorial import AgentContext

from agent import basic_agent, orchestrator


WS_REDIS_SUB_TIMEOUT = 5.0  # seconds


redis_client: RedisType


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    # Use the shared control plane's Redis connection
    redis_client = await orchestrator.get_redis_client()

    try:
        await redis_client.ping()  # type: ignore
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
    pubsub: PubSub = redis_client.pubsub()  # type: ignore
    channel = orchestrator.get_updates_channel(owner_id=user_id)
    print(
        f"WebSocket connection established for user_id={user_id}, subscribing to channel={channel}"
    )
    await pubsub.subscribe(channel)  # type: ignore

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
        await pubsub.unsubscribe(channel)  # type: ignore
        await pubsub.aclose()
        print(
            f"WebSocket cleanup completed for user_id={user_id}, unsubscribed from channel={channel}"
        )


@app.get("/")
def read_root():
    return {"Hello": "World"}


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
    payload = AgentContext(
        messages=request.message_history,
        query=request.query,
        turn=0,
    )
    task = await orchestrator.create_agent_task(
        agent=basic_agent,
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

        print(
            f"Steering messages sent for task {request.task_id} by user {request.user_id}"
        )
        return {
            "success": True,
            "message": f"Steering messages sent for task {request.task_id}",
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to steer task {request.task_id}: {str(e)}",
        )


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
        import traceback
        print(f"Error cancelling task {request.task_id}: {str(e)}", traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel task {request.task_id}: {str(e)}",
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
