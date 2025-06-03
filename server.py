import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
from redis.asyncio.client import Redis as RedisType, PubSub
import os
from pydantic import BaseModel
from datetime import datetime
from typing import Any
import uuid
from contextlib import asynccontextmanager
from starlette.websockets import WebSocket, WebSocketDisconnect

from agent.context import Task, AgentContext
from agent.queue import enqueue_task, cancel_task, steer_task
from agent.agent import DummyAgent, MultiAgent, FreeAgent
from example_agents.video_gen_agent import VideoGenAgent
from agent.logging import get_logger

logger = get_logger("server")

UPDATE_CHANNEL = "updates:{owner_id}"

redis_client: RedisType


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = redis.StrictRedis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        db=0,
        decode_responses=True,
    )

    try:
        await redis_client.ping()  # type: ignore
        logger.info("Connected to Redis successfully")
    except redis.ConnectionError:
        logger.error("Failed to connect to Redis")
        raise

    yield

    if redis_client:
        await redis_client.close()
        logger.info("Redis connection closed")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

WS_REDIS_SUB_TIMEOUT = 5.0  # seconds


@app.websocket("/ws/{user_id}")
async def websocket_updates(websocket: WebSocket, user_id: str):
    await websocket.accept()
    pubsub: PubSub = redis_client.pubsub()  # type: ignore
    channel = UPDATE_CHANNEL.format(owner_id=user_id)
    logger.info(
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
                await websocket.send_text(msg["data"])
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for user_id={user_id}")
    finally:
        await pubsub.unsubscribe(channel)  # type: ignore
        await pubsub.aclose()
        logger.info(
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
    agent = DummyAgent()
    # agent = MultiAgent()
    # agent = FreeAgent()
    # agent = VideoGenAgent()

    task = agent.create_task(
        owner_id=request.user_id,
        payload=AgentContext(
            messages=request.message_history,
            query=request.query,
            turn=0,
        ),
    )

    await enqueue_task(redis_client=redis_client, task=task, agent=agent)
    return {"task_id": task.id}


@app.post("/api/steer")
async def steer_task_endpoint(request: SteerRequest):
    try:
        await steer_task(
            redis_client=redis_client,
            task_id=request.task_id,
            messages=request.messages,
        )

        logger.info(
            f"Steering messages sent for task {request.task_id} by user {request.user_id}"
        )
        return {
            "success": True,
            "message": f"Steering messages sent for task {request.task_id}",
        }
    except Exception as e:
        logger.error(f"Failed to steer task {request.task_id}: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/cancel")
async def cancel_task_endpoint(request: CancelRequest):
    try:
        await cancel_task(
            redis_client=redis_client,
            task_id=request.task_id,
        )

        logger.info(
            f"Task {request.task_id} marked for cancellation by user {request.user_id}"
        )
        return {
            "success": True,
            "message": f"Task {request.task_id} marked for cancellation",
        }
    except Exception as e:
        logger.error(f"Failed to cancel task {request.task_id}: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
