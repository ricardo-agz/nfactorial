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
from agent.queue import enqueue_task
from agent.multi_agent import MultiAgent
from agent.logging import get_logger

logger = get_logger("server")

UPDATE_CHANNEL = "updates:{user_id}"

redis_client: RedisType


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = redis.StrictRedis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        db=0,
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
    channel = UPDATE_CHANNEL.format(user_id=user_id)
    await pubsub.subscribe(channel)  # type: ignore

    try:
        while True:
            msg: dict[str, Any] | None = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=WS_REDIS_SUB_TIMEOUT,
            )
            if msg and msg["type"] == "message":
                await websocket.send_text(msg["data"].decode())
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(channel)  # type: ignore
        await pubsub.close()


@app.get("/")
def read_root():
    return {"Hello": "World"}


class EnqueueRequest(BaseModel):
    user_id: str
    message_history: list[dict[str, str]]
    query: str


@app.post("/api/enqueue")
async def enqueue(request: EnqueueRequest):
    task = Task(
        id=str(uuid.uuid4()),
        owner_id=request.user_id,
        retries=0,
        created_at=datetime.utcnow(),
        payload=AgentContext(
            messages=request.message_history,
            query=request.query,
            turn=0,
        ),
    )

    agent = MultiAgent()

    await enqueue_task(redis_client=redis_client, task=task, agent=agent)
    return {"task_id": task.id}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
