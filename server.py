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

from agent.context import AgentContext
from orchestrator import control_plane, agent as default_agent
from agent.logging import get_logger

logger = get_logger("server")

UPDATE_CHANNEL = "updates:{owner_id}"

redis_client: RedisType


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    # Use the shared control plane's Redis connection
    redis_client = await control_plane.get_redis_client()

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
                data = msg["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                await websocket.send_text(data)
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
    # Use the default agent from shared manager
    # You could also look up agents by name: control_plane.get_agent("agent_name")
    agent = default_agent

    task = agent.create_task(
        owner_id=request.user_id,
        payload=AgentContext(
            messages=request.message_history,
            query=request.query,
            turn=0,
        ),
    )

    # Use the control plane's enqueue method with proper configuration
    await control_plane.enqueue_task(agent=agent, task=task)
    return {"task_id": task.id}


@app.post("/api/steer")
async def steer_task_endpoint(request: SteerRequest):
    try:
        # Use the control plane's steer method
        await control_plane.steer_task(
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
        # Use the control plane's cancel method that automatically determines the agent
        await control_plane.cancel_task(task_id=request.task_id)

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


@app.get("/api/task/{task_id}/status")
async def get_task_status_endpoint(task_id: str):
    """Get the status of a specific task"""
    try:
        status = await control_plane.get_task_status(task_id)
        return {"task_id": task_id, "status": status}
    except Exception as e:
        logger.error(f"Failed to get status for task {task_id}: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/task/{task_id}")
async def get_task_data_endpoint(task_id: str):
    """Get the full data of a specific task"""
    try:
        task_data = await control_plane.get_task_data(task_id)
        if not task_data:
            return {"success": False, "error": "Task not found"}
        return {"task_id": task_id, "data": task_data}
    except Exception as e:
        logger.error(f"Failed to get data for task {task_id}: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/agents")
async def get_agents_endpoint():
    """Get information about registered agents"""
    try:
        agents_info = []
        for agent_name, agent in control_plane._agents_by_name.items():
            metrics_config = control_plane.get_metrics_config(agent_name)
            agents_info.append(
                {
                    "name": agent_name,
                    "class": agent.__class__.__name__,
                    "metrics_config": {
                        "bucket_duration": metrics_config.bucket_duration
                        if metrics_config
                        else None,
                        "retention_duration": metrics_config.retention_duration
                        if metrics_config
                        else None,
                        "timeline_duration": metrics_config.timeline_duration
                        if metrics_config
                        else None,
                    }
                    if metrics_config
                    else None,
                }
            )
        return {"agents": agents_info}
    except Exception as e:
        logger.error(f"Failed to get agents info: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
