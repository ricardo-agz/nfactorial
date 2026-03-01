from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class EnqueueRequest(BaseModel):
    agent_name: str
    owner_id: str
    payload: dict[str, Any]
    idempotency_key: str | None = None


class EnqueueResponse(BaseModel):
    task_id: str


class SteerRequest(BaseModel):
    messages: list[dict[str, Any]]


class ResumeRequest(BaseModel):
    messages: list[dict[str, Any]]
    idempotency_key: str | None = None


class ResolveHookRequest(BaseModel):
    token: str
    payload: Any
    idempotency_key: str | None = None


class MessageTaskRequest(BaseModel):
    owner_id: str
    content: str
    metadata: dict[str, Any] | None = None


class MessageGroupRequest(BaseModel):
    owner_id: str
    content: str
    group_id: str | None = None
    group_name: str | None = None
    task_id: str | None = None
    team_id: str | None = None
    metadata: dict[str, Any] | None = None
