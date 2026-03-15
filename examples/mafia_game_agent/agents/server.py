from __future__ import annotations

import json
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from models import MafiaGameState
from orchestrator import orchestrator
from pydantic import BaseModel

from agents import (
    HUMAN_PLAYER_ID,
    TOWN_GROUP_NAME,
    WOLF_GROUP_NAME,
    mafia_game_master_agent,
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,  # type: ignore[arg-type]
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    return {"status": "ok", "name": "mafia-game-agent-demo"}


class EnqueueRequest(BaseModel):
    user_id: str
    game_name: str = "Mafia in nfactorial"
    include_human: bool = True
    human_name: str = "You"
    human_role_preference: Literal["random", "werewolf", "villager"] = "random"
    ai_player_count: int = 7
    day_discussion_seconds: int = 90
    day_vote_seconds: int = 35
    night_seconds: int = 25


class EnqueueResponse(BaseModel):
    task_id: str
    human_player_id: str | None = None


class CancelRequest(BaseModel):
    user_id: str
    task_id: str


class HumanChatRequest(BaseModel):
    user_id: str
    channel: Literal["town", "wolf"]
    content: str


class HumanVoteRequest(BaseModel):
    user_id: str
    target_player_id: str
    round_no: int | None = None


class HumanNightActionRequest(BaseModel):
    user_id: str
    target_player_id: str
    round_no: int | None = None


class HumanCallVoteRequest(BaseModel):
    user_id: str
    round_no: int | None = None


class GameSessionMeta(BaseModel):
    owner_id: str
    include_human: bool
    human_player_id: str | None = None
    human_display_name: str = "You"


GAME_SESSIONS: dict[str, GameSessionMeta] = {}


def _get_session_or_404(task_id: str) -> GameSessionMeta:
    session = GAME_SESSIONS.get(task_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown game task_id: {task_id}")
    return session


def _ensure_owner_or_403(session: GameSessionMeta, user_id: str) -> None:
    if session.owner_id != user_id:
        raise HTTPException(
            status_code=403,
            detail="This game belongs to a different user session.",
        )


@app.post("/api/enqueue")
async def enqueue(request: EnqueueRequest) -> EnqueueResponse:
    state = MafiaGameState(
        query=request.game_name,
        phase="init",
        game_name=request.game_name,
        include_human=request.include_human,
        human_name=request.human_name,
        human_role_preference=request.human_role_preference,
        ai_player_count=request.ai_player_count,
        day_discussion_seconds=request.day_discussion_seconds,
        day_vote_seconds=request.day_vote_seconds,
        night_seconds=request.night_seconds,
    )
    task = await orchestrator.enqueue(
        mafia_game_master_agent,
        input=request.game_name,
        owner_id=request.user_id,
        state=state,
    )
    human_name = (request.human_name.strip() or "You") if request.include_human else "You"
    GAME_SESSIONS[task.id] = GameSessionMeta(
        owner_id=request.user_id,
        include_human=request.include_human,
        human_player_id=(HUMAN_PLAYER_ID if request.include_human else None),
        human_display_name=human_name,
    )
    return EnqueueResponse(
        task_id=task.id,
        human_player_id=(HUMAN_PLAYER_ID if request.include_human else None),
    )


@app.post("/api/games/{task_id}/chat")
async def human_chat(task_id: str, request: HumanChatRequest) -> dict[str, Any]:
    session = _get_session_or_404(task_id)
    _ensure_owner_or_403(session, request.user_id)
    content = request.content.strip()
    if not content:
        raise HTTPException(status_code=400, detail="content must be non-empty")

    group_name = TOWN_GROUP_NAME if request.channel == "town" else WOLF_GROUP_NAME
    labeled_content = f"{session.human_display_name}: {content}"
    report = await orchestrator.message_group(
        owner_id=request.user_id,
        content=labeled_content,
        data={
            "kind": "human_chat",
            "channel": request.channel,
            "player_id": session.human_player_id,
            "display_name": session.human_display_name,
        },
        group_name=group_name,
        task_id=task_id,
    )
    return {"success": True, "channel": request.channel, **report}


@app.post("/api/games/{task_id}/vote")
async def human_vote(task_id: str, request: HumanVoteRequest) -> dict[str, Any]:
    session = _get_session_or_404(task_id)
    _ensure_owner_or_403(session, request.user_id)
    if not session.include_human:
        raise HTTPException(status_code=400, detail="This game has no human player.")

    target_player_id = request.target_player_id.strip()
    if not target_player_id:
        raise HTTPException(status_code=400, detail="target_player_id is required")

    report = await orchestrator.message_task(
        task_id=task_id,
        owner_id=request.user_id,
        content=f"Human submitted day vote for {target_player_id}.",
        data={
            "kind": "day_vote",
            "voter_id": session.human_player_id,
            "target_player_id": target_player_id,
            "round_no": request.round_no,
        },
    )
    return {"success": True, **report}


@app.post("/api/games/{task_id}/night_action")
async def human_night_action(
    task_id: str,
    request: HumanNightActionRequest,
) -> dict[str, Any]:
    session = _get_session_or_404(task_id)
    _ensure_owner_or_403(session, request.user_id)
    if not session.include_human:
        raise HTTPException(status_code=400, detail="This game has no human player.")

    target_player_id = request.target_player_id.strip()
    if not target_player_id:
        raise HTTPException(status_code=400, detail="target_player_id is required")

    report = await orchestrator.message_task(
        task_id=task_id,
        owner_id=request.user_id,
        content=f"Human submitted night action for {target_player_id}.",
        data={
            "kind": "night_action",
            "voter_id": session.human_player_id,
            "target_player_id": target_player_id,
            "round_no": request.round_no,
        },
    )
    return {"success": True, **report}


@app.post("/api/games/{task_id}/call_vote")
async def human_call_vote(task_id: str, request: HumanCallVoteRequest) -> dict[str, Any]:
    session = _get_session_or_404(task_id)
    _ensure_owner_or_403(session, request.user_id)
    if not session.include_human:
        raise HTTPException(status_code=400, detail="This game has no human player.")

    report = await orchestrator.message_task(
        task_id=task_id,
        owner_id=request.user_id,
        content="Human called for a vote.",
        data={
            "kind": "call_vote",
            "voter_id": session.human_player_id,
            "round_no": request.round_no,
        },
    )
    return {"success": True, **report}


@app.post("/api/cancel")
async def cancel_task_endpoint(request: CancelRequest) -> dict[str, Any]:
    session = _get_session_or_404(request.task_id)
    _ensure_owner_or_403(session, request.user_id)
    try:
        await orchestrator.cancel_task(task_id=request.task_id)
        GAME_SESSIONS.pop(request.task_id, None)
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
