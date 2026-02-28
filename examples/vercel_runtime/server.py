from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from examples.vercel_runtime.orchestrator import orchestrator
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse

ORCHESTRATOR_MOUNT_PATH = "/orchestrator"


app = FastAPI(title="factorial-vercel-runtime-example")
app.mount(ORCHESTRATOR_MOUNT_PATH, orchestrator.create_app())


_CHAT_HTML = Path(__file__).with_name("chat.html").read_text(encoding="utf-8")


@app.get("/chat")
async def chat_ui() -> HTMLResponse:
    return HTMLResponse(_CHAT_HTML)


@app.get("/chat/events/{owner_id}")
async def stream_chat_events(owner_id: str, request: Request) -> StreamingResponse:
    async def event_stream():
        async for update in orchestrator.subscribe_to_updates(owner_id=owner_id):
            if await request.is_disconnected():
                break
            payload = json.dumps(update, separators=(",", ":"))
            yield f"data: {payload}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/chat/config")
async def chat_config() -> dict[str, Any]:
    return {
        "default_agent": "assistant_agent",
        "api_prefix": ORCHESTRATOR_MOUNT_PATH,
    }
