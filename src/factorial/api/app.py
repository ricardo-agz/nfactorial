from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes import register_control_plane_routes

if TYPE_CHECKING:
    from factorial.orchestrator import Orchestrator


def create_control_plane_app(
    orchestrator: Orchestrator,
    *,
    enable_ws: bool = False,
    cors_origins: list[str] | None = None,
) -> FastAPI:
    app = FastAPI(title="factorial-api")
    if cors_origins is not None:
        app.add_middleware(
            cast(Any, CORSMiddleware),
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    register_control_plane_routes(
        app=app,
        orchestrator=orchestrator,
        enable_ws=enable_ws,
    )
    return app
