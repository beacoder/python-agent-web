"""FastAPI application factory.

Wires routers, CORS, static UI, and the controller singleton.  Run
with ``uvicorn app.main:app`` — the minimal chat UI is served at ``/``.
"""

from __future__ import annotations

import warnings
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .controller.manager import get_controller
from .core.config import get_settings
from .db import get_engine
from .routes import auth, billing, conversations, secrets


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_engine()  # create schema eagerly
    get_controller().reap_idle_sandboxes()
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    if settings.secret_key == "dev-only-insecure-key-change-me":
        warnings.warn(
            "PAW_SECRET_KEY is unset — using the insecure dev default; "
            "set it before any real deployment.",
            stacklevel=1,
        )
    app = FastAPI(title="python-agent-web", version="0.1.0", lifespan=lifespan)
    app.include_router(auth.router)
    app.include_router(conversations.router)
    app.include_router(billing.router)
    app.include_router(secrets.router)

    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True, "runner": settings.runner}

    static_dir = Path(__file__).parent / "static"
    if static_dir.is_dir():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="ui")
    return app


app = create_app()
