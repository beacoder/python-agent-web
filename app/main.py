"""FastAPI application factory.

Wires routers, CORS, static UI, and the controller singleton.  Run
with ``uvicorn app.main:app`` — the minimal chat UI is served at ``/``.
"""

from __future__ import annotations

import time
import uuid
import warnings
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .controllers.errors import DomainError
from .controllers.manager import get_controller
from .infra.config import get_settings
from .infra.db import get_engine
from .infra.logging import configure_logging, get_logger, reset_request_id, set_request_id
from .routes import account, auth, billing, conversations, secrets

_log = get_logger("request")


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_engine()  # migrate schema to head
    controller = get_controller()
    controller.reconcile_orphaned_runs()  # fail runs stranded by a restart
    controller.reap_idle_sandboxes()
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    if settings.secret_key == "dev-only-insecure-key-change-me":
        warnings.warn(
            "PAW_SECRET_KEY is unset — using the insecure dev default; "
            "set it before any real deployment.",
            stacklevel=1,
        )
    app = FastAPI(title="python-agent-web", version="0.1.0", lifespan=lifespan)

    @app.middleware("http")
    async def _request_context(request: Request, call_next):
        """Bind a request id to logs, time the request, and capture any
        unhandled error as a logged 500 that carries the id back to the
        client (so a user can quote it in a bug report)."""
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        token = set_request_id(rid)
        start = time.perf_counter()
        try:
            try:
                response = await call_next(request)
            except Exception:
                _log.exception("%s %s unhandled error", request.method, request.url.path)
                response = JSONResponse(
                    {"detail": "internal server error", "request_id": rid},
                    status_code=500,
                )
            dur_ms = (time.perf_counter() - start) * 1000
            response.headers["X-Request-ID"] = rid
            _log.info(
                "%s %s -> %s %.1fms",
                request.method,
                request.url.path,
                response.status_code,
                dur_ms,
            )
            return response
        finally:
            reset_request_id(token)

    @app.exception_handler(DomainError)
    def _domain_error(_request: Request, exc: DomainError) -> JSONResponse:
        """Render a controller's refusal in FastAPI's own error shape.

        With this, routes never translate business rules into status
        codes -- the controller names the failure, the status follows
        from its type.
        """
        headers = {}
        retry_after = getattr(exc, "retry_after", None)
        if retry_after is not None:
            headers["Retry-After"] = str(retry_after)
        return JSONResponse(
            {"detail": exc.detail}, status_code=exc.status_code, headers=headers or None
        )

    app.include_router(auth.router)
    app.include_router(account.router)
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

    static_dir = Path(__file__).parent / "views"
    if static_dir.is_dir():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="ui")
    return app


app = create_app()
