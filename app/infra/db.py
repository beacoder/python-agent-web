"""Database engine/session and schema management.

Schema is owned by Alembic migrations (``migrations/``), applied with
``upgrade_to_head`` on startup and in tests — so dev, test, and prod all
build the schema the same way.  SQLite is the default; Postgres is the
prod target, and nothing here is SQLite-specific.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import get_settings

# repo root holds alembic.ini + migrations/ (app/infra/db.py -> parents[2])
_REPO_ROOT = Path(__file__).resolve().parents[2]


def make_engine(db_url: str | None = None) -> Engine:
    url = db_url or get_settings().db_url
    kwargs: dict[str, Any] = {}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    engine = create_engine(url, **kwargs)
    if url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection: Any, connection_record: Any) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


def upgrade_to_head() -> None:
    """Bring the configured database up to the latest migration.

    Idempotent: Alembic applies only revisions not yet recorded in the
    ``alembic_version`` table.  The migration URL comes from settings
    (see ``migrations/env.py``), so this always targets the app's DB.
    """
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    command.upgrade(cfg, "head")


_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = make_engine()
        upgrade_to_head()
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _session_factory


def get_db() -> Iterator[Session]:
    """FastAPI dependency yielding a request-scoped session.

    The commit happens BEFORE the response is returned: since
    FastAPI 0.106, teardown of yield-dependencies runs after the
    response has been sent, so committing in teardown would let a
    client that fires a follow-up request immediately (register ->
    login) read against the pre-commit snapshot and fail.
    """
    factory = get_session_factory()
    db = factory()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def commit_now(db: Session) -> None:
    """Commit immediately for mutating routes, so the write is durable
    before the response is sent (see get_db docstring). Idempotent: a
    later commit of a clean session is a no-op."""
    db.commit()


def reset_engine_for_tests(db_url: str) -> Engine:
    """Point the module-level singletons at a fresh (test) database and
    migrate it to head — the same schema path the app uses."""
    global _engine, _session_factory
    _engine = make_engine(db_url)
    upgrade_to_head()
    _session_factory = sessionmaker(bind=_engine, expire_on_commit=False)
    return _engine
