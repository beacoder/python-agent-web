"""SQLAlchemy ORM models: users, conversations, runs, usage, secrets."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    conversations: Mapped[list[Conversation]] = relationship(back_populates="user")
    usage_events: Mapped[list[UsageEvent]] = relationship(back_populates="user")


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    title: Mapped[str] = mapped_column(String(255), default="New conversation")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    user: Mapped[User] = relationship(back_populates="conversations")
    runs: Mapped[list[Run]] = relationship(
        back_populates="conversation",
        order_by="Run.created_at",
        cascade="all, delete-orphan",
    )


class Run(Base):
    """One harness exec: a single prompt through the agent.

    ``status``: queued -> running -> done | error | cancelled
    ``events`` holds every harness JSON line (result last) so the
    conversation can be replayed; ``answer`` is the final text.
    """

    __tablename__ = "runs"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    conversation_id: Mapped[str] = mapped_column(ForeignKey("conversations.id"), index=True)
    prompt: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), default="queued", index=True)
    answer: Mapped[str] = mapped_column(Text, default="")
    events: Mapped[list] = mapped_column(JSON, default=list)
    error: Mapped[str] = mapped_column(Text, default="")
    harness_run_id: Mapped[str] = mapped_column(String(64), default="")
    exit_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cancelled: Mapped[bool] = mapped_column(Boolean, default=False)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    conversation: Mapped[Conversation] = relationship(back_populates="runs")


class UsageEvent(Base):
    """Token ledger row, attributed per user/conversation/run."""

    __tablename__ = "usage_events"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    conversation_id: Mapped[str] = mapped_column(String(40), index=True)
    run_id: Mapped[str] = mapped_column(String(40), index=True)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    rounds: Mapped[int] = mapped_column(Integer, default=0)
    model: Mapped[str] = mapped_column(String(120), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    user: Mapped[User] = relationship(back_populates="usage_events")


class Secret(Base):
    """User secrets, Fernet-encrypted at rest; values never leave the
    API as plaintext (write-only from the client's perspective)."""

    __tablename__ = "secrets"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    name: Mapped[str] = mapped_column(String(120))
    value_encrypted: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Sandbox(Base):
    """Sandbox registry: one row per live (or recently live) sandbox.

    ``runner_id`` names the backend that owns it (``server`` today);
    ``last_used_at`` drives the idle reaper.
    """

    __tablename__ = "sandboxes"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    runner_id: Mapped[str] = mapped_column(String(32), default="server")
    user_id: Mapped[str] = mapped_column(String(40), index=True)
    conversation_id: Mapped[str] = mapped_column(String(40), default="")
    spec: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(16), default="running", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_used_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


__all__ = [
    "Base",
    "Conversation",
    "Run",
    "Sandbox",
    "Secret",
    "UsageEvent",
    "User",
    "new_id",
    "utcnow",
]
