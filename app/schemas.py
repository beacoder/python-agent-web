"""Pydantic request/response schemas."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, EmailStr, Field

# --- auth ---


class RegisterIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshIn(BaseModel):
    refresh_token: str


class UserOut(BaseModel):
    id: str
    email: str
    is_active: bool
    is_admin: bool
    created_at: datetime


# --- conversations ---


class ConversationCreate(BaseModel):
    title: str = Field(default="New conversation", max_length=255)


class ConversationOut(BaseModel):
    id: str
    title: str
    created_at: datetime
    updated_at: datetime


class ConversationDetail(ConversationOut):
    runs: list[RunOut] = []


# --- runs ---


class RunCreate(BaseModel):
    prompt: str = Field(min_length=1)


class RunOut(BaseModel):
    id: str
    status: str
    prompt: str
    answer: str
    error: str
    cancelled: bool
    exit_code: int | None
    duration_ms: int
    created_at: datetime
    finished_at: datetime | None


class RunEvent(BaseModel):
    """One harness JSON line, relayed verbatim (plus lifecycle events)."""

    type: str
    seq: int | None = None
    run_id: str | None = None
    data: dict


# --- billing ---


class UsageSummary(BaseModel):
    input_tokens: int
    output_tokens: int
    rounds: int
    runs: int
    by_conversation: dict[str, int]


# --- secrets ---


class SecretIn(BaseModel):
    name: str = Field(min_length=1, max_length=120, pattern=r"^[A-Za-z0-9_.-]+$")
    value: str = Field(min_length=1)


class SecretOut(BaseModel):
    name: str
    created_at: datetime
