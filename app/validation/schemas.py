"""Pydantic request/response schemas."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator

# --- auth ---


class RegisterIn(BaseModel):
    # Deliberately a plain validated string, not EmailStr: email-validator
    # rejects single-label domains ("user@localhost", common in dev) and
    # reserved names outright, which made login/register 422 in practice.
    email: str = Field(min_length=3, max_length=254)
    password: str = Field(min_length=8, max_length=128)

    @field_validator("email")
    @classmethod
    def _normalize_email(cls, v: str) -> str:
        v = v.strip().lower()
        if v.count("@") != 1 or not v.split("@")[0]:
            raise ValueError("must contain exactly one @ with a local part")
        local, domain = v.split("@")
        if not domain or " " in v or any(c in v for c in "\"'<>,;:\\"):
            raise ValueError("invalid email address")
        return v


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshIn(BaseModel):
    refresh_token: str


class PasswordChange(BaseModel):
    old_password: str = Field(min_length=1, max_length=128)
    new_password: str = Field(min_length=8, max_length=128)


class UserOut(BaseModel):
    id: str
    email: str
    is_active: bool
    is_admin: bool
    created_at: datetime


class AccountProfile(BaseModel):
    """Everything the account dropdown and 个人中心 page need, in one read.

    ``points`` is the sum of ``plan_points`` and ``pack_points`` (never
    stored separately); ``points_expire_at`` is null for 无有效期.
    ``works`` is the user's conversation count (the app's "创作").
    ``name`` has no column yet — it is derived from the email.
    """

    name: str
    email: str
    plan: str
    phone: str | None
    points: int
    plan_points: int
    pack_points: int
    points_expire_at: datetime | None
    works: int


# --- conversations ---


class ConversationCreate(BaseModel):
    title: str = Field(default="New conversation", max_length=255)


class ConversationOut(BaseModel):
    id: str
    title: str
    created_at: datetime
    updated_at: datetime


class FileOut(BaseModel):
    id: str
    filename: str
    size: int
    created_at: datetime


class ArtifactOut(BaseModel):
    """A file the agent produced in its workspace (downloadable)."""

    name: str
    size: int
    modified_at: datetime


class ConversationDetail(ConversationOut):
    runs: list[RunOut] = []
    files: list[FileOut] = []


# --- runs ---


class RunCreate(BaseModel):
    prompt: str = Field(min_length=1)


class RunAnswer(BaseModel):
    """A user's reply to a pending mid-run question (resident mode)."""

    answers: list[str] = Field(min_length=1)


class RunEvent(BaseModel):
    """One harness JSON line, relayed verbatim (plus lifecycle events)."""

    type: str
    seq: int | None = None
    run_id: str | None = None
    data: dict


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
    events: list[RunEvent] = []


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
