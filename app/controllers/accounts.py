"""Account business logic: registration, credentials, token identity.

The routes keep only FastAPI's dependency plumbing; deciding whether a
credential or token grants access is decided here.
"""

from __future__ import annotations

import jwt as pyjwt
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..infra.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from ..models import User, new_id
from ..models.db import commit_now
from .errors import Conflict, Forbidden, Unauthorized


def token_pair(user: User) -> tuple[str, str]:
    """Fresh (access, refresh) pair for a user."""
    return create_access_token(user.id), create_refresh_token(user.id)


def register(db: Session, email: str, password: str) -> User:
    """Create an account; the email must be unused."""
    if db.scalar(select(User).where(User.email == email)) is not None:
        raise Conflict("email already registered")
    user = User(id=new_id("usr"), email=email, password_hash=hash_password(password))
    db.add(user)
    db.flush()
    commit_now(db)
    return user


def login(db: Session, email: str, password: str) -> User:
    """Authenticate a password.

    A wrong email and a wrong password give the same 401, so neither
    can be used to enumerate accounts; a disabled account is told
    apart (403) only once the password checked out.
    """
    user = db.scalar(select(User).where(User.email == email))
    if user is None or not verify_password(password, user.password_hash):
        raise Unauthorized("invalid email or password")
    if not user.is_active:
        raise Forbidden("account disabled")
    return user


def _user_of_token(db: Session, token: str, *, expected_type: str) -> User:
    try:
        payload = decode_token(token, expected_type=expected_type)
    except pyjwt.PyJWTError as exc:
        raise Unauthorized(f"invalid token: {exc}") from exc
    user = db.get(User, payload["sub"])
    if user is None or not user.is_active:
        raise Unauthorized("unknown or inactive user")
    return user


def user_for_access_token(db: Session, token: str | None) -> User:
    """Identify the caller from a bearer access token."""
    if not token:
        raise Unauthorized("missing bearer token")
    return _user_of_token(db, token, expected_type="access")


def user_for_refresh_token(db: Session, token: str) -> User:
    """Identify the holder of a refresh token."""
    return _user_of_token(db, token, expected_type="refresh")


def require_admin(user: User) -> User:
    if not user.is_admin:
        raise Forbidden("admin required")
    return user
