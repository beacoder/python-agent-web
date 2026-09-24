"""Account business logic: registration, credentials, token identity.

The routes keep only FastAPI's dependency plumbing; deciding whether a
credential or token grants access is decided here.
"""

from __future__ import annotations

import jwt as pyjwt
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..infra.db import commit_now
from ..infra.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from ..models import Conversation, User, new_id
from .errors import Conflict, Forbidden, Unauthorized


def token_pair(user: User) -> tuple[str, str]:
    """Fresh (access, refresh) pair for a user, stamped with the user's
    current token version so a later revocation invalidates them."""
    return (
        create_access_token(user.id, user.token_version),
        create_refresh_token(user.id, user.token_version),
    )


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
    # Tokens minted before the last revocation (logout / password change)
    # carry an older version and are rejected.
    if payload.get("ver", 0) != user.token_version:
        raise Unauthorized("token revoked")
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


def revoke_all_tokens(db: Session, user: User) -> None:
    """Invalidate every outstanding token for the user (logout).

    Bumps ``token_version``; tokens carrying the old version are then
    rejected on their next use.  Note this logs out *all* of the user's
    sessions, not just one — fine for this app's single-client UI.
    """
    user.token_version += 1
    commit_now(db)


def change_password(db: Session, user: User, old_password: str, new_password: str) -> None:
    """Set a new password after verifying the current one, and revoke
    all existing sessions so a leaked/old token can't outlive the change."""
    if not verify_password(old_password, user.password_hash):
        raise Unauthorized("current password is incorrect")
    user.password_hash = hash_password(new_password)
    user.token_version += 1
    commit_now(db)


def profile(db: Session, user: User) -> dict:
    """Read model for the account dropdown and 个人中心 page.

    One query joins the stored account fields with the user's live
    conversation count (the app's "创作").  ``points`` is derived from
    the two point buckets so the total can never drift from its parts.
    ``name`` is the email local-part until a display-name column exists.
    """
    works = (
        db.scalar(select(func.count(Conversation.id)).where(Conversation.user_id == user.id)) or 0
    )
    return {
        "name": user.email.split("@")[0],
        "email": user.email,
        "plan": user.plan,
        "phone": user.phone,
        "points": user.plan_points + user.pack_points,
        "plan_points": user.plan_points,
        "pack_points": user.pack_points,
        "points_expire_at": user.points_expire_at,
        "works": int(works),
    }
