"""Auth routes: register, login, refresh, me."""

from __future__ import annotations

import jwt as pyjwt
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..core.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from ..db import commit_now, get_db
from ..models import User, new_id
from ..schemas import RefreshIn, RegisterIn, TokenPair, UserOut

router = APIRouter(prefix="/auth", tags=["auth"])
_bearer = HTTPBearer(auto_error=False)


def _user_for_token(db: Session, token: str | None) -> User:
    """Shared bearer-token validation for header and query-param auth."""
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token")
    try:
        payload = decode_token(token, expected_type="access")
    except pyjwt.PyJWTError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"invalid token: {exc}") from exc
    user = db.get(User, payload["sub"])
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "unknown or inactive user")
    return user


def authenticate_user(
    db: Session = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    access_token: str | None = Query(default=None, include_in_schema=False),
) -> User:
    """Validate the bearer JWT and load the user (401 otherwise).

    Accepts the token from the ``Authorization`` header or, as a
    fallback for EventSource (which cannot set headers), an
    ``access_token`` query parameter.
    """
    header_token = credentials.credentials if credentials is not None else None
    return _user_for_token(db, header_token or access_token)


def _token_pair(user: User) -> TokenPair:
    return TokenPair(
        access_token=create_access_token(user.id),
        refresh_token=create_refresh_token(user.id),
    )


@router.post("/register", response_model=TokenPair, status_code=status.HTTP_201_CREATED)
def register(body: RegisterIn, db: Session = Depends(get_db)) -> TokenPair:
    exists = db.scalar(select(User).where(User.email == body.email))
    if exists is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, "email already registered")
    user = User(
        id=new_id("usr"),
        email=body.email,
        password_hash=hash_password(body.password),
    )
    db.add(user)
    db.flush()
    commit_now(db)
    return _token_pair(user)


@router.post("/login", response_model=TokenPair)
def login(body: RegisterIn, db: Session = Depends(get_db)) -> TokenPair:
    user = db.scalar(select(User).where(User.email == body.email))
    if user is None or not verify_password(body.password, user.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid email or password")
    if not user.is_active:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "account disabled")
    return _token_pair(user)


@router.post("/refresh", response_model=TokenPair)
def refresh(body: RefreshIn, db: Session = Depends(get_db)) -> TokenPair:
    try:
        payload = decode_token(body.refresh_token, expected_type="refresh")
    except pyjwt.PyJWTError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"invalid token: {exc}") from exc
    user = db.get(User, payload["sub"])
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "unknown or inactive user")
    return _token_pair(user)


@router.get("/me", response_model=UserOut)
def me(user: User = Depends(authenticate_user)) -> UserOut:
    return UserOut(
        id=user.id,
        email=user.email,
        is_active=user.is_active,
        is_admin=user.is_admin,
        created_at=user.created_at,
    )


def require_admin(user: User = Depends(authenticate_user)) -> User:
    if not user.is_admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "admin required")
    return user


@router.get("/admin-check")
def admin_check(user: User = Depends(require_admin)) -> dict:
    return {"admin": user.email}
