"""Auth routes: register, login, refresh, me.

HTTP wiring only -- credentials, tokens and account state are decided
in ``controllers.accounts``.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from ..controllers import accounts
from ..controllers.errors import TooManyRequests
from ..infra.config import get_settings
from ..infra.db import get_db
from ..infra.ratelimit import limiter
from ..models import User
from ..validation.schemas import PasswordChange, RefreshIn, RegisterIn, TokenPair, UserOut

router = APIRouter(prefix="/auth", tags=["auth"])
_bearer = HTTPBearer(auto_error=False)


def limit_auth(request: Request) -> None:
    """Per-IP limit on unauthenticated auth attempts (brute force / signup
    abuse).  Kept here rather than in ``limits.py`` to avoid an import
    cycle with ``authenticate_user``."""
    ip = request.client.host if request.client else "unknown"
    s = get_settings()
    allowed, retry_after = limiter.check(f"auth:{ip}", s.rate_limit_auth, s.rate_limit_window_s)
    if not allowed:
        secs = int(retry_after) + 1
        raise TooManyRequests(f"rate limit exceeded; retry in {secs}s", retry_after=secs)


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
    return accounts.user_for_access_token(db, header_token or access_token)


def require_admin(user: User = Depends(authenticate_user)) -> User:
    return accounts.require_admin(user)


def _token_pair(user: User) -> TokenPair:
    access, refresh_token = accounts.token_pair(user)
    return TokenPair(access_token=access, refresh_token=refresh_token)


def _user_out(user: User) -> UserOut:
    return UserOut(
        id=user.id,
        email=user.email,
        is_active=user.is_active,
        is_admin=user.is_admin,
        created_at=user.created_at,
    )


@router.post(
    "/register",
    response_model=TokenPair,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(limit_auth)],
)
def register(body: RegisterIn, db: Session = Depends(get_db)) -> TokenPair:
    return _token_pair(accounts.register(db, body.email, body.password))


@router.post("/login", response_model=TokenPair, dependencies=[Depends(limit_auth)])
def login(body: RegisterIn, db: Session = Depends(get_db)) -> TokenPair:
    return _token_pair(accounts.login(db, body.email, body.password))


@router.post("/refresh", response_model=TokenPair)
def refresh(body: RefreshIn, db: Session = Depends(get_db)) -> TokenPair:
    return _token_pair(accounts.user_for_refresh_token(db, body.refresh_token))


@router.get("/me", response_model=UserOut)
def me(user: User = Depends(authenticate_user)) -> UserOut:
    return _user_out(user)


@router.post("/logout")
def logout(user: User = Depends(authenticate_user), db: Session = Depends(get_db)) -> dict:
    """Revoke all of the caller's tokens (access + refresh)."""
    accounts.revoke_all_tokens(db, user)
    return {"revoked": True}


@router.post("/password")
def change_password(
    body: PasswordChange,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> dict:
    """Change password (verifying the current one) and revoke existing
    sessions."""
    accounts.change_password(db, user, body.old_password, body.new_password)
    return {"changed": True}


@router.get("/admin-check")
def admin_check(user: User = Depends(require_admin)) -> dict:
    return {"admin": user.email}
