"""Security primitives: password hashing, JWT access/refresh tokens.

Kept dependency-light on purpose (PyJWT + stdlib PBKDF2) so the
trusted side has a small, auditable surface.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from typing import Any

import jwt

from .config import get_settings

_PBKDF2_ITERATIONS = 240_000


def hash_password(password: str) -> str:
    """PBKDF2-HMAC-SHA256; ``salt$hash`` hex format."""
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), salt.encode(), _PBKDF2_ITERATIONS
    ).hex()
    return f"{salt}${digest}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt, expected = stored.split("$", 1)
    except ValueError:
        return False
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), salt.encode(), _PBKDF2_ITERATIONS
    ).hex()
    return hmac.compare_digest(digest, expected)


def create_token(
    subject: str, kind: str, expires_seconds: float, extra: dict[str, Any] | None = None
) -> str:
    settings = get_settings()
    now = int(time.time())
    payload: dict[str, Any] = {
        "sub": subject,
        "type": kind,
        "iat": now,
        "exp": now + int(expires_seconds),
        "jti": secrets.token_hex(8),
    }
    if extra:
        payload.update(extra)
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


def create_access_token(user_id: str, token_version: int = 0) -> str:
    return create_token(
        user_id,
        "access",
        get_settings().access_token_minutes * 60,
        {"role": "user", "ver": token_version},
    )


def create_refresh_token(user_id: str, token_version: int = 0) -> str:
    return create_token(
        user_id, "refresh", get_settings().refresh_token_days * 86400, {"ver": token_version}
    )


def decode_token(token: str, expected_type: str) -> dict[str, Any]:
    """Decode and validate a JWT; raises ``jwt.PyJWTError`` on any
    problem (bad signature, expired, wrong type)."""
    settings = get_settings()
    payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
    if payload.get("type") != expected_type:
        raise jwt.InvalidTokenError(f"expected {expected_type} token")
    return payload


def derive_fernet_key(secret: str) -> bytes:
    """Stable 32-byte key from an arbitrary secret (dev fallback)."""
    return hashlib.sha256(secret.encode()).digest()
