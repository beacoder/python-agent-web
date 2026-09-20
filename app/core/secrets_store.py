"""Fernet-encrypted secrets store.

Values are encrypted at rest with a key from settings (``fernet_key``
or derived from ``secret_key`` in dev).  The API never returns a
decrypted value; decryption exists so the sandbox manager can inject
secrets into the sandbox environment.
"""

from __future__ import annotations

import base64

from cryptography.fernet import Fernet, InvalidToken

from .config import get_settings
from .security import derive_fernet_key


def _fernet() -> Fernet:
    settings = get_settings()
    key = settings.fernet_key
    if key:
        # Must be a 32-byte url-safe base64 key (e.g. Fernet.generate_key()).
        if isinstance(key, str):
            key = key.encode()
    else:
        # Dev fallback: derive a stable valid key from the app secret.
        key = base64.urlsafe_b64encode(derive_fernet_key(settings.secret_key))
    return Fernet(key)


def encrypt(value: str) -> str:
    return _fernet().encrypt(value.encode()).decode()


def decrypt(token: str) -> str:
    try:
        return _fernet().decrypt(token.encode()).decode()
    except InvalidToken as exc:
        raise ValueError("secret decryption failed (wrong key?)") from exc
