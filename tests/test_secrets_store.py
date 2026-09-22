"""Secrets store + admin dependency edge cases."""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

from app.infra import secrets_store


def test_explicit_fernet_key_used(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.infra.config import get_settings

    key = Fernet.generate_key().decode()
    monkeypatch.setattr(get_settings(), "fernet_key", key)
    token = secrets_store.encrypt("payload")
    assert secrets_store.decrypt(token) == "payload"


def test_bad_fernet_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.infra.config import get_settings

    monkeypatch.setattr(get_settings(), "fernet_key", "not-a-valid-fernet-key")
    with pytest.raises(ValueError, match="32 url-safe base64"):
        secrets_store.encrypt("payload")


def test_decrypt_wrong_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.infra.config import get_settings

    token = secrets_store.encrypt("payload")
    monkeypatch.setattr(get_settings(), "fernet_key", Fernet.generate_key().decode())
    with pytest.raises(ValueError, match="decryption failed"):
        secrets_store.decrypt(token)


def test_derived_key_survives_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.infra.config import get_settings

    monkeypatch.setattr(get_settings(), "fernet_key", "")
    token = secrets_store.encrypt("stable")
    assert secrets_store.decrypt(token) == "stable"
