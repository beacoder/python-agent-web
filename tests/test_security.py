"""Security primitives: password hashing + JWT."""

from __future__ import annotations

import jwt as pyjwt
import pytest

from app.infra.config import get_settings
from app.infra.security import (
    create_access_token,
    create_refresh_token,
    create_token,
    decode_token,
    derive_fernet_key,
    hash_password,
    verify_password,
)


class TestPasswords:
    def test_roundtrip(self) -> None:
        stored = hash_password("s3cret-password")
        assert verify_password("s3cret-password", stored)
        assert not verify_password("wrong", stored)

    def test_unique_salts(self) -> None:
        assert hash_password("x") != hash_password("x")

    def test_malformed_stored(self) -> None:
        assert not verify_password("x", "no-separator")


class TestTokens:
    def test_access_roundtrip(self) -> None:
        token = create_access_token("usr_1")
        payload = decode_token(token, expected_type="access")
        assert payload["sub"] == "usr_1"
        assert payload["type"] == "access"

    def test_refresh_rejected_as_access(self) -> None:
        with pytest.raises(pyjwt.InvalidTokenError):
            decode_token(create_refresh_token("usr_1"), expected_type="access")

    def test_expired_rejected(self) -> None:
        token = create_token("usr_1", "access", expires_seconds=-10)
        with pytest.raises(pyjwt.ExpiredSignatureError):
            decode_token(token, expected_type="access")

    def test_bad_signature_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        token = create_access_token("usr_1")
        monkeypatch.setattr(get_settings(), "secret_key", "other-key-with-32-bytes-0123456789")
        with pytest.raises(pyjwt.InvalidSignatureError):
            decode_token(token, expected_type="access")

    def test_garbage_rejected(self) -> None:
        with pytest.raises(pyjwt.PyJWTError):
            decode_token("not-a-jwt", expected_type="access")

    def test_derive_key_stable(self) -> None:
        assert derive_fernet_key("abc") == derive_fernet_key("abc")
        assert len(derive_fernet_key("abc")) == 32
