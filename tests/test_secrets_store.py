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


class TestDecryptForUser:
    """Host-side secret injection map used by the docker runner."""

    def _session(self):
        from app.infra.config import get_settings
        from app.infra.db import get_session_factory, reset_engine_for_tests

        reset_engine_for_tests(get_settings().db_url)
        return get_session_factory()()

    def _user(self, db):
        from app.models import User, new_id

        user = User(id=new_id("usr"), email="s@example.com", password_hash="x")
        db.add(user)
        db.flush()
        return user

    def test_decrypts_all_owned_secrets(self) -> None:
        from app.controllers import secrets as secrets_controller

        with self._session() as db:
            user = self._user(db)
            secrets_controller.put(
                db, user, "OPENAI_API_KEY", body_name="OPENAI_API_KEY", value="sk-1"
            )
            secrets_controller.put(db, user, "STRIPE_KEY", body_name="STRIPE_KEY", value="sk-2")
            env = secrets_controller.decrypt_for_user(db, user.id)
        assert env == {"OPENAI_API_KEY": "sk-1", "STRIPE_KEY": "sk-2"}

    def test_skips_non_env_identifier_names(self) -> None:
        from app.controllers import secrets as secrets_controller

        with self._session() as db:
            user = self._user(db)
            # a name that is a valid secret but not a valid env var key
            secrets_controller.put(db, user, "my-key.v2", body_name="my-key.v2", value="nope")
            secrets_controller.put(db, user, "GOOD_KEY", body_name="GOOD_KEY", value="yes")
            env = secrets_controller.decrypt_for_user(db, user.id)
        assert env == {"GOOD_KEY": "yes"}  # dotted/hyphenated name skipped

    def test_isolated_per_user(self) -> None:
        from app.controllers import secrets as secrets_controller

        with self._session() as db:
            owner = self._user(db)
            from app.models import User, new_id

            other = User(id=new_id("usr"), email="o@example.com", password_hash="x")
            db.add(other)
            db.flush()
            secrets_controller.put(db, owner, "MINE", body_name="MINE", value="v")
            env = secrets_controller.decrypt_for_user(db, other.id)
        assert env == {}  # another user's secrets never leak in
