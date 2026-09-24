"""Auth + API integration tests (TestClient)."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.infra.db import get_session_factory
from app.models import Run, new_id


def _utcnow() -> datetime:
    return datetime.now(UTC)


class TestAuthFlow:
    def test_register_login_me(self, client: TestClient) -> None:
        res = client.post(
            "/auth/register", json={"email": "a@example.com", "password": "password-1"}
        )
        assert res.status_code == 201
        tokens = res.json()
        assert tokens["token_type"] == "bearer"
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        me = client.get("/auth/me", headers=headers)
        assert me.status_code == 200
        assert me.json()["email"] == "a@example.com"

    def test_duplicate_register_conflict(self, client: TestClient) -> None:
        body = {"email": "dup@example.com", "password": "password-1"}
        assert client.post("/auth/register", json=body).status_code == 201
        assert client.post("/auth/register", json=body).status_code == 409

    def test_login_wrong_password(self, client: TestClient) -> None:
        client.post("/auth/register", json={"email": "b@example.com", "password": "password-1"})
        res = client.post(
            "/auth/login", json={"email": "b@example.com", "password": "wrong-pass-1"}
        )
        assert res.status_code == 401

    def test_refresh_flow(self, client: TestClient) -> None:
        tokens = client.post(
            "/auth/register", json={"email": "c@example.com", "password": "password-1"}
        ).json()
        res = client.post("/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
        assert res.status_code == 200
        assert "access_token" in res.json()

    def test_refresh_with_access_token_rejected(self, client: TestClient) -> None:
        tokens = client.post(
            "/auth/register", json={"email": "d@example.com", "password": "password-1"}
        ).json()
        res = client.post("/auth/refresh", json={"refresh_token": tokens["access_token"]})
        assert res.status_code == 401

    def test_me_requires_token(self, client: TestClient) -> None:
        assert client.get("/auth/me").status_code == 401

    def test_me_bad_token(self, client: TestClient) -> None:
        res = client.get("/auth/me", headers={"Authorization": "Bearer garbage"})
        assert res.status_code == 401

    def test_short_password_rejected(self, client: TestClient) -> None:
        res = client.post("/auth/register", json={"email": "e@example.com", "password": "short"})
        assert res.status_code == 422


class TestHealth:
    def test_healthz(self, client: TestClient) -> None:
        assert client.get("/healthz").json() == {"ok": True, "runner": "server"}


class TestConversations:
    def test_crud(self, client: TestClient, auth_headers: dict) -> None:
        res = client.post("/conversations", json={"title": "my chat"}, headers=auth_headers)
        assert res.status_code == 201
        conversation_id = res.json()["id"]
        assert res.json()["title"] == "my chat"

        listing = client.get("/conversations", headers=auth_headers)
        assert [c["id"] for c in listing.json()] == [conversation_id]

        detail = client.get(f"/conversations/{conversation_id}", headers=auth_headers)
        assert detail.status_code == 200
        assert detail.json()["runs"] == []

        assert (
            client.delete(f"/conversations/{conversation_id}", headers=auth_headers).status_code
            == 204
        )
        assert (
            client.get(f"/conversations/{conversation_id}", headers=auth_headers).status_code == 404
        )

    def test_isolated_per_user(self, client: TestClient) -> None:
        tokens_a = client.post(
            "/auth/register", json={"email": "a2@example.com", "password": "password-1"}
        ).json()
        headers_a = {"Authorization": f"Bearer {tokens_a['access_token']}"}
        conversation_id = client.post("/conversations", json={}, headers=headers_a).json()["id"]
        tokens_b = client.post(
            "/auth/register", json={"email": "b2@example.com", "password": "password-1"}
        ).json()
        headers_b = {"Authorization": f"Bearer {tokens_b['access_token']}"}
        assert client.get(f"/conversations/{conversation_id}", headers=headers_b).status_code == 404

    def test_delete_with_runs_cascades(self, client: TestClient, auth_headers: dict) -> None:
        conversation_id = client.post("/conversations", json={}, headers=auth_headers).json()["id"]
        # a finished run row attached to the conversation
        db = get_session_factory()()
        try:
            db.add(
                Run(
                    id=new_id("run"),
                    conversation_id=conversation_id,
                    prompt="p",
                    status="done",
                    finished_at=_utcnow(),
                )
            )
            db.commit()
        finally:
            db.close()
        res = client.delete(f"/conversations/{conversation_id}", headers=auth_headers)
        assert res.status_code == 204, res.text

    def test_requires_auth(self, client: TestClient) -> None:
        assert client.get("/conversations").status_code == 401


class TestSecretsRoutes:
    def test_put_list_delete(self, client: TestClient, auth_headers: dict) -> None:
        res = client.put(
            "/secrets/API_KEY",
            json={"name": "API_KEY", "value": "hunter2"},
            headers=auth_headers,
        )
        assert res.status_code == 201
        assert res.json()["name"] == "API_KEY"
        assert "value" not in res.json()

        listing = client.get("/secrets", headers=auth_headers)
        assert listing.json() == [
            {"name": "API_KEY", "created_at": listing.json()[0]["created_at"]}
        ]

        assert client.delete("/secrets/API_KEY", headers=auth_headers).status_code == 204
        assert client.get("/secrets", headers=auth_headers).json() == []

    def test_value_never_returned(self, client: TestClient, auth_headers: dict) -> None:
        client.put(
            "/secrets/TOKEN",
            json={"name": "TOKEN", "value": "super-secret"},
            headers=auth_headers,
        )
        body = client.get("/secrets", headers=auth_headers).text
        assert "super-secret" not in body

    def test_overwrite_keeps_single_row(self, client: TestClient, auth_headers: dict) -> None:
        for value in ("one", "two"):
            client.put("/secrets/K", json={"name": "K", "value": value}, headers=auth_headers)
        assert len(client.get("/secrets", headers=auth_headers).json()) == 1

    def test_missing_delete_404(self, client: TestClient, auth_headers: dict) -> None:
        assert client.delete("/secrets/NOPE", headers=auth_headers).status_code == 404

    def test_bad_name_rejected(self, client: TestClient, auth_headers: dict) -> None:
        res = client.put(
            "/secrets/bad%20name",
            json={"name": "bad name", "value": "v"},
            headers=auth_headers,
        )
        assert res.status_code == 422

    def test_body_name_must_match_path(self, client: TestClient, auth_headers: dict) -> None:
        """Otherwise the write would land on a different secret than the
        URL names."""
        res = client.put(
            "/secrets/WANTED",
            json={"name": "OTHER", "value": "v"},
            headers=auth_headers,
        )
        assert res.status_code == 400, res.text
        assert res.json()["detail"] == "name mismatch with path"
        assert client.get("/secrets", headers=auth_headers).json() == []

    def test_isolated_per_user(self, client: TestClient) -> None:
        t1 = client.post(
            "/auth/register", json={"email": "s1@example.com", "password": "password-1"}
        ).json()
        t2 = client.post(
            "/auth/register", json={"email": "s2@example.com", "password": "password-1"}
        ).json()
        h1 = {"Authorization": f"Bearer {t1['access_token']}"}
        h2 = {"Authorization": f"Bearer {t2['access_token']}"}
        client.put("/secrets/X", json={"name": "X", "value": "v"}, headers=h1)
        assert client.get("/secrets", headers=h2).json() == []

    def test_refresh_token_of_unknown_user_rejected(self, client: TestClient) -> None:
        import jwt as pyjwt

        from app.infra.config import get_settings
        from app.infra.security import create_token

        token = create_token("usr_missing", "refresh", 600)
        assert client.post("/auth/refresh", json={"refresh_token": token}).status_code == 401
        _ = pyjwt, get_settings  # keep imports referenced

    def test_access_token_of_unknown_user_rejected(self, client: TestClient) -> None:
        from app.infra.security import create_token

        token = create_token("usr_missing", "access", 600)
        res = client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert res.status_code == 401

    def test_admin_check_forbidden_for_regular(self, client: TestClient) -> None:
        tokens = client.post(
            "/auth/register", json={"email": "adm@example.com", "password": "password-1"}
        ).json()
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        res = client.get("/auth/admin-check", headers=headers)
        assert res.status_code == 403

    def test_inactive_user_rejected(self, client: TestClient) -> None:

        tokens = client.post(
            "/auth/register", json={"email": "inact@example.com", "password": "password-1"}
        ).json()
        import tempfile

        from app.infra.config import get_settings
        from app.infra.db import get_session_factory
        from app.models import User

        db = get_session_factory()()
        try:
            user = db.query(User).filter(User.email == "inact@example.com").one()
            user.is_active = False
            db.commit()
        finally:
            db.close()
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        assert client.get("/auth/me", headers=headers).status_code == 401
        # login blocked for disabled accounts too
        res = client.post(
            "/auth/login",
            json={"email": "inact@example.com", "password": "password-1"},
        )
        assert res.status_code == 403
        _ = get_settings, tempfile

    def test_decrypt_roundtrip(self, client: TestClient, auth_headers: dict) -> None:
        from app.infra import secrets_store
        from app.infra.db import get_session_factory
        from app.models import Secret

        client.put("/secrets/KEY", json={"name": "KEY", "value": "plain"}, headers=auth_headers)
        db = get_session_factory()()
        try:
            row = db.query(Secret).one()
            assert row.value_encrypted != "plain"
            assert secrets_store.decrypt(row.value_encrypted) == "plain"
        finally:
            db.close()


class TestBillingRoutes:
    def test_empty_summary(self, client: TestClient, auth_headers: dict) -> None:
        summary = client.get("/usage/summary", headers=auth_headers).json()
        assert summary == {
            "input_tokens": 0,
            "output_tokens": 0,
            "rounds": 0,
            "runs": 0,
            "by_conversation": {},
        }

    def test_summary_after_run(self, client: TestClient, auth_headers: dict) -> None:
        from app.infra.db import get_session_factory
        from app.models import Conversation, UsageEvent, new_id

        conversation_id = client.post(
            "/conversations", json={"title": "bill"}, headers=auth_headers
        ).json()["id"]
        db = get_session_factory()()
        try:
            db.add(
                UsageEvent(
                    id=new_id("use"),
                    user_id=client.get("/auth/me", headers=auth_headers).json()["id"],
                    conversation_id=conversation_id,
                    run_id="run_x",
                    input_tokens=100,
                    output_tokens=50,
                    rounds=2,
                    model="m",
                )
            )
            db.query(Conversation).filter(Conversation.id == conversation_id).one()
            db.commit()
        finally:
            db.close()
        summary = client.get("/usage/summary", headers=auth_headers).json()
        assert summary["input_tokens"] == 100
        assert summary["output_tokens"] == 50
        assert summary["rounds"] == 2
        assert summary["by_conversation"] == {conversation_id: 150}
