"""Token revocation via token_version: logout and password change."""

from __future__ import annotations

from fastapi.testclient import TestClient


def _register(client: TestClient, email: str = "rev@example.com"):
    tokens = client.post("/auth/register", json={"email": email, "password": "password-1"}).json()
    return tokens["access_token"], tokens["refresh_token"]


class TestLogoutRevocation:
    def test_logout_invalidates_access_and_refresh(self, client: TestClient) -> None:
        access, refresh = _register(client)
        h = {"Authorization": f"Bearer {access}"}
        assert client.get("/auth/me", headers=h).status_code == 200

        assert client.post("/auth/logout", headers=h).status_code == 200

        # both the access token and the refresh token are now dead
        assert client.get("/auth/me", headers=h).status_code == 401
        assert client.post("/auth/refresh", json={"refresh_token": refresh}).status_code == 401

    def test_fresh_login_after_logout_works(self, client: TestClient) -> None:
        access, _ = _register(client, "relog@example.com")
        client.post("/auth/logout", headers={"Authorization": f"Bearer {access}"})
        # a new login mints tokens at the new version
        res = client.post(
            "/auth/login", json={"email": "relog@example.com", "password": "password-1"}
        )
        assert res.status_code == 200
        new = res.json()["access_token"]
        assert client.get("/auth/me", headers={"Authorization": f"Bearer {new}"}).status_code == 200


class TestPasswordChangeRevocation:
    def test_change_password_revokes_old_tokens(self, client: TestClient) -> None:
        access, refresh = _register(client, "pw@example.com")
        h = {"Authorization": f"Bearer {access}"}
        res = client.post(
            "/auth/password",
            headers=h,
            json={"old_password": "password-1", "new_password": "password-2"},
        )
        assert res.status_code == 200
        # old sessions are gone
        assert client.get("/auth/me", headers=h).status_code == 401
        assert client.post("/auth/refresh", json={"refresh_token": refresh}).status_code == 401
        # new password works, old doesn't
        assert (
            client.post(
                "/auth/login", json={"email": "pw@example.com", "password": "password-2"}
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/auth/login", json={"email": "pw@example.com", "password": "password-1"}
            ).status_code
            == 401
        )

    def test_wrong_current_password_rejected(self, client: TestClient) -> None:
        access, _ = _register(client, "pw2@example.com")
        res = client.post(
            "/auth/password",
            headers={"Authorization": f"Bearer {access}"},
            json={"old_password": "wrong-one", "new_password": "password-2"},
        )
        assert res.status_code == 401
        # unchanged: original password still works
        assert (
            client.post(
                "/auth/login", json={"email": "pw2@example.com", "password": "password-1"}
            ).status_code
            == 200
        )


class TestRefreshStillWorksNormally:
    def test_refresh_rotates_without_revocation(self, client: TestClient) -> None:
        _, refresh = _register(client, "norm@example.com")
        res = client.post("/auth/refresh", json={"refresh_token": refresh})
        assert res.status_code == 200
        new_access = res.json()["access_token"]
        assert (
            client.get("/auth/me", headers={"Authorization": f"Bearer {new_access}"}).status_code
            == 200
        )
