"""Account profile read-model endpoint (GET /account/profile)."""

from __future__ import annotations

from fastapi.testclient import TestClient


class TestAccountProfile:
    def test_defaults_for_a_fresh_user(self, client: TestClient, auth_headers: dict) -> None:
        res = client.get("/account/profile", headers=auth_headers)
        assert res.status_code == 200, res.text
        body = res.json()
        # name derived from the email local-part; credit fields default to 0
        assert body["name"] == "dev"
        assert body["email"] == "dev@example.com"
        assert body["plan"] == "free"
        assert body["phone"] is None
        assert body["points"] == 0
        assert body["plan_points"] == 0
        assert body["pack_points"] == 0
        assert body["points_expire_at"] is None
        assert body["works"] == 0

    def test_requires_auth(self, client: TestClient) -> None:
        assert client.get("/account/profile").status_code == 401

    def test_points_is_the_sum_of_the_two_buckets(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        """total = plan_points + pack_points, and it is never stored."""
        from app.infra.db import get_session_factory
        from app.models import User

        uid = client.get("/auth/me", headers=auth_headers).json()["id"]
        db = get_session_factory()()
        try:
            u = db.get(User, uid)
            u.plan_points, u.pack_points = 30, 70
            u.plan = "pro"
            db.commit()
        finally:
            db.close()
        body = client.get("/account/profile", headers=auth_headers).json()
        assert (body["plan_points"], body["pack_points"]) == (30, 70)
        assert body["points"] == 100
        assert body["plan"] == "pro"

    def test_works_counts_only_the_callers_conversations(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        client.post("/conversations", json={"title": "a"}, headers=auth_headers)
        client.post("/conversations", json={"title": "b"}, headers=auth_headers)
        # a second user's conversations must not leak into the count
        other = client.post(
            "/auth/register", json={"email": "other@example.com", "password": "password-123"}
        ).json()
        other_h = {"Authorization": f"Bearer {other['access_token']}"}
        client.post("/conversations", json={"title": "x"}, headers=other_h)

        assert client.get("/account/profile", headers=auth_headers).json()["works"] == 2
        assert client.get("/account/profile", headers=other_h).json()["works"] == 1


class TestProfileController:
    """The read model is assembled without any HTTP layer."""

    def test_profile_payload_shape(self, client: TestClient, auth_headers: dict) -> None:
        from app.controllers import accounts
        from app.infra.db import get_session_factory
        from app.models import User

        uid = client.get("/auth/me", headers=auth_headers).json()["id"]
        db = get_session_factory()()
        try:
            data = accounts.profile(db, db.get(User, uid))
        finally:
            db.close()
        assert set(data) == {
            "name",
            "email",
            "plan",
            "phone",
            "points",
            "plan_points",
            "pack_points",
            "points_expire_at",
            "works",
        }
        assert data["points"] == data["plan_points"] + data["pack_points"]
