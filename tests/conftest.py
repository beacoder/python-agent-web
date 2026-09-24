"""Shared test fixtures: isolated DB, settings, auth client."""

from __future__ import annotations

import tempfile
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

import app.infra.db as db_mod
from app.infra.config import get_settings
from app.main import create_app


@pytest.fixture(autouse=True)
def _isolated_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    tmp = tempfile.mkdtemp(prefix="paw-test-")
    monkeypatch.setenv("PAW_DB_URL", f"sqlite:///{tmp}/test.db")
    monkeypatch.setenv("PAW_SECRET_KEY", "test-secret-key-not-for-prod-0123456789abcdef")
    monkeypatch.setenv("PAW_RUNNER", "server")
    monkeypatch.setenv("PAW_WORKSPACE_ROOT", f"{tmp}/workspaces")
    get_settings.cache_clear()
    db_mod._engine = None
    db_mod._session_factory = None
    from app.infra.ratelimit import limiter

    limiter.reset()  # the limiter is a process-wide singleton; isolate tests
    yield
    get_settings.cache_clear()
    db_mod._engine = None
    db_mod._session_factory = None


@pytest.fixture()
def client() -> Iterator[TestClient]:
    from app.infra.db import reset_engine_for_tests

    reset_engine_for_tests(get_settings().db_url)
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def auth_headers(client: TestClient) -> dict[str, str]:
    res = client.post(
        "/auth/register",
        json={"email": "dev@example.com", "password": "password-123"},
    )
    assert res.status_code == 201, res.text
    tokens = res.json()
    return {"Authorization": f"Bearer {tokens['access_token']}"}
