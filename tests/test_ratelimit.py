"""Rate limiting on auth (per-IP) and run (per-user) endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.infra.config import get_settings


class TestAuthRateLimit:
    def test_login_throttled_after_limit(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr(get_settings(), "rate_limit_auth", 3)
        # wrong creds return 401; the limiter counts every attempt
        seen = [
            client.post(
                "/auth/login", json={"email": "x@y.com", "password": "password-1"}
            ).status_code
            for _ in range(4)
        ]
        assert seen[:3] == [401, 401, 401]
        assert seen[3] == 429

    def test_429_carries_retry_after(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr(get_settings(), "rate_limit_auth", 1)
        client.post("/auth/login", json={"email": "a@b.com", "password": "password-1"})
        res = client.post("/auth/login", json={"email": "a@b.com", "password": "password-1"})
        assert res.status_code == 429
        assert res.headers.get("Retry-After") is not None
        assert "rate limit" in res.json()["detail"]

    def test_register_is_limited_too(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr(get_settings(), "rate_limit_auth", 2)
        codes = [
            client.post(
                "/auth/register", json={"email": f"u{i}@e.com", "password": "password-1"}
            ).status_code
            for i in range(3)
        ]
        assert codes[:2] == [201, 201]
        assert codes[2] == 429


class TestRunRateLimit:
    def _runner_patch(self, monkeypatch):
        from app.controllers import manager as manager_mod
        from app.controllers.runner import ExecResult, Runner

        class QuietRunner(Runner):
            def create(self, user_id, conversation_id):
                return "sbx"

            def destroy(self, sandbox_id):
                pass

            def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
                return ExecResult(
                    exit_code=0,
                    stdout='{"type": "result", "answer": "ok", "errors": []}\n',
                    stderr="",
                )

            def reap_idle(self, ttl_seconds):
                return []

        monkeypatch.setattr(manager_mod, "_controller", None)
        monkeypatch.setattr(manager_mod, "get_runner", lambda: QuietRunner(), raising=True)

    def test_run_submissions_throttled_per_user(
        self, client: TestClient, auth_headers: dict, monkeypatch
    ) -> None:
        self._runner_patch(monkeypatch)
        monkeypatch.setattr(get_settings(), "rate_limit_runs", 2)
        cid = client.post("/conversations", json={"title": "t"}, headers=auth_headers).json()["id"]
        codes = [
            client.post(
                f"/conversations/{cid}/runs", json={"prompt": f"p{i}"}, headers=auth_headers
            ).status_code
            for i in range(3)
        ]
        # 202 accepted until the per-user window fills, then 429
        assert codes.count(429) == 1
        assert codes[2] == 429

        # let the accepted runs' worker threads finalize before the next
        # test resets the DB engine (a late finalize would hit a dead engine)
        import time

        from app.controllers.manager import get_controller

        ctrl = get_controller()
        deadline = time.time() + 10
        while ctrl._active and time.time() < deadline:
            time.sleep(0.02)


class TestLimiterUnit:
    def test_sliding_window_allows_then_blocks(self) -> None:
        from app.infra.ratelimit import SlidingWindowLimiter

        lim = SlidingWindowLimiter()
        assert lim.check("k", 2, 60)[0] is True
        assert lim.check("k", 2, 60)[0] is True
        allowed, retry = lim.check("k", 2, 60)
        assert allowed is False and retry > 0

    def test_keys_are_independent(self) -> None:
        from app.infra.ratelimit import SlidingWindowLimiter

        lim = SlidingWindowLimiter()
        assert lim.check("a", 1, 60)[0] is True
        assert lim.check("b", 1, 60)[0] is True  # different key, own budget
        assert lim.check("a", 1, 60)[0] is False

    def test_reset_clears_counters(self) -> None:
        from app.infra.ratelimit import SlidingWindowLimiter

        lim = SlidingWindowLimiter()
        lim.check("k", 1, 60)
        assert lim.check("k", 1, 60)[0] is False
        lim.reset()
        assert lim.check("k", 1, 60)[0] is True


@pytest.fixture(autouse=True)
def _reset_limiter():
    from app.infra.ratelimit import limiter

    limiter.reset()
    yield
    limiter.reset()


class TestBackendSelection:
    def test_memory_is_default(self) -> None:
        from app.infra.ratelimit import SlidingWindowLimiter, get_limiter, reset_limiter_for_tests

        reset_limiter_for_tests()
        assert isinstance(get_limiter(), SlidingWindowLimiter)

    def test_redis_selected_by_config(self, monkeypatch) -> None:
        from app.infra.ratelimit import RedisRateLimiter, get_limiter, reset_limiter_for_tests

        monkeypatch.setenv("PAW_RATE_LIMIT_BACKEND", "redis")
        get_settings.cache_clear()
        reset_limiter_for_tests()
        try:
            # build fails to connect lazily only on use; construction is fine
            assert isinstance(get_limiter(), RedisRateLimiter)
        finally:
            get_settings.cache_clear()
            reset_limiter_for_tests()

    def test_unknown_backend_raises(self, monkeypatch) -> None:
        from app.infra.ratelimit import get_limiter, reset_limiter_for_tests

        monkeypatch.setenv("PAW_RATE_LIMIT_BACKEND", "nope")
        get_settings.cache_clear()
        reset_limiter_for_tests()
        try:
            with pytest.raises(ValueError, match="unknown rate-limit backend"):
                get_limiter()
        finally:
            get_settings.cache_clear()
            reset_limiter_for_tests()


class FakeRedis:
    """Minimal in-memory stand-in for the redis client surface used by
    RedisRateLimiter: an ``eval`` that mimics the Lua window script
    atomically (single-threaded Python == atomic), plus scan_iter/delete."""

    def __init__(self) -> None:
        self.z: dict[str, list[tuple[float, str]]] = {}

    def eval(self, script, numkeys, key, now, cutoff, limit, member, ttl):  # noqa: ARG002
        now = float(now)
        cutoff = float(cutoff)
        limit = int(limit)
        items = [(s, m) for (s, m) in self.z.get(key, []) if not (0 <= s <= cutoff)]
        self.z[key] = items
        if len(items) >= limit:
            oldest = sorted(items)[0][0] if items else now
            return [0, str(oldest)]
        items.append((now, member))
        return [1, "0"]

    def scan_iter(self, match=None):  # noqa: ARG002
        return list(self.z.keys())

    def delete(self, key):
        self.z.pop(key, None)


class TestRedisRateLimiter:
    def test_allows_under_limit_then_blocks(self) -> None:
        from app.infra.ratelimit import RedisRateLimiter

        limiter = RedisRateLimiter(client=FakeRedis())
        # limit 2 per 60s: first two allowed, third blocked
        assert limiter.check("k", 2, 60)[0] is True
        assert limiter.check("k", 2, 60)[0] is True
        allowed, retry = limiter.check("k", 2, 60)
        assert allowed is False
        assert retry > 0

    def test_keys_are_independent(self) -> None:
        from app.infra.ratelimit import RedisRateLimiter

        limiter = RedisRateLimiter(client=FakeRedis())
        assert limiter.check("a", 1, 60)[0] is True
        assert limiter.check("b", 1, 60)[0] is True  # different key, own budget
        assert limiter.check("a", 1, 60)[0] is False

    def test_reset_clears(self) -> None:
        from app.infra.ratelimit import RedisRateLimiter

        limiter = RedisRateLimiter(client=FakeRedis())
        limiter.check("k", 1, 60)
        assert limiter.check("k", 1, 60)[0] is False
        limiter.reset()
        assert limiter.check("k", 1, 60)[0] is True
