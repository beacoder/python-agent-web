"""Sliding-window rate limiting with a pluggable backend.

Two backends behind one ``check(key, limit, window)`` interface:

* ``SlidingWindowLimiter`` -- process-local (in-memory).  The default:
  correct for a single instance, and the right trade for dev.  Limits
  are per-instance, so N replicas allow N x the intended rate.
* ``RedisRateLimiter`` -- a shared sorted-set window in Redis, so the
  limit is *global* across every instance.  Select it for a
  multi-instance deploy (``PAW_RATE_LIMIT__BACKEND=redis``).

Both throttle the money-spending run endpoint (per user) and login /
register (per IP), raising 429 + ``Retry-After`` when a window is full.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Any, Protocol, runtime_checkable

from .config import get_settings


@runtime_checkable
class RateLimiter(Protocol):
    """Allow at most ``limit`` events per ``window`` seconds per key."""

    def check(self, key: str, limit: int, window: float) -> tuple[bool, float]:
        """Record an attempt; return ``(allowed, retry_after_seconds)``."""
        ...

    def reset(self) -> None:
        """Forget all counters (used to isolate tests)."""
        ...


class SlidingWindowLimiter:
    """Process-local sliding window (in-memory)."""

    def __init__(self) -> None:
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str, limit: int, window: float) -> tuple[bool, float]:
        now = time.monotonic()
        cutoff = now - window
        with self._lock:
            hits = self._hits[key]
            while hits and hits[0] <= cutoff:
                hits.popleft()
            if len(hits) >= limit:
                return False, max(0.0, window - (now - hits[0]))
            hits.append(now)
            return True, 0.0

    def reset(self) -> None:
        with self._lock:
            self._hits.clear()


class RedisRateLimiter:
    """Shared sliding window backed by a Redis sorted set per key.

    The trim + count + conditional-add is done in a single Lua ``eval``
    so it is atomic on the Redis server: concurrent checks across
    instances cannot all observe ``count < limit`` and then each add,
    which a client-side read-then-write pipeline would allow (and which
    would defeat the whole point of a *global* limiter).  The client is
    injectable for tests.
    """

    # KEYS[1]=zset  ARGV[1]=now  ARGV[2]=cutoff  ARGV[3]=limit
    # ARGV[4]=member  ARGV[5]=ttl.  Returns {allowed(0/1), retry_ms_basis}.
    _LUA = """
    redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, tonumber(ARGV[2]))
    local count = redis.call('ZCARD', KEYS[1])
    if count >= tonumber(ARGV[3]) then
      local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
      return {0, oldest[2]}
    end
    redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[5]))
    return {1, '0'}
    """

    def __init__(self, url: str | None = None, client: Any = None, prefix: str = "rl:") -> None:
        self._client = client
        self._url = url
        self._prefix = prefix

    def _redis(self) -> Any:
        if self._client is None:
            import redis  # lazy: only when the redis backend is selected

            self._client = redis.Redis.from_url(self._url or "redis://localhost:6379/0")
        return self._client

    def check(self, key: str, limit: int, window: float) -> tuple[bool, float]:
        r = self._redis()
        full_key = self._prefix + key
        now = time.time()
        cutoff = now - window
        # a globally-unique member: uuid4 avoids the id() reuse/collision
        # that could silently overwrite a live hit and under-count
        member = f"{now}:{uuid.uuid4().hex}"
        try:
            allowed, basis = r.eval(
                self._LUA, 1, full_key, now, cutoff, limit, member, int(window) + 1
            )
        except Exception:
            # a limiter outage must not take down the endpoint: fail open
            # (allow) rather than 500.  Logged by the caller if needed.
            return True, 0.0
        if int(allowed) == 1:
            return True, 0.0
        oldest_score = float(basis) if basis else now
        return False, max(0.0, window - (now - oldest_score))

    def reset(self) -> None:
        # scoped flush of our namespace; best-effort (tests / admin)
        r = self._redis()
        for k in r.scan_iter(match=self._prefix + "*"):
            r.delete(k)


def _build_limiter() -> RateLimiter:
    backend = get_settings().rate_limit_backend
    if backend == "memory":
        return SlidingWindowLimiter()
    if backend == "redis":
        return RedisRateLimiter(url=get_settings().rate_limit_redis_url or None)
    raise ValueError(f"unknown rate-limit backend: {backend!r}")


# Process-wide limiter shared by all rate-limit dependencies.  Rebuilt
# lazily so a config change (backend selection) is picked up in tests.
_limiter: RateLimiter | None = None
_limiter_lock = threading.Lock()


def get_limiter() -> RateLimiter:
    global _limiter
    with _limiter_lock:
        if _limiter is None:
            _limiter = _build_limiter()
        return _limiter


def reset_limiter_for_tests() -> None:
    """Drop the cached limiter so the next call rebuilds from config."""
    global _limiter
    with _limiter_lock:
        _limiter = None


class _LimiterProxy:
    """Attribute proxy so existing ``from ratelimit import limiter`` call
    sites keep working while the real backend is selected lazily."""

    def check(self, key: str, limit: int, window: float) -> tuple[bool, float]:
        return get_limiter().check(key, limit, window)

    def reset(self) -> None:
        # reset is best-effort (test/admin cleanup): never raise, even if
        # the configured backend can't be built or reached right now.
        try:
            get_limiter().reset()
        except Exception:
            reset_limiter_for_tests()


# Back-compat singleton used by the rate-limit dependencies.
limiter = _LimiterProxy()
