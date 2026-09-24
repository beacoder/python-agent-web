"""In-memory sliding-window rate limiter.

Process-local: the counters live in this process's memory, so limits are
per-instance.  That's the right trade for a single-instance deployment;
running multiple web instances would need a shared store (Redis) or
sticky routing to enforce a global limit.  Good enough to stop one
client hammering the money-spending run endpoint or brute-forcing auth.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque


class SlidingWindowLimiter:
    """Allow at most ``limit`` events per ``window`` seconds per key."""

    def __init__(self) -> None:
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str, limit: int, window: float) -> tuple[bool, float]:
        """Record an attempt for *key*. Returns ``(allowed, retry_after)``.

        ``retry_after`` is seconds until the oldest hit in the window
        expires (0 when allowed).
        """
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
        """Forget all counters (used to isolate tests)."""
        with self._lock:
            self._hits.clear()


# Process-wide limiter shared by all rate-limit dependencies.
limiter = SlidingWindowLimiter()
