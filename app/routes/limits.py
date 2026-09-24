"""Rate-limit dependency for the cost-incurring run endpoint (per user).

The per-IP auth limiter lives in ``auth.py`` instead — putting it here
would form an import cycle (this module needs ``authenticate_user`` from
``auth``).  Both share the process-local ``limiter`` and raise
``TooManyRequests`` (429 + Retry-After) when a window is exceeded.
"""

from __future__ import annotations

from fastapi import Depends

from ..controllers.errors import TooManyRequests
from ..infra.config import get_settings
from ..infra.ratelimit import limiter
from ..models import User
from .auth import authenticate_user


def limit_runs(user: User = Depends(authenticate_user)) -> None:
    """Per-user limit on run submissions (each spends LLM tokens)."""
    s = get_settings()
    allowed, retry_after = limiter.check(
        f"runs:{user.id}", s.rate_limit_runs, s.rate_limit_window_s
    )
    if not allowed:
        secs = int(retry_after) + 1
        raise TooManyRequests(f"rate limit exceeded; retry in {secs}s", retry_after=secs)
