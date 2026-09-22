"""Application settings, loaded from environment (prefix ``PAW_``).

Nested settings use double-underscore separators, e.g.
``PAW_HARNESS__CMD=/path/to/harness`` sets ``settings.harness.cmd``.
Secrets for production (``secret_key``, Fernet ``fernet_key``) must be
provided via the environment; defaults exist only so a dev server can
boot, and startup warns when they are unset.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class HarnessSettings(BaseSettings):
    """How to exec the (untrusted) agent harness."""

    model_config = SettingsConfigDict(env_prefix="PAW_HARNESS__")

    cmd: str = "python-agent-harness"
    """Harness binary to run (must support ``serve``)."""

    cwd: str = ""
    """Agent workspace directory; empty = the server process's cwd."""

    timeout: float | None = None
    """Host-side wall-clock budget for one run (result line); off by
    default and left to config inside the sandbox image."""

    max_rounds: int | None = None
    """Reserved: not sent over the serve protocol today (serve runs
    have no harness-side round budget)."""


class SandboxSettings(BaseSettings):
    """Sandbox-manager knobs (runner selection is top-level)."""

    model_config = SettingsConfigDict(env_prefix="PAW_SANDBOX__")

    ttl_seconds: float = 300.0
    """Idle reaper TTL: a sandbox unused this long is destroyed."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PAW_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    db_url: str = "sqlite:///./paw.db"
    secret_key: str = "dev-only-insecure-key-change-me-0123456789abcdef"
    fernet_key: str = ""
    """Fernet key for the secrets store; empty = derived from
    ``secret_key`` (dev convenience, stable across restarts)."""

    algorithm: str = "HS256"
    access_token_minutes: float = 30.0
    refresh_token_days: float = 14.0

    workspace_root: str = "./workspaces"
    """Base directory for per-conversation agent workspaces; uploaded
    files are stored under ``<root>/<conversation_id>/``."""
    runner: str = "server"
    """Sandbox runner: ``server`` (one resident ``harness serve``
    process per sandbox: multi-turn memory, mid-run Q&A, protocol-level
    cancel).  Docker later."""
    harness: HarnessSettings = HarnessSettings()
    sandbox: SandboxSettings = SandboxSettings()

    cors_origins: list[str] = []
    """Extra allowed CORS origins for a split frontend."""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def conversation_workspace(conversation_id: str) -> Path:
    """Per-conversation agent workspace (created on demand).

    Uploaded files are placed here so the agent sees them as plain
    files in its cwd.  ``PAW_HARNESS__CWD`` still wins when set
    explicitly (e.g. a shared dev workspace).
    """
    settings = get_settings()
    if settings.harness.cwd:
        path = Path(settings.harness.cwd)
    else:
        path = Path(settings.workspace_root) / conversation_id
    path.mkdir(parents=True, exist_ok=True)
    return path
