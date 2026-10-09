"""Application settings, loaded from environment (prefix ``PAW_``).

Nested settings use double-underscore separators, e.g.
``PAW_HARNESS__CMD=/path/to/harness`` sets ``settings.harness.cmd``.
Secrets for production (``secret_key``, Fernet ``fernet_key``) must be
provided via the environment; defaults exist only so a dev server can
boot, and startup warns when they are unset.
"""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
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
    """Cap EACH run at N LLM rounds, passed to ``serve --max-rounds``
    at spawn.  None (the default) omits the flag entirely, leaving runs
    unlimited exactly as before.

    Budgets are deliberately spawn-time arguments rather than fields on
    the submit op: ``serve`` sandboxes untrusted agent code on behalf
    of this host, so the ceiling belongs to whoever starts the sandbox
    and a run must not be able to raise its own."""

    sandbox_timeout: float | None = None
    """Per-run wall-clock budget enforced INSIDE the sandbox, passed to
    ``serve --timeout`` at spawn.  Distinct from ``timeout`` above,
    which is this host's own watchdog around the exec: the sandbox-side
    budget is cooperative (checked between rounds) and ends the run
    with a proper ``result`` line carrying real usage, where the
    host-side one can only cancel and then kill.  None omits the flag,
    leaving runs unlimited."""

    answer_timeout: float | None = None
    """How long the sandbox waits for a host answer to a mid-run
    question before falling back to "Unanswered", passed to ``serve
    --answer-timeout`` at spawn.  None omits the flag, which keeps the
    harness default of waiting forever (a web user needs time to type).

    Note that a non-None value makes answer correlation matter: once an
    ask has timed out the harness refuses an ``answer`` that carries no
    ``ask_id``, and this host does not send one yet."""


class SandboxSettings(BaseSettings):
    """Sandbox-manager knobs (runner selection is top-level)."""

    model_config = SettingsConfigDict(env_prefix="PAW_SANDBOX__")

    ttl_seconds: float = 300.0
    """Idle reaper TTL: a sandbox unused this long is destroyed.

    Only ever applied to sandboxes with no live run (see
    ``Runner.reap_idle``), so this bounds idleness, never a run."""

    reap_interval_seconds: float = 60.0
    """How often the background reaper looks for idle sandboxes.

    Before this existed the reaper ran once at startup against a
    freshly-empty in-memory registry, so ``ttl_seconds`` never applied
    to anything and idle sandboxes leaked for the life of the process.
    0 disables the background sweep (startup-only, the old no-op
    behaviour)."""

    probe_timeout: float = 5.0
    """How long to wait for ``pong`` when reusing a warm sandbox.

    A resident process that is alive but no longer reading its stdin is
    invisible to an exit-status check, and submitting to it hangs for
    the host timeout -- which is unbounded by default.  One ``ping``
    round-trip before reusing a warm process converts that hang into a
    respawn.  Only runs between turns (never while a run owns stdout).
    0 disables the probe."""


class StorageSettings(BaseSettings):
    """Durable blob-store selection for uploads and artifacts.

    ``local`` is a directory tree (dev / single node); ``s3`` is an
    S3-compatible bucket (prod / multi-node).  The per-conversation
    workspace is unaffected -- this is the durable tier behind it.
    """

    model_config = SettingsConfigDict(env_prefix="PAW_STORAGE__")

    backend: str = "local"
    """``local`` or ``s3``."""

    local_root: str = "./storage"
    """Root directory for the ``local`` backend."""

    s3_bucket: str = ""
    """Bucket name for the ``s3`` backend."""

    s3_prefix: str = ""
    """Key prefix within the bucket (namespacing, e.g. ``paw/``)."""

    s3_region: str = ""
    """AWS region; empty = SDK default resolution."""

    s3_endpoint_url: str = ""
    """Custom endpoint (e.g. MinIO / localstack); empty = real AWS."""


class DockerSettings(BaseSettings):
    """Container isolation knobs for the ``docker`` runner.

    Defaults are the safe ones: no network, dropped capabilities,
    read-only rootfs, non-root user, resource ceilings.  A wedged or
    hostile run is therefore bounded and cannot reach the host env, the
    database, or other tenants' workspaces.
    """

    model_config = SettingsConfigDict(env_prefix="PAW_DOCKER__")

    image: str = "python-agent-harness:latest"
    """Sandbox image; harness must be its entrypoint-able command.
    Pin by digest in production (``name@sha256:...``)."""

    workdir: str = "/workspace"
    """Container path the conversation workspace is mounted at (cwd)."""

    network: str = "none"
    """Container network mode; ``none`` = no egress (exfiltration/SSRF
    off).  Override only with an explicit, filtered egress policy."""

    mem_limit: str = "1g"
    """Hard memory ceiling (docker ``mem_limit`` syntax)."""

    nano_cpus: int = 1_000_000_000
    """CPU quota in units of 1e-9 CPUs (1e9 = one core)."""

    pids_limit: int = 256
    """Max PIDs in the container (fork-bomb ceiling)."""

    user: str = "1000:1000"
    """Non-root uid:gid the harness runs as inside the container."""

    read_only_rootfs: bool = True
    """Mount the container root filesystem read-only (scratch via tmpfs)."""

    tmpfs_size: str = "256m"
    """Size of the writable ``/tmp`` tmpfs mounted into the container."""

    stop_timeout: int = 5
    """Seconds to wait after SIGTERM before the daemon kills on destroy."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PAW_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    db_url: str = "sqlite:///./paw.db"
    db_pool_size: int = 5
    """SQLAlchemy connection pool size (server DBs only; ignored for
    SQLite).  One pool per web replica."""
    db_max_overflow: int = 10
    """Extra connections allowed beyond ``db_pool_size`` under load."""
    db_pool_recycle_s: int = 1800
    """Recycle a pooled connection after this many seconds, so a
    connection a proxy would drop is replaced proactively."""
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
    cancel) or ``docker`` (the same, isolated in a per-sandbox
    container: no host env, no host filesystem, no network by default)."""
    harness: HarnessSettings = Field(default_factory=HarnessSettings)
    sandbox: SandboxSettings = Field(default_factory=SandboxSettings)
    docker: DockerSettings = Field(default_factory=DockerSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)

    cors_origins: list[str] = []
    """Extra allowed CORS origins for a split frontend."""

    log_level: str = "INFO"
    """Root level for the ``paw`` logger tree."""
    log_json: bool = False
    """Emit one JSON object per log line (for log shippers) vs. text."""

    rate_limit_window_s: float = 60.0
    """Sliding window for rate limits (seconds)."""
    rate_limit_runs: int = 20
    """Max run submissions per user per window (the cost-incurring path)."""
    rate_limit_auth: int = 10
    """Max login/register attempts per client IP per window."""

    rate_limit_backend: str = "memory"
    """Rate-limit store: ``memory`` (process-local, per-instance) or
    ``redis`` (shared across instances for a global limit)."""
    rate_limit_redis_url: str = ""
    """Redis URL for the ``redis`` backend; empty = localhost default."""

    max_line_bytes: int = 4 * 1024 * 1024
    """Longest single JSONL line accepted from a sandbox.

    ``readline`` is unbounded by default, so one pathological line from
    untrusted agent code is enough to exhaust the trusted process's
    memory before anything can inspect it.  An oversize line is
    discarded (to its newline, so the remainder is not reparsed as
    fresh lines) and counted.  Generous on purpose: legitimate lines
    are far smaller than this, bounded in turn by
    ``max_event_bytes``.  0 disables the cap."""

    max_run_stdout_bytes: int = 8 * 1024 * 1024
    """Cap on the raw transcript an exec keeps in memory.

    The pump accumulated every line of a run, so a chatty agent held
    the whole stream in RAM on top of the copies in the fan-out buffer
    and the database.  Past the cap only outcome-bearing lines
    (``result``, ``error``, error/usage ``notify``) are retained, so
    the verdict and the billing numbers can never be dropped.
    0 disables the cap."""

    max_stored_events: int = 2000
    """Cap on the events persisted with a finished run.

    Consecutive ``delta`` lines are merged first (concatenation is
    lossless for rendering), which removes most of the volume; this
    bounds what survives for a pathological run.  Elided events are
    replaced by one marker so a replay is honest about the gap."""

    max_subscriber_queue: int = 1000
    """Per-SSE-subscriber queue depth.

    Unbounded queues meant one slow client could grow without limit
    while a chatty run streamed.  When full, the oldest event is
    dropped so the stream stays current; the ``id:`` gap is visible to
    the client, which can reconnect and resume.  0 disables the bound."""

    max_event_bytes: int = 64 * 1024
    """Per-event cap on relayed harness payloads.

    Everything in a ``notify``/``log`` payload is produced by untrusted
    agent code and forwarded verbatim into an SSE frame, the stored
    transcript, and the browser DOM.  Oversize bodies are replaced with
    a truncation marker rather than relayed; the envelope a client
    needs to stay parseable is never touched.  0 disables the cap."""

    shutdown_drain_seconds: float = 25.0
    """On shutdown, how long to wait for in-flight runs to finish before
    exiting.  Keep under the orchestrator's SIGTERM grace period so the
    drain completes before a forced kill; runs still in flight at the
    deadline are reconciled to ``error`` on the next startup."""

    cancel_grace_seconds: float = 10.0
    """After a USER cancels a run, how long to wait for the harness to
    honour the protocol ``cancel`` before destroying the sandbox.

    This is a recovery path, not a run budget: it is armed only by an
    explicit cancel request and never by a timer, so a run nobody
    cancels is still unlimited.  It exists because ``op:cancel`` is
    cooperative — a run wedged in an uninterruptible tool ignores it,
    and without escalation the row stays ``running`` forever, which the
    partial unique index turns into a permanent lockout for that
    conversation.  0 disables escalation (cancel stays best-effort)."""

    budget_enforce: bool = False
    """Enforce a per-user token budget before starting a run (the spend
    kill-switch).  Off by default so existing/dev deployments are
    unaffected; turn on for a public billed platform."""
    budget_free_tokens: int = 1_000_000
    """Token allowance for a user with no purchased points (the free
    tier).  Total budget = this + ``tokens_per_point`` × the user's
    points."""
    budget_tokens_per_point: int = 1000
    """How many tokens one account point is worth, converting the
    points buckets (plan_points + pack_points) into a token budget."""

    max_upload_bytes: int = 100 * 1024 * 1024
    """Per-file upload size cap."""
    max_user_storage_bytes: int = 1024 * 1024 * 1024
    """Per-user total storage quota across all conversations (sum of
    uploaded file sizes).  0 disables the quota."""
    upload_allowed_types: list[str] = []
    """Allow-list of upload content types by short name (e.g.
    ``["xlsx", "csv", "pdf", "png"]``), validated by magic bytes.  Empty
    = no content validation (any type accepted; dev default)."""


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
