<div align="center">

# python-agent-web

**A minimal Web interface for validating agent runtime and interaction patterns.**

</div>

---

## Contents

- [What this is](#what-this-is)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [How a run works](#how-a-run-works)
- [The `serve` protocol](#the-serve-protocol)
- [Sandbox isolation](#sandbox-isolation)
- [Storage & durability](#storage--durability)
- [Billing, budgets & rate limits](#billing-budgets--rate-limits)
- [Observability](#observability)
- [Database & migrations](#database--migrations)
- [Configuration](#configuration)
- [Design principles](#design-principles)

---

## What this is

`python-agent-web` is a **minimal web interface for validating agent
runtime and interaction patterns** — a chat UI and API over a real,
sandboxed agent, used to exercise how runs, streaming events, mid-run
questions, files, and billing behave end to end.

Architecturally it is the **trusted** half of an agent platform: the API
server, run controller, auth, billing, and secret storage.

The **untrusted** half — the agent runtime itself — is a separate
program, [`python-agent-harness`](#). The web layer never imports it.
Instead it runs the harness as a **resident subprocess per sandbox**
(`python-agent-harness serve`) and talks to it over a bidirectional
JSON-lines protocol on stdin/stdout.

That separation is the whole point:

- **The harness is a black box.** No imports, no shared state — driven
  only through its documented protocol. The two repos version and deploy
  independently; the harness just needs to be on `PATH`.
- **The trust boundary is a process, not a function call.** Untrusted
  agent code never runs in the web process. In production it runs in an
  isolated container (see [Sandbox isolation](#sandbox-isolation)).

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ Browser (views/index.html)                                           │
│   chat UI · file upload · task cards · SSE event stream              │
└───────────────┬──────────────────────────────────────▲───────────────┘
                │ REST (JSON, Bearer JWT)              │ SSE (text/event-stream)
                │ POST /conversations/{id}/files       │ GET  /conversations/{id}/runs/{rid}/stream
                │ POST /conversations/{id}/runs        │ start/delta/notify/log/result
                │ GET  /conversations/{id}/artifacts   │
┌───────────────▼──────────────────────────────────────┴───────────────┐
│ FastAPI app  (TRUSTED)                                               │
│                                                                      │
│  routes/        HTTP only: dependencies, status codes, schemas       │
│                 auth · conversations(+files, runs, artifacts)        │
│                 billing · secrets                                    │
│  controllers/   all business logic                                   │
│    domain       accounts · conversations · files · runs              │
│                 secrets · usage  (rules, persistence, refusals)      │
│    machinery    manager.py  run lifecycle, event fan-out (subscribe) │
│                 protocol.py JSONL line parsing                       │
│                 runner.py   ServerRunner: one resident harness       │
│                             process per conversation                 │
│  infra/         config (PAW_* env) · db (engine + migrations) ·      │
│                 security (JWT, Fernet)                               │
│  models/        ORM entities: users, conversations, runs,            │
│                 files, usage ledger, secrets, sandboxes              │
└───────────────┬──────────────────────────────────────────────────────┘
                │ stdin/stdout pipes — bidirectional JSONL
                │ host→agent: {"op": submit|answer|cancel|ping|shutdown}
                │ agent→host: ready / start / delta / notify / log / result
                │
┌───────────────▼──────────────────────────────────────────────────────┐
│ python-agent-harness serve  (UNTRUSTED, black box — never imported)  │
│   cwd = workspaces/<conversation_id>/                                │
│   tools, bash, pandas/openpyxl …                                     │
└───────────────┬──────────────────────────────────────────────────────┘
                │ reads uploads / writes outputs
┌───────────────▼──────────────────────────────────────────────────────┐
│ workspaces/<conversation_id>/                                        │
│   a1b2c3d4_sales.xlsx   ← user upload (hex-prefixed)                 │
│   final.xlsx            ← agent output (downloadable via /artifacts) │
└──────────────────────────────────────────────────────────────────────┘
```

**Two protocols, two links:**

| Link | Transport | Direction |
|---|---|---|
| Backend ↔ harness | JSON-lines over stdin/stdout pipes | ops down, events up |
| Browser ↔ backend | Server-Sent Events (SSE) | events streamed to the UI |

The controller is a **protocol translator**: it parses each JSONL line
from the harness, fans it out to in-memory subscribers, and re-emits it
as an SSE `data:` frame. The browser therefore sees the same events the
harness's own TUI would render — tool progress, todos, errors, and
mid-run questions.

---

## Quick start

```bash
python -m venv venv && . venv/bin/activate
pip install -e ".[dev]"
uvicorn app.main:app --reload    # http://127.0.0.1:8000  (UI served at /)
```

- **Harness:** `python-agent-harness` must be a command on `PATH`. If it
  isn't, point `PAW_HARNESS__CMD` at the absolute binary.
- **Auth:** register a user at `/auth/register`, then log in — the UI
  does both for you.
- **Schema:** created automatically on first startup (see
  [Database & migrations](#database--migrations)); no manual step for dev.

Optional dependency groups:

```bash
pip install -e ".[postgres]"   # psycopg driver for Postgres
pip install -e ".[s3]"         # boto3 for S3 object storage
pip install -e ".[redis]"      # redis for shared rate limiting
```

---

## How a run works

One conversation turn is one run:

1. The browser `POST`s a prompt to `/conversations/{id}/runs`.
2. The controller creates a `Run` row (`status=running`), ensures a
   sandbox exists, and starts the harness exec on a worker thread.
3. The prompt is augmented with the conversation's uploaded filenames
   and sent to the harness as an `op:submit`.
4. The harness streams `start` / `delta` / `notify` / `log` events; the
   controller fans them out to any SSE subscribers on
   `/conversations/{id}/runs/{rid}/stream`.
5. A terminal `result` line finalizes the run: answer, token usage, and
   status land in the database; agent-produced files are synced to
   durable storage.

**One running run per conversation** is enforced by a partial unique
index in the database, so the guarantee holds even across processes.

---

## The `serve` protocol

One sandbox = one long-lived `serve` process. The web side writes ops;
the harness answers with events.

```
host → harness: {"op": "submit", "prompt": ..., "run_id": ...}
                {"op": "answer", "run_id": ..., "answers": [...]}
                {"op": "cancel", "run_id": ...} / {"op": "ping"} / {"op": "shutdown"}

harness → host: {"type": "ready"}                 (once, on startup)
                start / delta / notify / log       (per run, streamed)
                {"type": "result", "answer": ..., "usage": ..., "cancelled": ...}
```

Because the process is **resident** (survives between turns):

- **Multi-turn memory** — conversation history persists across turns.
- **No per-turn spawn** — the interpreter/harness stays warm.
- **Mid-run Q&A** — `answer` delivers the user's reply to a pending
  question (the agent's Question tool / plan-exit confirm).
- **Protocol-level cancel** — `cancel` is a message, not a signal.

### `notify` event kinds

`notify` lines carry the progress the UI renders, keyed by `kind`:

| kind | meaning |
|---|---|
| `tool_start` | the round's tool names |
| `tool_calls` | the same round with each call's arguments (e.g. `Bash(command='ls -la')`) |
| `tool_running`, `tool` | per-tool progress and completion |
| `todos` | the agent's task list |
| `compact`, `retry`, `error` | context compaction, retries, errors |
| `ask` | a mid-run question awaiting `answer` |

`tool_calls` is additive: a harness that doesn't emit it degrades
gracefully to the bare names from `tool_start`. `log` lines are shown
too, except internal session bookkeeping (the generated title).

---

## Sandbox isolation

The runner is selected by `PAW_RUNNER`:

- **`server`** (default) — one resident `harness serve` **process** per
  sandbox. Fast and simple; suitable for a trusted single-node
  deployment. The agent shares the host environment.
- **`docker`** — one resident **container** per sandbox, same protocol
  over the container's stdio. This is the real trust boundary for
  untrusted/public users:

  - **no host environment** — the container gets only
    `PAW_NONINTERACTIVE` plus the user's own decrypted secrets;
  - **no host filesystem** — only the conversation's workspace is mounted;
  - **no network** by default (`PAW_DOCKER__NETWORK=none`);
  - **locked down** — read-only rootfs, dropped Linux capabilities,
    non-root user, `no-new-privileges`, and memory/CPU/PID ceilings.

Idle sandboxes are reaped after `PAW_SANDBOX__TTL_SECONDS`.

**Secret injection.** User secrets are Fernet-encrypted at rest and
never returned by the API. For the `docker` runner they are decrypted
**host-side** (the key never enters the sandbox) and injected as scoped
environment variables into that user's container only.

---

## Storage & durability

Two tiers keep the agent's filesystem contract while surviving pod loss:

- **Sandbox-facing tier** — the per-conversation workspace
  (`workspaces/<id>/`) is the harness's working directory, so it can open
  uploads by name and write outputs directly.
- **Durable tier** — a blob store (`PAW_STORAGE__BACKEND`: `local` or
  `s3`) is the system of record. Uploads are written through to it on
  arrival; agent outputs are synced to it when a run finishes. If a
  workspace is lost (pod restart, a different instance), files are
  rehydrated from the durable tier on demand.

Uploads are validated and quota-limited: a per-file size cap
(`PAW_MAX_UPLOAD_BYTES`), a per-user total quota
(`PAW_MAX_USER_STORAGE_BYTES`), and an optional magic-byte type
allow-list (`PAW_UPLOAD_ALLOWED_TYPES`).

---

## Billing, budgets & rate limits

- **Usage ledger.** On each run's `result`, the controller records token
  usage (input/output/rounds) into `usage_events`, attributed to the
  user and conversation.
- **Budget kill-switch.** With `PAW_BUDGET_ENFORCE=true`, a run is
  refused (HTTP 402) when the user's consumed tokens reach their
  allowance — a free-tier grant plus the token value of purchased
  account points.
- **Rate limiting.** A sliding-window limiter throttles the run endpoint
  per user and login/register per IP (429 + `Retry-After`). The backend
  is `memory` (per-instance) or `redis` (shared/global across instances).
- **Token revocation.** JWTs carry a `ver` claim matched against the
  user's `token_version`; logout and password change bump it, instantly
  invalidating every outstanding token.

---

## Observability

- **Request correlation.** Every request gets an `X-Request-ID` (echoed
  if the client sent one), bound to a contextvar so every `paw.*` log
  line carries it. Unhandled errors are logged with a traceback and
  returned as a 500 quoting the id. `PAW_LOG_JSON=true` emits one JSON
  object per line for log shippers.
- **Metrics.** Prometheus text at `GET /metrics` — HTTP requests
  (by method/status), request latency, runs by terminal state, run
  duration, and tokens consumed.
- **Tracing.** Spans are marked in the code and become real spans once a
  tracer is registered via `infra.metrics.set_tracer` (a no-op otherwise).

**Graceful shutdown.** On `SIGTERM` the app stops accepting new runs and
drains in-flight ones for up to `PAW_SHUTDOWN_DRAIN_SECONDS`. Runs still
running at the deadline are reconciled to `error` on the next startup —
scoped to this instance's id (`PAW_INSTANCE_ID`), so a restart never
disturbs another instance's live runs.

---

## Database & migrations

The schema is owned by Alembic (`migrations/`). Startup runs
`upgrade_to_head()` automatically, so a fresh `uvicorn` run creates the
schema and stamps `alembic_version` — no manual step for dev. The test
suite migrates each throwaway database the same way, so tests always run
against the real migrations.

After changing a model:

```bash
alembic revision --autogenerate -m "describe the change"   # writes migrations/versions/*
alembic upgrade head                                        # apply it
alembic check                                               # models ⇆ migrations in sync
```

`alembic check` reporting *"No new upgrade operations detected"* means
the migrations match the models. SQLite uses batch mode for ALTERs; the
migration URL comes from `PAW_DB_URL` (see `migrations/env.py`).

---

## Configuration

All settings are environment variables with the `PAW_` prefix. Nested
groups use a double underscore (e.g. `PAW_HARNESS__CMD`).

### Core

| var | default | note |
|---|---|---|
| `PAW_DB_URL` | `sqlite:///./paw.db` | any SQLAlchemy URL (Postgres: `postgresql+psycopg://…`) |
| `PAW_SECRET_KEY` | dev default | **set in production** |
| `PAW_ACCESS_TOKEN_MINUTES` | `30` | JWT access TTL |
| `PAW_REFRESH_TOKEN_DAYS` | `14` | JWT refresh TTL |
| `PAW_LOG_JSON` | `false` | emit JSON log lines instead of text |

### Database pool (server DBs; ignored for SQLite)

| var | default | note |
|---|---|---|
| `PAW_DB_POOL_SIZE` | `5` | connection pool size |
| `PAW_DB_MAX_OVERFLOW` | `10` | extra connections beyond the pool under load |
| `PAW_DB_POOL_RECYCLE_S` | `1800` | recycle a pooled connection after N seconds |

### Harness & sandbox

| var | default | note |
|---|---|---|
| `PAW_HARNESS__CMD` | `python-agent-harness` | harness binary |
| `PAW_HARNESS__CWD` | `""` | override the agent workspace dir per sandbox |
| `PAW_HARNESS__TIMEOUT` | unset | host-side wall-clock budget for one run |
| `PAW_WORKSPACE_ROOT` | `./workspaces` | base dir for per-conversation workspaces |
| `PAW_RUNNER` | `server` | `server` (host subprocess) or `docker` (isolated container) |
| `PAW_SANDBOX__TTL_SECONDS` | `300` | idle sandbox reaper TTL |

### Docker runner

| var | default | note |
|---|---|---|
| `PAW_DOCKER__IMAGE` | `python-agent-harness:latest` | sandbox image; pin by digest in prod |
| `PAW_DOCKER__NETWORK` | `none` | container network mode; `none` = no egress |
| `PAW_DOCKER__MEM_LIMIT` | `1g` | per-container memory ceiling |

### Storage

| var | default | note |
|---|---|---|
| `PAW_STORAGE__BACKEND` | `local` | durable blob store: `local` or `s3` |
| `PAW_STORAGE__LOCAL_ROOT` | `./storage` | root dir for the `local` backend |
| `PAW_STORAGE__S3_BUCKET` | `""` | bucket for the `s3` backend |
| `PAW_STORAGE__S3_ENDPOINT_URL` | `""` | custom S3 endpoint (MinIO/localstack); empty = real AWS |

### Uploads & quotas

| var | default | note |
|---|---|---|
| `PAW_MAX_UPLOAD_BYTES` | `104857600` | per-file upload size cap (100 MB) |
| `PAW_MAX_USER_STORAGE_BYTES` | `1073741824` | per-user total storage quota (1 GB); `0` = unlimited |
| `PAW_UPLOAD_ALLOWED_TYPES` | `[]` | magic-byte allow-list (e.g. `["xlsx","csv","pdf"]`); empty = any type |

### Billing & rate limits

| var | default | note |
|---|---|---|
| `PAW_BUDGET_ENFORCE` | `false` | enforce a per-user token budget before a run (spend kill-switch) |
| `PAW_BUDGET_FREE_TOKENS` | `1000000` | free-tier token allowance per user |
| `PAW_BUDGET_TOKENS_PER_POINT` | `1000` | token value of one account point (adds to the budget) |
| `PAW_RATE_LIMIT_BACKEND` | `memory` | `memory` (per-instance) or `redis` (shared across instances) |
| `PAW_RATE_LIMIT_REDIS_URL` | `""` | Redis URL for the `redis` backend; empty = localhost default |

### Operations

| var | default | note |
|---|---|---|
| `PAW_SHUTDOWN_DRAIN_SECONDS` | `25` | on SIGTERM, seconds to let in-flight runs finish before exit |
| `PAW_INSTANCE_ID` | host+pid | this instance's id (set to pod/task name); scopes run reconciliation |

---

## Design principles

**Routes are thin; controllers own the rules.** A route resolves
dependencies, calls a controller, and maps the result to a response
schema — nothing else. No route touches the DB, the filesystem, or
crypto. A controller refuses work by raising a typed error from
`controllers/errors.py` (`NotFound`, `Conflict`, `InvalidRequest`,
`PayloadTooLarge`, `PaymentRequired`, `Unauthorized`, `Forbidden`,
`TooManyRequests`); a single handler in `main.py` renders it as
FastAPI's `{"detail": ...}` shape with the right status. Business logic
never imports `HTTPException`, so a controller stays callable from a
test, a CLI, or a worker thread.

**`infra` is stateless mechanism; `controllers` is stateful policy.**
The line between the two layers is not "core vs supporting." `infra`
holds primitives with no entities and no session — password hashing,
JWT, Fernet encrypt/decrypt, storage backends, rate limiters, metrics —
and imports nothing but `infra`. Anything that takes a `Session`, reads
or writes an ORM entity, or can refuse a request lives in `controllers`.
Keeping that direction means the bottom layer never imports `models` or
`controllers.errors`.

**The harness is decoupled.** It is a black-box binary driven only by
its JSONL protocol (`seq` for ordering, `run_id` for correlation,
`usage` for billing). No imports, no shared state — the web side is
versioned and deployed independently.

**Runs are protocol turns, not process lifecycles.** One conversation
turn = one `op:submit` = one `Run` row. The resident process survives
the run and serves the next turn. Events stream to subscribers exactly
as the harness emitted them (plus run-lifecycle events), and the
`result` line is what lands in the database.
