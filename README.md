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
│                 runner.py   ResidentRunner: the whole sandbox        │
│                             lifecycle, once; Server/Docker supply    │
│                             only a SandboxTransport                  │
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

Each SSE frame carries the harness `seq` as its event id, so a
reconnecting client sends `Last-Event-ID` (or `?last_event_id=`) and
resumes from there instead of replaying the run. Lifecycle events have
no `seq` and are always delivered, so a resume can never skip the
terminal event and leave a client hanging.

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
                {"op": "answer", "run_id": ..., "answers": [...], "ask_id": ...}
                {"op": "cancel", "run_id": ...} / {"op": "ping"} / {"op": "shutdown"}
                {"op": "hello", "protocol_versions": [...]}
                ... each op carries an "op_id", echoed on what it causes

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
too, except internal session bookkeeping (the generated title). A
`kind` this build does not know is still relayed, flagged
`unknown_kind` so a client can decline to render it.

### What the driver validates

The protocol's own guarantees are checked rather than assumed:

| field | check |
|---|---|
| `protocol_version` (on `ready`) | refused unless supported — loudly, once per run, instead of misparsing every line |
| `capabilities` (on `ready`) | recorded and logged; features degrade gracefully, so nothing is gated on them |
| `run_id` | a line labelled with another run is dropped, not fanned out under this one |
| `seq` | monotonicity checked per run; gaps and reorders counted (`paw_protocol_seq_anomalies_total`) |
| `errors[].code` | kept beside the text and persisted as `run.error_code`, so callers branch on a code, not on prose |
| `op_id` (on `error`) | echoed back by the harness, so a refusal names the op it refused instead of arriving unattributed |
| `notify` payloads | size-capped before relay (`PAW_MAX_EVENT_BYTES`), type-preserving |

A protocol `error` line (a rejected `answer`, a stale `ask_id`) is kept
in its own trail: it stays visible, but it does not by itself mark an
otherwise successful run as failed.

Negotiation runs both ways: `ready` tells the host what the harness
speaks, and `hello` tells the harness what the host can parse, so a
version mismatch is settled before any run. Both are sent only to a
build advertising the matching capability (`hello`, `op_id`) — an older
harness answers an unknown op with an `error` line, so the host does
not speak features the sandbox never claimed.

A warm sandbox is probed with `ping` before reuse
(`PAW_SANDBOX__PROBE_TIMEOUT`). A resident process that is alive but no
longer reading its stdin is invisible to an exit-status check, and
submitting to it would hang for the host timeout; the probe turns that
into a respawn. Teardown sends `shutdown` before closing stdin, so an
active run can still emit its terminal `result`.

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

Idle sandboxes are reaped after `PAW_SANDBOX__TTL_SECONDS` by a
background sweep (`PAW_SANDBOX__REAP_INTERVAL_SECONDS`). The sweep
skips any sandbox with a live run, so the TTL bounds idleness and never
shortens a run — `last_used` is only touched when a run starts, so a
long run looks increasingly idle while it is working. The idle/stale
decision is made inside the same lock that removes the sandbox, so a
run claiming a stale sandbox cannot lose it to a concurrent sweep.

**Sandboxes are per-instance.** A sandbox is a process (or container)
held by one web instance, so its registry row carries an
`owner_instance` like runs do. A restart retires its own rows at
startup; a row whose runner no longer holds it is retired lazily on the
next run. If a conversation is routed to a *different* instance, that
instance cannot reach the resident process holding the history, so it
rebuilds — the agent's context restarts. That is logged and counted
(`paw_sandbox_instance_migrations_total`), and the fix is session
affinity: route a conversation to the same instance.

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
| `PAW_HARNESS__MAX_ROUNDS` | unset | cap each run at N LLM rounds (`serve --max-rounds`); unset = unlimited |
| `PAW_HARNESS__SANDBOX_TIMEOUT` | unset | per-run wall-clock budget enforced *inside* the sandbox (`serve --timeout`); unset = unlimited |
| `PAW_HARNESS__ANSWER_TIMEOUT` | unset | how long the sandbox waits for an answer to a mid-run question; unset = wait forever |
| `PAW_WORKSPACE_ROOT` | `./workspaces` | base dir for per-conversation workspaces |
| `PAW_RUNNER` | `server` | `server` (host subprocess) or `docker` (isolated container) |
| `PAW_SANDBOX__TTL_SECONDS` | `300` | idle sandbox reaper TTL (never applied to a sandbox with a live run) |
| `PAW_SANDBOX__REAP_INTERVAL_SECONDS` | `60` | how often the background reaper sweeps; `0` disables it |
| `PAW_SANDBOX__PROBE_TIMEOUT` | `5` | liveness `ping` before reusing a warm sandbox; `0` disables it |

The three budget vars are spawn-time arguments to `harness serve`, not
fields on the submit op: the harness refuses to take a budget off the
wire so a run cannot raise its own ceiling. Each is omitted from the
command line entirely when unset, so the default is an unbounded run.

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
| `PAW_CANCEL_GRACE_SECONDS` | `10` | after a *user* cancels, how long to wait for the harness to honour it before destroying the sandbox; `0` disables escalation |
| `PAW_MAX_EVENT_BYTES` | `65536` | per-event cap on relayed harness payloads; `0` disables it |
| `PAW_MAX_LINE_BYTES` | `4194304` | longest single JSONL line accepted from a sandbox; oversize lines are discarded whole |
| `PAW_MAX_RUN_STDOUT_BYTES` | `8388608` | cap on the raw transcript an exec keeps in memory; past it only outcome-bearing lines are retained |
| `PAW_MAX_STORED_EVENTS` | `2000` | cap on events persisted with a finished run (deltas are merged first) |
| `PAW_MAX_SUBSCRIBER_QUEUE` | `1000` | per-SSE-subscriber queue depth; when full the oldest event is dropped |
| `PAW_INSTANCE_ID` | host+pid | this instance's id (set to pod/task name); scopes run reconciliation |

`PAW_CANCEL_GRACE_SECONDS` is a recovery path, not a run budget: it is
armed only by an explicit cancel request, never by a timer. The
protocol `cancel` is cooperative, so a run wedged in an uninterruptible
operation never reaches the check — and while its row stays `running`
the partial unique index locks that conversation out of new runs.
Escalation closes the sandbox so the run's own worker can finalize it.

---

### Bounded by construction

Everything the sandbox emits is untrusted input, so each hop has a
ceiling rather than growing with whatever the agent decides to print:

| hop | bound |
|---|---|
| one line off the pipe | `PAW_MAX_LINE_BYTES` — an oversize line is drained to its newline and dropped, so its tail is never reparsed as fresh lines |
| one relayed event | `PAW_MAX_EVENT_BYTES` — oversize bodies are replaced, type-preserving, envelope untouched |
| the exec's in-memory transcript | `PAW_MAX_RUN_STDOUT_BYTES` — past it only `result`/`error`/error-and-usage `notify` lines are kept, so the verdict and token counts can never be dropped |
| a subscriber's backlog | `PAW_MAX_SUBSCRIBER_QUEUE` — oldest-first drop keeps a live view current; the `id:` gap tells the client to resume. The terminal event is never dropped |
| the stored transcript | `PAW_MAX_STORED_EVENTS` — consecutive deltas are merged (lossless for rendering), then the middle is elided with a marker |

Deltas are token-sized, and the transcript used to keep one row per
token in a JSON column, so a long answer grew the row without bound.
Merging them is lossless because any client renders deltas by
concatenation.

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

**One resident lifecycle, two transports.** `ServerRunner` and
`DockerRunner` differ only in what hosts the harness: a host
subprocess or a container. That difference is confined to
`SandboxTransport` (spawn, read a line, write a line, check alive,
interrupt a blocked read, tear down). The registry and its locking,
the ready handshake, version negotiation, liveness probing, the idle
reaper, cancel/answer, the exec lifecycle and the event pump live once
in `ResidentRunner`.

They used to live twice, and the two copies drifted five separate
times — `destroy` skipping teardown in one of them, a respawn
re-registering onto a swept sandbox, `hello` never sent, a warm
sandbox never probed, and two different answers for a create-time
spawn failure. Every one was found by reading the pair side by side
rather than by a test. Sharing the logic is what makes that class of
bug unrepresentable.

**The harness is decoupled.** It is a black-box binary driven only by
its JSONL protocol (`seq` for ordering, `run_id` for correlation,
`usage` for billing). No imports, no shared state — the web side is
versioned and deployed independently.

**Runs are protocol turns, not process lifecycles.** One conversation
turn = one `op:submit` = one `Run` row. The resident process survives
the run and serves the next turn. Events stream to subscribers exactly
as the harness emitted them (plus run-lifecycle events), and the
`result` line is what lands in the database.
