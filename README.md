<div align="center">

# python-agent-web

**A minimal Web interface for validating agent runtime and interaction patterns.**

</div>

The trusted side of an agent platform: API server, controller, auth,
billing, secrets.

The untrusted side — the agent runtime itself — is
`python-agent-harness`, executed as a **resident subprocess** per
sandbox: `python-agent-harness serve`, a bidirectional JSON-lines
protocol over stdin/stdout.

The harness is never imported; the repos stay decoupled (the harness
only needs to be on PATH of whatever runs the agent).

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

Protocol split: **JSONL** is the backend↔harness link (pipes); **SSE**
is the browser↔backend link.

The controller is a protocol translator — each parsed JSONL line is
fanned out to in-memory subscribers and re-emitted as an SSE `data:`
frame, so the browser sees the same events the harness TUI renders (tool
progress, todos, errors, mid-run questions).

## Quick start

```bash
python -m venv venv && . venv/bin/activate
pip install -e ".[dev]"
uvicorn app.main:app --reload    # http://127.0.0.1:8000 (UI at /)
```

`python-agent-harness` must be importable-on-PATH as a command; point
`PAW_HARNESS__CMD` at the absolute binary if it is not.

Auth: create a user via `/auth/register`, then log in; the UI does this
for you.

### Database & migrations

The schema is owned by Alembic (`migrations/`). Startup runs
`upgrade_to_head()` automatically, so a fresh `uvicorn` run creates the
schema and stamps `alembic_version` — no manual step for dev. Tests
migrate each throwaway database the same way, so the whole suite runs
against real migrations.

After changing a model, generate and review a migration:

```bash
alembic revision --autogenerate -m "describe the change"   # writes migrations/versions/*
alembic upgrade head                                        # apply it
alembic check                                               # models ⇆ migrations in sync
```

`alembic check` reporting "No new upgrade operations detected" means the
migrations match the models. SQLite uses batch mode for ALTERs; the
migration URL comes from `PAW_DB_URL` (see `migrations/env.py`).

## The serve protocol (harness side)

One sandbox = one long-lived `serve` process.
The web side writes ops, the harness answers with events:

```
host → harness: {"op": "submit", "prompt": ..., "run_id": ...}
                {"op": "answer", "run_id": ..., "answers": [...]}
                {"op": "cancel", "run_id": ...} / {"op": "ping"} / {"op": "shutdown"}
harness → host: {"type": "ready"} then per-run
                start/delta/notify/log lines and a terminal
                {"type": "result", "answer": ..., "usage": ..., "cancelled": ...}
```

Because the process is resident: conversation history persists across
turns (multi-turn memory), no per-turn interpreter spawn, `answer`
delivers the user's reply to a pending mid-run question (the agent's
Question tool / PlanExit confirm), and cancel is a protocol message — no
signal semantics.

`notify` lines carry the progress the UI renders, keyed by `kind`:
`tool_start` (the round's tool names), `tool_calls` (the same round with
each call's arguments, so a row reads `Bash(command='ls -la')` rather
than a bare `Bash`), `tool_running`, `tool`, `todos`, `compact`,
`retry`, `error`, and `ask` for a mid-run question.

`tool_calls` is additive — a harness that does not send it degrades to
the names from `tool_start`.

`log` lines are shown too, except session bookkeeping (the generated
session title), which says nothing about what the agent is doing.

## Configuration (env, prefix `PAW_`)

| var | default | note |
|---|---|---|
| `PAW_DB_URL` | `sqlite:///./paw.db` | any SQLAlchemy URL |
| `PAW_SECRET_KEY` | dev default | **set in production** |
| `PAW_ACCESS_TOKEN_MINUTES` | `30` | JWT access TTL |
| `PAW_REFRESH_TOKEN_DAYS` | `14` | JWT refresh TTL |
| `PAW_HARNESS__CMD` | `python-agent-harness` | harness binary |
| `PAW_HARNESS__CWD` | `""` | agent workspace dir per sandbox (overrides per-conversation workspaces) |
| `PAW_WORKSPACE_ROOT` | `./workspaces` | base dir for per-conversation workspaces; uploads land in `<root>/<conversation_id>/` |
| `PAW_HARNESS__TIMEOUT` | unset | host-side wall-clock budget for one run |
| `PAW_RUNNER` | `server` | `server` only; docker later |
| `PAW_SANDBOX__TTL_SECONDS` | `300` | idle reaper TTL |

## Design notes

- **Routes are thin, controllers own the rules**: a route resolves
  dependencies, calls a controller, and maps the result to a response
  schema — nothing else.
  No route touches the DB, the filesystem or crypto.
  A controller refuses work by raising from `controllers/errors.py`
  (`NotFound`, `Conflict`, `InvalidRequest`, `PayloadTooLarge`,
  `Unauthorized`, `Forbidden`); one handler in `main.py` renders that as
  FastAPI's own `{"detail": ...}` shape with the status the error type
  carries.
  So business logic never imports `HTTPException`, and a controller
  stays callable from a test, a CLI or a worker thread.
- **Stateless mechanism vs stateful policy** is the
  `infra`/`controllers` line, not "core vs supporting".
  `infra` holds primitives with no entities and no session — password
  hashing, JWT, Fernet `encrypt`/`decrypt` — and imports nothing but
  `infra`.
  Anything that takes a `Session`, reads or writes an ORM entity, or can
  refuse a request lives in `controllers`, even for supporting areas
  like accounts and secrets.
  Moving those down would make the bottom layer import `models` and
  `controllers.errors`, inverting the dependency direction.
- **Decoupling**: the harness is a black-box binary driven by its
  documented JSONL protocols (the same `start`/`delta`/`notify`/
  `log`/`result` line shapes on the resident `serve` pipe and the
  one-shot `headless --json` pipe; `seq` for ordering, `run_id` for
  correlation, `usage` for billing).
  No imports, no shared state; the web side can be versioned and
  deployed independently.
- **Runs are protocol turns, not process lifecycles**: one conversation
  turn = one `op:submit` = one `Run` row; the resident process survives
  the run and serves the next turn.
  Events stream to subscribers over SSE exactly as the harness emitted
  them (plus run lifecycle events), and the `result` line lands in the
  DB.
- **Secrets** are Fernet-encrypted at rest and never returned by the
  API; they are meant to be injected into the sandbox environment by the
  sandbox manager (not exposed to agents via the API).
- **Billing** is a token ledger: the controller snapshots `result.usage`
  (input/output/rounds) from the harness into `usage_events`, attributed
  to the user and conversation.
- **Observability**: every request gets an `X-Request-ID` (echoed if the
  client sent one), bound to a contextvar so every `paw.*` log line
  carries it; unhandled errors are logged with a traceback and returned
  as a 500 quoting the id. `PAW_LOG_JSON=true` switches to JSON lines.
- **Rate limiting**: a process-local sliding-window limiter throttles the
  run endpoint per user (each run spends tokens) and login/register per
  IP, returning 429 + `Retry-After`. Per-instance only — a multi-instance
  deploy would need a shared store.
- **Token revocation**: tokens carry a `ver` claim matched against the
  user's `token_version`; logout and password change bump it, so every
  outstanding token (all sessions) is rejected on next use.
