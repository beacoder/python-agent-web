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
│  infra/         config (PAW_* env) · security (JWT, Fernet)          │
│  models/        SQLite (SQLAlchemy): users, conversations, runs,     │
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

## Layout

```
app/
  main.py          FastAPI app factory + routers + static UI
  models/          (model layer)
    __init__.py    ORM models: User, Conversation, ConversationFile, Run,
                   UsageEvent, Secret, Sandbox
    db.py          SQLite engine/session (SQLAlchemy ORM)
  validation/      (request/response validation)
    schemas.py     Pydantic request/response models
  routes/          (view layer, HTTP only — no business logic)
    auth.py        POST /auth/register /auth/login /auth/refresh /auth/me
    conversations.py  CRUD + POST /{id}/files (upload) + GET/DELETE
                      /{id}/artifacts (agent outputs) + POST /{id}/runs
                      (start) + POST /{id}/answer + GET /{id}/stream (SSE)
    billing.py     usage summary (token ledger)
    secrets.py     CRUD (write-only read: value never returned)
  controllers/     (all business logic)
    errors.py      DomainError hierarchy; main.py renders it as
                   {"detail": ...} so controllers never mention HTTP
    accounts.py    register/login rules, token identity, admin check
    conversations.py  ownership, CRUD, workspace teardown
    files.py       upload sanitizing + size cap, uploads-vs-artifacts,
                   traversal-safe artifact paths
    runs.py        harness prompt augmentation, run access, cancel/answer
    secrets.py     upsert + encryption-at-rest rules
    usage.py       token ledger aggregation
    protocol.py    Parse harness event lines (seq/run_id/result/usage)
    manager.py     Controller: start_run, event pump, subscribe, cancel, answer
    runner.py      Runner protocol + ServerRunner (resident serve process,
                   per-conversation workspace cwd)
  views/           (static UI)
    index.html     Chat UI: upload, task cards, SSE progress (tool status,
                   todos, ask/answer), artifact downloads
  infra/           (cross-cutting infrastructure)
    config.py      Settings (pydantic-settings, env prefix PAW_)
    security.py    JWT auth (access/refresh) + password hashing
    secrets_store.py  Fernet-encrypted secret values
```

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
