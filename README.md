<div align="center">

# python-agent-web

**A minimal Web interface for validating agent runtime and interaction patterns.**

</div>

The trusted side of an agent platform: API server, controller, auth,
billing, secrets.  The untrusted side — the agent runtime itself — is
`python-agent-harness`, executed as a **resident subprocess** per
sandbox: `python-agent-harness serve`, a bidirectional JSON-lines
protocol over stdin/stdout.  The harness is never imported; the repos
stay decoupled (the harness only needs to be on PATH of whatever runs
the agent).

```
Coding Space Server
        │
 ┌──────▼─────────┐
 │ Agent Controller│   app.controller
 └──────┬─────────┘
        │ resident subprocess: python-agent-harness serve (JSONL over pipes)
 ┌──────▼─────────┐
 │    Sandbox      │   app.controller.runner (ServerRunner now, Docker later)
 │  agent process  │
 │  tools, bash, … │
 └─────────────────┘

TRUSTED                    UNTRUSTED / ISOLATED
─────────────              ─────────────────────
FastAPI app                harness agent process
Controller                 agent-generated commands
Auth (JWT)                 repository, builds
Billing (usage ledger)
Secrets (Fernet)
Runner (sandbox manager contract; ServerRunner today, Docker later)
```

## Layout

```
app/
  main.py          FastAPI app factory + routers + static UI
  core/
    config.py      Settings (pydantic-settings, env prefix PAW_)
    security.py    JWT auth (access/refresh) + password hashing
  db.py            SQLite engine/session (SQLAlchemy ORM)
  models.py        User, Conversation, Run, UsageEvent, Secret
  schemas.py       Pydantic request/response models
  routes/
    auth.py        POST /auth/register /auth/login /auth/refresh /auth/me
    conversations.py  CRUD + POST /{id}/files (upload) + POST /{id}/runs (start)
                      + POST /{id}/answer + GET /{id}/stream (SSE)
    billing.py     usage summary (token ledger)
    secrets.py     CRUD (write-only read: value never returned)
  controller/
    protocol.py    Parse harness event lines (seq/run_id/result/usage)
    manager.py     Controller: start_run, event pump, subscribe, cancel, answer
    runner.py      Runner protocol + ServerRunner (resident serve process)
  static/index.html  Minimal chat UI (EventSource -> runs, fetch -> API)
```

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
uvicorn app.main:app --reload    # http://127.0.0.1:8000 (UI at /)
```

`python-agent-harness` must be importable-on-PATH as a command; point
`PAW_HARNESS__CMD` at the absolute binary if it is not.  Auth: create
a user via `/auth/register`, then log in; the UI does this for you.

## The serve protocol (harness side)

One sandbox = one long-lived `serve` process.  The web side writes
ops, the harness answers with events:

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
Question tool / PlanExit confirm), and cancel is a protocol message —
no signal semantics.

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

- **Decoupling**: the harness is a black-box binary driven by its
  documented JSONL protocols (the same `start`/`delta`/`notify`/
  `log`/`result` line shapes on the resident `serve` pipe and the
  one-shot `headless --json` pipe; `seq` for ordering, `run_id` for
  correlation, `usage` for billing).  No imports, no shared state; the
  web side can be versioned and deployed independently.
- **Runs are protocol turns, not process lifecycles**: one
  conversation turn = one `op:submit` = one `Run` row; the resident
  process survives the run and serves the next turn.  Events stream to
  subscribers over SSE exactly as the harness emitted them (plus run
  lifecycle events), and the `result` line lands in the DB.
- **Secrets** are Fernet-encrypted at rest and never returned by the
  API; they are meant to be injected into the sandbox environment by
  the sandbox manager (not exposed to agents via the API).
- **Billing** is a token ledger: the controller snapshots
  `result.usage` (input/output/rounds) from the harness into
  `usage_events`, attributed to the user and conversation.
