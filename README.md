<div align="center">

# python-agent-web

**Web/API platform for python-agent-harness.**

</div>

The trusted side of an agent platform: API server, controller, auth,
billing, secrets.  The untrusted side — the agent runtime itself — is
`python-agent-harness`, executed as a **subprocess** with
`python-agent-harness headless --json` and consumed as a JSON-lines
event stream.  The harness is never imported; the repos stay decoupled
(the harness only needs to be on PATH of whatever runs the agent).

```
Coding Space Server
        │
 ┌──────▼─────────┐
 │ Agent Controller│   app.controller
 └──────┬─────────┘
        │ subprocess: python-agent-harness headless --json
 ┌──────▼─────────┐
 │    Sandbox      │   app.controller.runner (LocalRunner now, Docker later)
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
Runner (sandbox manager contract; LocalRunner today, Docker later)
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
    conversations.py  CRUD + POST /{id}/runs (start) + GET /{id}/stream (SSE)
    billing.py     usage summary (token ledger)
    secrets.py     CRUD (write-only read: value never returned)
  controller/
    protocol.py    Parse harness --json lines (seq/run_id/result/usage)
    manager.py     Controller: start_run, event pump, subscribe, cancel
    runner.py      Runner protocol + LocalRunner (subprocess exec, cancel)
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

## Configuration (env, prefix `PAW_`)

| var | default | note |
|---|---|---|
| `PAW_DB_URL` | `sqlite:///./paw.db` | any SQLAlchemy URL |
| `PAW_SECRET_KEY` | dev default | **set in production** |
| `PAW_ACCESS_TOKEN_MINUTES` | `30` | JWT access TTL |
| `PAW_REFRESH_TOKEN_DAYS` | `14` | JWT refresh TTL |
| `PAW_HARNESS__CMD` | `python-agent-harness` | harness binary |
| `PAW_HARNESS__CWD` | `""` | agent workspace dir per run |
| `PAW_HARNESS__MAX_ROUNDS` | unset | round budget forwarded to `--max-rounds` |
| `PAW_HARNESS__TIMEOUT` | unset | wall-clock budget forwarded to `--timeout` |
| `PAW_RUNNER` | `local` | `local` only; docker later |
| `PAW_SANDBOX__TTL_SECONDS` | `300` | idle reaper TTL |

## Design notes

- **Decoupling**: the harness is a black-box binary driven by its
  documented `headless --json` protocol (`start`/`delta`/`notify`/
  `log`/`result` lines, `seq` for ordering, `run_id` for correlation,
  `usage` for billing).  No imports, no shared state; the web side
  can be versioned and deployed independently.
- **Runs are processes**: one conversation turn = one harness exec =
  one `Run` row; events stream to subscribers over SSE exactly as the
  harness emitted them (plus run lifecycle events), and the `result`
  line lands in the DB.
- **Secrets** are Fernet-encrypted at rest and never returned by the
  API; they are meant to be injected into the sandbox environment by
  the sandbox manager (not exposed to agents via the API).
- **Billing** is a token ledger: the controller snapshots
  `result.usage` (input/output/rounds) from the harness into
  `usage_events`, attributed to the user and conversation.
