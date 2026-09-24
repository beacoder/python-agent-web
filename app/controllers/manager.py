"""Agent controller: owns run lifecycle and event fan-out.

One conversation turn = one ``Run`` row + one harness exec.  The
manager starts the exec on a worker thread, parses each stdout line
through the protocol module, fans events out to in-memory subscribers
(SSE handlers), and folds the ``result`` into the DB (answer, usage
ledger entry, status).  Subscribers attaching after a run started get
a replay of already-seen events first, so a reconnecting browser never
misses anything.
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..infra.config import get_settings
from ..infra.db import get_session_factory
from ..infra.logging import get_logger
from ..models import Conversation, Run, Sandbox, UsageEvent, new_id
from .protocol import RunOutcome, apply_event, parse_line
from .runner import Runner, SandboxNotFoundError, get_runner

_log = get_logger("run")


@dataclass
class ActiveRun:
    run_id: str
    sandbox_id: str
    subscribers: set[queue.Queue[dict[str, Any]]] = field(default_factory=set)
    seen: list[dict[str, Any]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


class Controller:
    """Coordinates sandboxes, harness execs, and subscribers."""

    def __init__(self, runner: Runner | None = None) -> None:
        self._runner = runner or get_runner()
        self._active: dict[str, ActiveRun] = {}
        self._lock = threading.Lock()

    # -- subscriptions ----------------------------------------------------

    def subscribe(self, run_id: str) -> queue.Queue[dict[str, Any]] | None:
        """Attach to a run; replay already-seen events into the queue."""
        with self._lock:
            active = self._active.get(run_id)
        if active is None:
            return None
        q: queue.Queue[dict[str, Any]] = queue.Queue()
        with active.lock:
            for event in active.seen:
                q.put(event)
            active.subscribers.add(q)
        return q

    def unsubscribe(self, run_id: str, q: queue.Queue[dict[str, Any]]) -> None:
        with self._lock:
            active = self._active.get(run_id)
        if active is not None:
            with active.lock:
                active.subscribers.discard(q)

    # -- run lifecycle ----------------------------------------------------

    def start_run(
        self,
        db: Session,
        *,
        user_id: str,
        conversation_id: str,
        prompt: str,
        harness_prompt: str | None = None,
    ) -> Run:
        """Create the Run row, exec the harness on a worker thread.

        ``prompt`` is stored verbatim on the Run row (the UI echoes it);
        ``harness_prompt`` is what the agent actually receives (the
        route augments it with file context).  Defaults to ``prompt``.
        """
        # Fast, friendly pre-check; the partial unique index on runs is
        # the authoritative guard (below) since this check-then-insert
        # otherwise races a concurrent submit.
        existing = (
            db.query(Run)
            .filter(Run.conversation_id == conversation_id, Run.status == "running")
            .one_or_none()
        )
        if existing is not None:
            raise RuntimeError("conversation already has a running run")

        sandbox = self._ensure_sandbox(db, user_id=user_id, conversation_id=conversation_id)
        run = Run(
            id=new_id("run"),
            conversation_id=conversation_id,
            prompt=prompt,
            status="running",
            harness_run_id="",
        )
        db.add(run)
        # Commit now: the worker thread finalizes the row from its own
        # session, so the "running" state must be durable before the
        # harness process is spawned.  A concurrent submit that won the
        # race trips the unique index here -> reject this one.
        try:
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            raise RuntimeError("conversation already has a running run") from exc
        run_id = run.id

        active = ActiveRun(run_id=run_id, sandbox_id=sandbox.id)
        with self._lock:
            self._active[run_id] = active

        def _worker() -> None:
            started = time.time()
            _log.info("run %s started (conversation %s)", run_id, conversation_id)
            try:
                result = self._runner.exec_run(
                    sandbox.id,
                    harness_prompt or prompt,
                    run_id,
                    on_line=lambda line: self._on_line(run_id, line),
                    # watchdog: without it a wedged harness (or a
                    # forever-pending ask) keeps the run "running" forever
                    timeout=get_settings().harness.timeout,
                )
            except SandboxNotFoundError as exc:
                _log.warning("run %s: sandbox gone: %s", run_id, exc)
                self._finish_run(run_id, RunOutcome(errors=[f"sandbox gone: {exc}"], duration_ms=0))
                return
            except Exception as exc:  # worker must always finish the run row
                _log.exception("run %s: runner error", run_id)
                self._finish_run(run_id, RunOutcome(errors=[f"runner error: {exc}"], duration_ms=0))
                return
            outcome = RunOutcome(exit_code=result.exit_code)
            for event in _events_of(result.stdout):
                apply_event(outcome, event)
            if result.timed_out:
                outcome.errors.append("runner timed out")
            if not outcome.saw_result and not outcome.errors:
                outcome.errors.append(f"harness exited without result (exit={result.exit_code})")
                outcome.errors.append(result.stderr.strip()[-2000:])
            outcome.duration_ms = int((time.time() - started) * 1000)
            self._finish_run(run_id, outcome)

        threading.Thread(target=_worker, name=f"run-{run_id}", daemon=True).start()
        return run

    def cancel_run(self, run_id: str) -> bool:
        """Best-effort graceful cancel: send the protocol cancel op to
        the resident harness process and mark the intent on the run
        row.  The run's own exit (result line with ``cancelled: true``,
        or error) finalizes the row."""
        with self._lock:
            active = self._active.get(run_id)
        if active is None:
            return False
        self._broadcast(run_id, {"type": "run", "state": "cancel_requested"})
        self._runner.cancel(active.sandbox_id, run_id)
        with get_session_factory()() as db:
            run = db.get(Run, run_id)
            if run is not None and run.status == "running":
                run.cancelled = True
                db.commit()
        return True

    def deliver_answer(self, run_id: str, answers: list[str]) -> bool:
        """Forward a user's answer to a pending mid-run question.

        Returns False when the run is not live (unknown/finished); the
        route maps that to 409.  The runner forwards the answer as an
        ``op:answer`` protocol message to the resident harness process.
        """
        with self._lock:
            active = self._active.get(run_id)
        if active is None:
            return False
        delivered = getattr(self._runner, "deliver_answer", None)
        if delivered is None:
            return False
        return bool(delivered(active.sandbox_id, run_id, answers))

    # -- internals ---------------------------------------------------------

    def _ensure_sandbox(self, db: Session, *, user_id: str, conversation_id: str) -> Sandbox:
        sandbox = (
            db.query(Sandbox)
            .filter(
                Sandbox.user_id == user_id,
                Sandbox.conversation_id == conversation_id,
                Sandbox.status == "running",
            )
            .one_or_none()
        )
        if sandbox is None:
            sandbox_id = self._runner.create(user_id, conversation_id)
            sandbox = Sandbox(
                id=sandbox_id,
                runner_id=get_settings().runner,
                user_id=user_id,
                conversation_id=conversation_id,
                status="running",
            )
            db.add(sandbox)
            db.flush()
        return sandbox

    def _on_line(self, run_id: str, line: str) -> None:
        event = parse_line(line)
        if event is None:
            return
        payload = {"type": event.type, "data": event.data, "malformed": event.malformed}
        if event.seq is not None:
            payload["seq"] = event.seq
        if event.run_id is not None:
            payload["run_id"] = event.run_id
        self._broadcast(run_id, payload)

    def _broadcast(self, run_id: str, payload: dict[str, Any]) -> None:
        with self._lock:
            active = self._active.get(run_id)
        if active is None:
            return
        with active.lock:
            active.seen.append(payload)
            subscribers = list(active.subscribers)
        for q in subscribers:
            q.put(payload)

    def _finish_run(self, run_id: str, outcome: RunOutcome) -> None:
        """Persist terminal state, then notify subscribers.

        Orchestration only: the persistence details live in
        ``_write_terminal_run`` below.  The subscriber notify/cleanup
        runs in a ``finally`` so a DB failure can't strand a waiting SSE
        client on a run that is really over.
        """
        final_state = run_status(outcome)
        events = self._events_snapshot(run_id)
        if outcome.errors:
            _log.warning("run %s finished: %s — %s", run_id, final_state, "; ".join(outcome.errors))
        else:
            _log.info("run %s finished: %s (%dms)", run_id, final_state, outcome.duration_ms)
        try:
            with get_session_factory()() as db:
                _write_terminal_run(db, run_id, outcome, final_state, events)
                db.commit()
        finally:
            self._close_active(run_id, final_state)

    def _events_snapshot(self, run_id: str) -> list[dict[str, Any]] | None:
        """A copy of the run's seen events, or None if it isn't active.

        Taken before the DB write so the stored ``run.events`` excludes
        the terminal ``run`` event (that one is appended at replay time).
        """
        with self._lock:
            active = self._active.get(run_id)
        if active is None:
            return None
        with active.lock:
            return list(active.seen)

    def _close_active(self, run_id: str, final_state: str) -> None:
        """Drop the active run and push the terminal event to subscribers."""
        with self._lock:
            active = self._active.pop(run_id, None)
        if active is None:
            return
        with active.lock:
            final = {"type": "run", "state": final_state}
            active.seen.append(final)
            subscribers = list(active.subscribers)
        for q in subscribers:
            q.put(final)

    # -- reaper ------------------------------------------------------------

    def reconcile_orphaned_runs(self) -> int:
        """Fail runs left ``running`` by a previous process (startup only).

        Runs execute on in-process daemon threads, so a restart abandons
        any in-flight run while its row still says ``running`` — stranding
        it forever and (via the partial unique index) blocking the
        conversation from starting a new one.  At startup ``_active`` is
        empty, so every ``running`` row is orphaned: mark them ``error``.
        Returns the number reconciled.
        """
        with get_session_factory()() as db:
            n = (
                db.query(Run)
                .filter(Run.status == "running")
                .update(
                    {
                        Run.status: "error",
                        Run.error: "interrupted by server restart",
                        Run.finished_at: _now(),
                    },
                    synchronize_session=False,
                )
            )
            db.commit()
        if n:
            _log.warning("reconciled %d orphaned run(s) to error on startup", n)
        return n

    def reap_idle_sandboxes(self) -> list[str]:
        destroyed = self._runner.reap_idle(get_settings().sandbox.ttl_seconds)
        if destroyed:
            factory = get_session_factory()
            db = factory()
            try:
                db.query(Sandbox).filter(Sandbox.id.in_(destroyed)).update(
                    {Sandbox.status: "destroyed"}, synchronize_session=False
                )
                db.commit()
            finally:
                db.close()
        return destroyed


def run_status(outcome: RunOutcome) -> str:
    return "cancelled" if outcome.cancelled else ("error" if outcome.errors else "done")


def _write_terminal_run(
    db: Session,
    run_id: str,
    outcome: RunOutcome,
    final_state: str,
    events: list[dict[str, Any]] | None,
) -> None:
    """Write the run's terminal state (and usage) into the DB.

    Pure persistence — no threads, no subscribers.  ``events`` is the
    seen-event snapshot to store, or None when the run wasn't active.
    """
    run = _resolve_run_row(db, run_id, final_state)
    run.status = final_state
    run.answer = outcome.answer
    run.error = "\n".join(outcome.errors)
    run.exit_code = outcome.exit_code
    run.cancelled = outcome.cancelled
    run.finished_at = _now()
    if outcome.duration_ms:
        run.duration_ms = outcome.duration_ms
    if events is not None:
        run.events = events
    if outcome.usage is not None:
        _record_usage(db, run, outcome)


def _resolve_run_row(db: Session, run_id: str, final_state: str, timeout_s: float = 10) -> Run:
    """Fetch the Run row, waiting briefly for the request thread's commit.

    The row is created on the request thread's session and committed
    when that request succeeds, so the worker can arrive here first.
    Retry briefly; past the deadline, synthesize a phantom row so a
    terminal state is never silently lost.
    """
    deadline = time.time() + timeout_s
    while True:
        run = db.get(Run, run_id)
        if run is not None:
            return run
        if time.time() >= deadline:
            run = Run(
                id=run_id,
                conversation_id=_phantom_conversation(db),
                prompt="",
                status=final_state,
                error="run row missing (request never committed?)",
            )
            db.add(run)
            return run
        db.rollback()
        time.sleep(0.02)


def _record_usage(db: Session, run: Run, outcome: RunOutcome) -> None:
    """Append a token-ledger row for the run, attributed to its owner."""
    usage = outcome.usage or {}
    owner = (
        db.query(Conversation.user_id).filter(Conversation.id == run.conversation_id).scalar() or ""
    )
    db.add(
        UsageEvent(
            id=new_id("use"),
            user_id=owner,
            conversation_id=run.conversation_id,
            run_id=run.id,
            input_tokens=int(usage.get("input", 0) or 0),
            output_tokens=int(usage.get("output", 0) or 0),
            rounds=int(usage.get("rounds", 0) or 0),
            model=str(outcome.model or ""),
        )
    )


def _phantom_conversation(db: Session) -> str:
    """Ensure a dummy conversation exists so a phantom run row can
    satisfy the FK constraint; returns its id."""
    conversation = db.get(Conversation, "cnv_orphaned")
    if conversation is None:
        conversation = Conversation(id="cnv_orphaned", user_id=_phantom_user(db))
        db.add(conversation)
        db.flush()
    return conversation.id


def _phantom_user(db: Session) -> str:
    from app.models import User

    user = db.get(User, "usr_orphaned")
    if user is None:
        user = User(id="usr_orphaned", email="orphaned@invalid", password_hash="")
        db.add(user)
        db.flush()
    return user.id


_controller: Controller | None = None
_controller_lock = threading.Lock()


def get_controller() -> Controller:
    """Process-wide controller singleton (FastAPI dependency)."""
    global _controller
    with _controller_lock:
        if _controller is None:
            _controller = Controller()
        return _controller


def _events_of(stdout: str):
    events = []
    for line in stdout.splitlines():
        event = parse_line(line)
        if event is not None:
            events.append(event)
    return events


def _now():
    from ..models import utcnow

    return utcnow()
