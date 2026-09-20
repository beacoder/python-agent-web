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

from sqlalchemy.orm import Session

from ..core.config import get_settings
from ..db import get_session_factory
from ..models import Conversation, Run, Sandbox, UsageEvent, new_id
from .protocol import RunOutcome, apply_event, parse_line
from .runner import Runner, SandboxNotFoundError, get_runner


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
    ) -> Run:
        """Create the Run row, exec the harness on a worker thread."""
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
        # harness process is spawned.
        db.commit()
        run_id = run.id

        active = ActiveRun(run_id=run_id, sandbox_id=sandbox.id)
        with self._lock:
            self._active[run_id] = active

        def _worker() -> None:
            started = time.time()
            try:
                result = self._runner.exec_run(
                    sandbox.id,
                    prompt,
                    run_id,
                    on_line=lambda line: self._on_line(run_id, line),
                )
            except SandboxNotFoundError as exc:
                self._finish_run(run_id, RunOutcome(errors=[f"sandbox gone: {exc}"], duration_ms=0))
                return
            except Exception as exc:  # worker must always finish the run row
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
        """Best-effort graceful cancel: signal the harness process via
        the runner (SIGINT path; tools get their salvage window) and
        mark the intent on the run row.  The run's own exit (result
        line with ``cancelled: true``, or error) finalizes the row."""
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
        """Persist terminal state from the worker thread.

        The Run row is created on the request thread's session (which
        commits when the request succeeds), so the worker may reach
        this point before that commit lands.  Retry briefly on a
        missing row, then finalize with an always-committed session —
        a lost terminal state is worse than a short wait.
        """
        factory = get_session_factory()
        deadline = time.time() + 10
        db = factory()
        final_state = run_status(outcome)
        try:
            while True:
                run = db.get(Run, run_id)
                if run is not None:
                    break
                if time.time() >= deadline:
                    run = Run(  # phantom finalize: preserve the outcome
                        id=run_id,
                        conversation_id=_phantom_conversation(db),
                        prompt="",
                        status=final_state,
                        error="run row missing (request never committed?)",
                    )
                    db.add(run)
                    break
                db.rollback()
                time.sleep(0.02)
            run.status = final_state
            run.answer = outcome.answer
            run.error = "\n".join(outcome.errors)
            run.exit_code = outcome.exit_code
            run.cancelled = outcome.cancelled
            run.finished_at = _now()
            if outcome.duration_ms:
                run.duration_ms = outcome.duration_ms
            with self._lock:
                active = self._active.get(run_id)
            if active is not None:
                with active.lock:
                    run.events = list(active.seen)
            usage = outcome.usage
            if usage is not None:
                owner = (
                    db.query(Conversation.user_id)
                    .filter(Conversation.id == run.conversation_id)
                    .scalar()
                    or ""
                )
                db.add(
                    UsageEvent(
                        id=new_id("use"),
                        user_id=owner,
                        conversation_id=run.conversation_id,
                        run_id=run_id,
                        input_tokens=int(usage.get("input", 0) or 0),
                        output_tokens=int(usage.get("output", 0) or 0),
                        rounds=int(usage.get("rounds", 0) or 0),
                        model=str(outcome.model or ""),
                    )
                )
            db.commit()
        finally:
            db.close()
            with self._lock:
                active = self._active.pop(run_id, None)
            if active is not None:
                with active.lock:
                    final = {"type": "run", "state": final_state}
                    active.seen.append(final)
                    subscribers = list(active.subscribers)
                for q in subscribers:
                    q.put(final)

    # -- reaper ------------------------------------------------------------

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
