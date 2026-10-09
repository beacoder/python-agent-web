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

import contextlib
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import or_
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
    escalation_armed: bool = False
    """Whether a cancel-escalation watchdog is already running for this
    run.  The cancel endpoint is NOT rate-limited (unlike run
    submission), so without this latch every repeated cancel on the
    same live run would spawn another watchdog thread that polls for
    the whole grace window — cheap to trigger from a client, and a
    thread-amplification vector."""


class Controller:
    """Coordinates sandboxes, harness execs, and subscribers."""

    def __init__(self, runner: Runner | None = None, instance_id: str | None = None) -> None:
        self._runner = runner or get_runner()
        self._active: dict[str, ActiveRun] = {}
        self._lock = threading.Lock()
        # Stable per-process identity so a restart reconciles only its
        # OWN orphaned runs, never another live instance's.  Overridable
        # for tests; defaults to a value stable for this process.
        self._instance_id = instance_id or _default_instance_id()
        # In-flight run worker threads, tracked so shutdown can drain
        # them (let short runs finish) instead of killing them mid-run.
        self._workers: dict[str, threading.Thread] = {}
        # Set on shutdown: reject new runs while draining so we do not
        # start work we are about to abandon.
        self._draining = threading.Event()

    @property
    def instance_id(self) -> str:
        return self._instance_id

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

        # Reject new work once shutdown has begun: a run started now
        # would be abandoned by the imminent exit.
        if self._draining.is_set():
            raise RuntimeError("server is shutting down; run rejected")

        run = Run(
            id=new_id("run"),
            conversation_id=conversation_id,
            prompt=prompt,
            status="running",
            harness_run_id="",
            owner_instance=self._instance_id,
        )
        db.add(run)
        # Commit the run row BEFORE building the sandbox.  Two reasons:
        # the worker finalizes this row from its own session, so
        # "running" must be durable first; and this commit is what
        # actually claims the conversation (the partial unique index is
        # the authoritative guard), so a submit that LOSES the race is
        # rejected here having spawned nothing.  Creating the sandbox
        # first meant the loser had already started a harness process
        # that the rollback then abandoned -- an orphan with no row.
        try:
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            raise RuntimeError("conversation already has a running run") from exc
        run_id = run.id

        # The conversation is ours; now get a sandbox to run in.
        spawned: list[str] = []
        try:
            sandbox = self._ensure_sandbox(
                db, user_id=user_id, conversation_id=conversation_id, spawned=spawned
            )
            sandbox_id = sandbox.id
            db.commit()
        except Exception as exc:
            # The run row is already live, so it must not be left
            # behind: finalize it before surfacing the failure, or the
            # conversation stays locked by a run that never started.
            db.rollback()
            # A sandbox created just now is only known to the runner;
            # the rollback threw away its row, so without this it would
            # be a process nothing owns (the same orphan the commit
            # ordering above exists to prevent, on the failure path).
            for orphan in spawned:
                with contextlib.suppress(Exception):
                    self._runner.destroy(orphan)
            _log.warning("run %s: sandbox unavailable: %s", run_id, exc)
            self._finish_run(run_id, RunOutcome(errors=[f"sandbox unavailable: {exc}"]))
            raise

        active = ActiveRun(run_id=run_id, sandbox_id=sandbox_id)
        with self._lock:
            self._active[run_id] = active

        def _worker() -> None:
            started = time.time()
            _log.info("run %s started (conversation %s)", run_id, conversation_id)
            try:
                result = self._runner.exec_run(
                    sandbox_id,
                    harness_prompt or prompt,
                    run_id,
                    on_line=lambda line: self._on_line(run_id, line),
                    # watchdog: without it a wedged harness (or a
                    # forever-pending ask) keeps the run "running" forever
                    timeout=get_settings().harness.timeout,
                )
            except SandboxNotFoundError as exc:
                _log.warning("run %s: sandbox gone: %s", run_id, exc)
                # Retire the row too, or _ensure_sandbox hands the same
                # dead sandbox to the next run and the conversation is
                # broken for good rather than for one turn.
                self._mark_sandbox_destroyed(sandbox_id)
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

        def _tracked_worker() -> None:
            try:
                _worker()
            finally:
                # untrack self so drain() does not wait on a finished
                # thread and the dict does not grow unbounded
                with self._lock:
                    self._workers.pop(run_id, None)

        thread = threading.Thread(target=_tracked_worker, name=f"run-{run_id}", daemon=True)
        with self._lock:
            self._workers[run_id] = thread
        thread.start()
        return run

    def cancel_run(self, run_id: str) -> bool:
        """Best-effort graceful cancel: send the protocol cancel op to
        the resident harness process and mark the intent on the run
        row.  The run's own exit (result line with ``cancelled: true``,
        or error) finalizes the row.

        A grace watchdog is armed behind the op (see
        ``_arm_cancel_escalation``) because ``op:cancel`` is
        cooperative and a wedged run can ignore it.
        """
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
        self._arm_cancel_escalation(run_id, active.sandbox_id)
        return True

    def _arm_cancel_escalation(self, run_id: str, sandbox_id: str) -> None:
        """Destroy the sandbox if a cancelled run refuses to end.

        ``op:cancel`` is a cooperative message: the harness checks it
        between rounds and unwinds the agent loop, which is why it is
        preferred (tools salvage, history is retained for the next
        turn).  A run blocked in an uninterruptible operation never
        reaches that check, so the cancel is simply ignored — and
        because the run row stays ``running``, the partial unique index
        locks the conversation out of starting another run until the
        process restarts.  That is the unrecoverable case this closes.

        Escalation destroys the sandbox, which closes the process's
        stdout; the exec's pump sees EOF and the run's own worker
        finalizes the row through its normal death path, so the
        terminal state is still written in exactly one place.

        This is NOT a run budget.  It is armed only by an explicit user
        cancel and never by a timer, so a run nobody cancels is
        unbounded as before.  ``cancel_grace_seconds`` of 0 disables it.
        """
        grace = get_settings().cancel_grace_seconds
        if grace <= 0:
            return
        with self._lock:
            active = self._active.get(run_id)
            if active is None or active.escalation_armed:
                return  # already finished, or a watchdog is already waiting
            active.escalation_armed = True

        def _escalate() -> None:
            deadline = time.time() + grace
            while time.time() < deadline:
                with self._lock:
                    if run_id not in self._active:
                        return  # honoured the cancel and unwound
                time.sleep(0.1)
            with self._lock:
                if run_id not in self._active:
                    return
            # Guarded on the run still OWNING the sandbox, not merely on
            # the run still being active: between the check above and
            # the teardown below this run could finalize and the next
            # turn could claim the same (resident) sandbox, and
            # destroying it then would kill an innocent successor run.
            # The runner re-checks its own live-run slot atomically.
            try:
                released = self._runner.destroy_if_running(sandbox_id, run_id)
            except Exception:  # noqa: BLE001 - teardown is best-effort
                # A half-torn-down sandbox is not reusable, so retire
                # the row anyway and let the next turn build a fresh one.
                _log.warning(
                    "run %s: escalation teardown of sandbox %s failed",
                    run_id,
                    sandbox_id,
                    exc_info=True,
                )
                released = True
            if not released:
                _log.info(
                    "run %s released the sandbox before escalation fired; left %s alone",
                    run_id,
                    sandbox_id,
                )
                return
            _log.warning(
                "run %s ignored cancel after %.1fs; destroyed sandbox %s to release it",
                run_id,
                grace,
                sandbox_id,
            )
            self._mark_sandbox_destroyed(sandbox_id)

        threading.Thread(target=_escalate, name=f"cancel-escalate-{run_id}", daemon=True).start()

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

    def _ensure_sandbox(
        self,
        db: Session,
        *,
        user_id: str,
        conversation_id: str,
        spawned: list[str] | None = None,
    ) -> Sandbox:
        """The conversation's live sandbox, creating one if needed.

        Appends the id of any sandbox it CREATES to *spawned*, so a
        caller whose transaction later fails can tear down a process
        that its rollback just orphaned.
        """
        from ..infra.metrics import get_registry

        sandbox = (
            db.query(Sandbox)
            .filter(
                Sandbox.user_id == user_id,
                Sandbox.conversation_id == conversation_id,
                Sandbox.status == "running",
            )
            .one_or_none()
        )
        if sandbox is not None and not self._runner.exists(sandbox.id):
            # The row outlived the runner that backed it: sandboxes are
            # in-process state, so every ``running`` row this runner
            # does not hold is a corpse.  Reusing it made exec_run raise
            # SandboxNotFoundError and the run fail with "sandbox
            # gone" — and since nothing ever cleaned the row up, EVERY
            # later run on that conversation failed the same way, which
            # is a permanent break rather than a transient one.  Retire
            # it and fall through to a fresh sandbox.
            if sandbox.owner_instance and sandbox.owner_instance != self._instance_id:
                # Another instance created it, so its resident process
                # (and the conversation history inside it) is on that
                # host and unreachable from here.  We can only start
                # over, which silently costs the user their multi-turn
                # context — so make it loud: resident sandboxes need
                # sticky routing to stay on one instance.
                _log.warning(
                    "conversation %s moved from instance %s to %s; its resident "
                    "sandbox is unreachable and agent context restarts — enable "
                    "session affinity for conversation routing",
                    conversation_id,
                    sandbox.owner_instance,
                    self._instance_id,
                )
                get_registry().counter(
                    "paw_sandbox_instance_migrations_total",
                    help="Conversations whose sandbox was rebuilt on another instance.",
                )
            else:
                _log.info(
                    "sandbox %s has no live runner state; retiring the row (conversation %s)",
                    sandbox.id,
                    conversation_id,
                )
            sandbox.status = "destroyed"
            sandbox.owner_instance = None
            db.flush()
            sandbox = None
        if sandbox is not None:
            # Claim it: the reaper judges a sandbox by idle age, and a
            # sandbox whose conversation the user is returning to is
            # stale by exactly that measure.  The exec happens later on
            # a worker thread, so without touching here the reaper may
            # destroy it in between and the run fails with "sandbox
            # gone" on the very request that revived the conversation.
            self._runner.touch(sandbox.id)
        if sandbox is None:
            sandbox_id = self._runner.create(user_id, conversation_id)
            if spawned is not None:
                spawned.append(sandbox_id)
            sandbox = Sandbox(
                id=sandbox_id,
                runner_id=get_settings().runner,
                user_id=user_id,
                conversation_id=conversation_id,
                status="running",
                owner_instance=self._instance_id,
            )
            db.add(sandbox)
            db.flush()
        return sandbox

    def _mark_sandbox_destroyed(self, sandbox_id: str) -> None:
        """Flip a sandbox row to ``destroyed``, in its own session.

        Called from worker/watchdog threads that have no request
        session.  Best-effort: failing to retire the row must not turn
        into a second failure on a path that is already handling one.
        """
        try:
            with get_session_factory()() as db:
                db.query(Sandbox).filter(Sandbox.id == sandbox_id).update(
                    {Sandbox.status: "destroyed", Sandbox.owner_instance: None},
                    synchronize_session=False,
                )
                db.commit()
        except Exception:  # noqa: BLE001 - bookkeeping must not mask the real error
            _log.warning("could not retire sandbox row %s", sandbox_id, exc_info=True)

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
        # metrics: runs by terminal state, latency, and tokens spent
        from ..infra.metrics import get_registry

        registry = get_registry()
        registry.counter(
            "paw_runs_total", labels={"state": final_state}, help="Runs by terminal state."
        )
        if outcome.duration_ms:
            registry.observe(
                "paw_run_duration_seconds",
                outcome.duration_ms / 1000.0,
                help="Run wall-clock duration in seconds.",
            )
        if outcome.usage:
            tokens = int(outcome.usage.get("input", 0) or 0) + int(
                outcome.usage.get("output", 0) or 0
            )
            if tokens:
                registry.counter(
                    "paw_tokens_total", value=tokens, help="LLM tokens consumed across runs."
                )
        try:
            with get_session_factory()() as db:
                _write_terminal_run(db, run_id, outcome, final_state, events)
                db.commit()
                # persist agent outputs to durable storage while we have
                # a session and the run's conversation is known.  Runs on
                # the terminal path so an errored/cancelled run's partial
                # outputs are captured too.
                _sync_run_artifacts(db, run_id)
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
        """Fail runs left ``running`` by a previous life of THIS instance.

        Runs execute on in-process daemon threads, so a restart abandons
        any in-flight run while its row still says ``running`` — stranding
        it forever and (via the partial unique index) blocking the
        conversation from starting a new one.  At startup ``_active`` is
        empty, so any ``running`` row this instance owns is orphaned:
        mark them ``error``.

        Instance-scoped on purpose: a multi-instance deploy has other
        live instances whose ``running`` rows are healthy.  Matching on
        ``owner_instance`` means this instance's restart never touches
        another's runs.  Legacy rows with a NULL owner (pre-migration)
        are reconciled too, since no live instance claims them.  Returns
        the number reconciled.
        """
        with get_session_factory()() as db:
            n = (
                db.query(Run)
                .filter(
                    Run.status == "running",
                    or_(
                        Run.owner_instance == self._instance_id,
                        Run.owner_instance.is_(None),
                    ),
                )
                .update(
                    {
                        Run.status: "error",
                        Run.error: "interrupted by server restart",
                        Run.finished_at: _now(),
                        Run.owner_instance: None,
                    },
                    synchronize_session=False,
                )
            )
            db.commit()
        if n:
            _log.warning(
                "reconciled %d orphaned run(s) to error on startup (instance %s)",
                n,
                self._instance_id,
            )
        return n

    def reconcile_orphaned_sandboxes(self) -> int:
        """Retire sandbox rows left ``running`` by a previous life of THIS
        instance.

        Sandboxes are in-process state, so at startup this instance's
        runner registry is empty and every ``running`` row it owns is a
        corpse.  Leaving them meant ``_ensure_sandbox`` handed a dead
        sandbox to the conversation's next run; they are retired lazily
        there too, but doing it once up front keeps the table honest
        and spares the first request the work.

        Instance-scoped like ``reconcile_orphaned_runs``: another live
        instance's sandboxes are healthy and must not be touched.
        Legacy rows with a NULL owner (pre-migration) are retired too,
        since no live instance claims them.  Returns the count.
        """
        with get_session_factory()() as db:
            n = (
                db.query(Sandbox)
                .filter(
                    Sandbox.status == "running",
                    or_(
                        Sandbox.owner_instance == self._instance_id,
                        Sandbox.owner_instance.is_(None),
                    ),
                )
                .update(
                    {Sandbox.status: "destroyed", Sandbox.owner_instance: None},
                    synchronize_session=False,
                )
            )
            db.commit()
        if n:
            _log.info(
                "retired %d orphaned sandbox row(s) on startup (instance %s)",
                n,
                self._instance_id,
            )
        return n

    def drain(self, timeout: float = 25.0) -> int:
        """Stop accepting new runs and wait for in-flight ones to finish.

        Called from the app's shutdown (SIGTERM): flips the draining
        flag so ``start_run`` rejects new work, then joins the live
        worker threads up to ``timeout`` (kept under the orchestrator's
        grace period).  Runs that finish within the window persist their
        result normally; any still running at the deadline are left for
        the next startup's instance-scoped reconciliation.  Returns the
        number of workers still running when the wait ended (0 = clean).
        """
        self._draining.set()
        with self._lock:
            threads = list(self._workers.values())
        deadline = time.time() + timeout
        for thread in threads:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            thread.join(timeout=remaining)
        with self._lock:
            still_running = sum(1 for t in self._workers.values() if t.is_alive())
        if still_running:
            _log.warning(
                "drain: %d run(s) still in flight at deadline; reconciled on next startup",
                still_running,
            )
        else:
            _log.info("drain: all in-flight runs finished cleanly")
        return still_running

    def reap_idle_sandboxes(self) -> list[str]:
        destroyed = self._runner.reap_idle(get_settings().sandbox.ttl_seconds)
        if destroyed:
            factory = get_session_factory()
            db = factory()
            try:
                db.query(Sandbox).filter(Sandbox.id.in_(destroyed)).update(
                    {Sandbox.status: "destroyed", Sandbox.owner_instance: None},
                    synchronize_session=False,
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
    # release ownership: a finished run is no longer any instance's to
    # reconcile (defensive — status is already terminal here).
    run.owner_instance = None
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


def _sync_run_artifacts(db: Session, run_id: str) -> None:
    """Best-effort: push a finished run's agent outputs to durable storage.

    Deferred import keeps the machinery layer from importing the files
    domain at module load.  Never raises -- artifact durability must not
    turn a finished run into a failed one.
    """
    try:
        from . import files as files_controller

        run = db.get(Run, run_id)
        if run is None:
            return
        conversation = db.get(Conversation, run.conversation_id)
        if conversation is None:
            return
        files_controller.sync_artifacts(db, conversation)
    except Exception:  # durability is best-effort; log and move on
        _log.warning("run %s: artifact sync failed", run_id, exc_info=True)


def _default_instance_id() -> str:
    """Stable identity for this process's run ownership.

    Prefers ``PAW_INSTANCE_ID`` (set it to the pod/task name in an
    orchestrator so the id survives as a meaningful label); otherwise
    derives a per-process id from hostname + pid, which is stable for
    the life of the process and unique enough to distinguish instances.
    """
    explicit = os.environ.get("PAW_INSTANCE_ID")
    if explicit:
        return explicit[:64]
    host = os.environ.get("HOSTNAME") or "host"
    return f"{host}-{os.getpid()}-{uuid.uuid4().hex[:6]}"[:64]
