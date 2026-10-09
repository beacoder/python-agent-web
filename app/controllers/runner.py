"""Sandbox runners: where the (untrusted) harness process executes.

``Runner`` is the sandbox-manager contract: create a sandbox, exec a
run inside it, destroy it.

Both real runners are *resident*: one long-lived ``python-agent-harness
serve`` per sandbox, driven over the bidirectional JSONL protocol.
They differ only in what that resident thing is -- a host subprocess
(``ServerRunner``) or a container (``DockerRunner``, where the trust
boundary becomes real isolation: no host env, no host filesystem, no
network by default, resource-capped).

That difference is confined to ``SandboxTransport``: spawn something,
read a line, write a line, check it is alive, interrupt a blocked read,
tear it down.  Everything else -- the registry and its locking, the
ready handshake, version negotiation, liveness probing, the idle
reaper, cancel/answer, the exec lifecycle and the event pump -- lives
once in ``ResidentRunner``.

It used to live twice, and the two copies drifted four separate times:
``destroy`` silently skipped teardown in one of them, one re-registered
a process onto a sandbox that had been swept, one never sent ``hello``,
and one never probed a warm sandbox before reusing it.  Each was found
only by reading the pair side by side.  Keeping the shared logic in one
place is what makes those bugs unrepresentable rather than merely
fixed.
"""

from __future__ import annotations

import contextlib
import json
import os
import shlex
import subprocess
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any

from ..infra.config import HarnessSettings, conversation_workspace, get_settings
from ..infra.logging import get_logger
from ..infra.metrics import get_registry
from .protocol import (
    CAP_HELLO,
    CAP_OP_ID,
    SUPPORTED_PROTOCOL_VERSIONS,
    Handshake,
    affects_outcome,
    parse_ready,
    read_capped_line,
)

_log = get_logger("sandbox")


@dataclass
class ExecResult:
    exit_code: int | None
    stdout: str
    stderr: str
    timed_out: bool = False
    killed: bool = False


# How long to wait for a process to be reaped after its stdout closed.
# Generous: this only runs on the death path, and reporting the real
# exit code is the whole point of that path.
_EXIT_REAP_TIMEOUT = 10.0

# How long a timed-out run is given to honour the protocol cancel before
# the transport is interrupted.
_CANCEL_ESCALATION_TIMEOUT = 10.0


def exit_status(proc: subprocess.Popen, timeout: float = _EXIT_REAP_TIMEOUT) -> int | None:
    """Exit code of a process whose stdout just reached EOF.

    EOF means the child closed stdout, NOT that it has been reaped, so
    ``poll()`` here races the child's exit and returns None a good
    fraction of the time -- reporting a crashed harness as "unknown
    exit code" and losing the one diagnostic the death path exists to
    provide.  Waiting collects the real status.

    The wait is bounded because closing stdout does not oblige a
    process to exit: a child that keeps running must not block the
    request thread, so that case still degrades to None.
    """
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        return proc.poll()


class SandboxNotFoundError(KeyError):
    pass


def serve_flags(harness: HarnessSettings) -> list[str]:
    """Sandbox-side budget flags for ``harness serve``, from settings.

    Each flag is emitted ONLY when its setting is non-None, so the
    default configuration produces an empty list and the spawn command
    is byte-identical to one with no budgets at all -- runs stay
    unlimited unless an operator opts in.

    These are spawn-time arguments on purpose.  The harness refuses to
    take budgets off the wire (a submit cannot raise its own ceiling),
    so the only way to give an operator the knob is here, at the point
    where the trusted side starts the sandbox.

    A 0/negative setting is normalized to "omit the flag" rather than
    passed through.  The harness reads 0 as "explicitly disable", so
    both mean unlimited -- but ``--max-rounds 0`` on a process listing
    reads like a ceiling of zero, and omitting it cannot be
    misinterpreted by a future harness that gives 0 a meaning.
    """
    flags: list[str] = []
    if harness.max_rounds is not None and harness.max_rounds > 0:
        flags += ["--max-rounds", str(int(harness.max_rounds))]
    if harness.sandbox_timeout is not None and harness.sandbox_timeout > 0:
        flags += ["--timeout", str(float(harness.sandbox_timeout))]
    if harness.answer_timeout is not None and harness.answer_timeout > 0:
        flags += ["--answer-timeout", str(float(harness.answer_timeout))]
    return flags


# -- the transport: the only thing the two runners disagree about ----------


class SandboxTransport:
    """One resident harness's stdio, however it is hosted.

    The seam between ``ResidentRunner`` (which owns all the protocol and
    lifecycle logic) and the two ways of hosting a sandbox.  Everything
    here is mechanical: no registry, no locking, no protocol.
    """

    label = "process"
    """What to call this in an operator-facing message."""

    def read_line(self, max_chars: int) -> tuple[str, bool]:
        """One line, and whether an oversize line was discarded."""
        raise NotImplementedError

    def write(self, text: str) -> None:
        raise NotImplementedError

    def alive(self) -> bool:
        raise NotImplementedError

    def interrupt(self) -> None:
        """Unblock a reader parked in ``read_line``.

        A blocking read with no data can only be broken by closing the
        thing underneath it, which is why every watchdog in here needs
        this rather than a flag.
        """
        raise NotImplementedError

    def exit_code(self) -> int | None:
        raise NotImplementedError

    def close(self) -> None:
        """Stop and release everything (idempotent)."""
        raise NotImplementedError

    def start_stderr_drain(self, sink: Any) -> None:
        """Begin forwarding stderr to *sink*.  No-op where not needed."""

    def take_oversize_drops(self) -> int:
        """Oversize lines this transport discarded since the last call.

        Non-zero only where the transport does its own capping; the
        pipe-backed one reports truncation inline from ``read_line``.
        """
        return 0

    # -- test/diagnostic accessors -------------------------------------
    proc: subprocess.Popen | None = None
    container: Any = None
    stream: Any = None


class _ProcessTransport(SandboxTransport):
    """A host subprocess speaking the protocol over pipes."""

    label = "process"

    def __init__(self, proc: subprocess.Popen) -> None:
        self.proc = proc
        self._draining = False

    def read_line(self, max_chars: int) -> tuple[str, bool]:
        # Not an assert: `python -O` strips those, and an AttributeError
        # on None would escape the callers' error handling.
        if self.proc.stdout is None:  # pragma: no cover - Popen with PIPE
            return "", False
        return read_capped_line(self.proc.stdout, max_chars)

    def write(self, text: str) -> None:
        if self.proc.stdin is None:  # pragma: no cover - Popen with PIPE
            raise OSError("sandbox stdin is closed")
        self.proc.stdin.write(text)
        self.proc.stdin.flush()

    def alive(self) -> bool:
        return self.proc.poll() is None

    def interrupt(self) -> None:
        with contextlib.suppress(OSError):
            self.proc.kill()

    def exit_code(self) -> int | None:
        return exit_status(self.proc)

    def close(self) -> None:
        # Graceful first: `shutdown` lets an active run emit its
        # terminal result, and closing stdin ends the reader loop by
        # EOF.  Kill is the backstop for a process that ignores both.
        with contextlib.suppress(ValueError, OSError, TypeError):
            if self.proc.stdin is not None:
                self.proc.stdin.write(json.dumps({"op": "shutdown"}) + "\n")
                self.proc.stdin.flush()
        with contextlib.suppress(ValueError, OSError):
            if self.proc.stdin is not None:
                self.proc.stdin.close()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            with contextlib.suppress(subprocess.TimeoutExpired):
                self.proc.wait(timeout=5)
        for pipe in (self.proc.stdout, self.proc.stderr):
            if pipe is None:  # pragma: no cover - Popen with PIPE
                continue
            with contextlib.suppress(ValueError, OSError):
                pipe.close()

    def start_stderr_drain(self, sink: Any) -> None:
        """One stderr drainer per PROCESS, not per exec.

        A resident process never EOFs its stderr between turns, so a
        per-exec readline thread would leak one blocked thread per turn.
        The drainer dies with the process.
        """
        if self._draining or self.proc.stderr is None:
            return
        self._draining = True
        stderr = self.proc.stderr

        def _drain() -> None:
            try:
                for chunk in iter(stderr.readline, ""):
                    sink(chunk)
            except (ValueError, OSError):
                pass  # stream closed at teardown

        threading.Thread(target=_drain, daemon=True, name="serve-stderr").start()


class _ContainerTransport(SandboxTransport):
    """A container speaking the protocol over its attach socket."""

    label = "container"

    def __init__(self, container: Any, stream: _DockerStream) -> None:
        self.container = container
        self.stream = stream
        self._oversize_seen = 0

    def read_line(self, max_chars: int) -> tuple[str, bool]:
        # The stream caps and discards oversize lines itself -- it has
        # to, because the cap applies to its demultiplexed buffer and
        # to the frame layer below it, not to a single pipe read.  So
        # the line handed back is already good; "oversize" here only
        # reports that the read produced nothing usable.
        line = self.stream.readline()
        return line, False

    def take_oversize_drops(self) -> int:
        """Oversize lines discarded since the last call."""
        dropped = self.stream.oversize_lines - self._oversize_seen
        self._oversize_seen = self.stream.oversize_lines
        return dropped

    def write(self, text: str) -> None:
        self.stream.write(text)

    def alive(self) -> bool:
        if self.container is None:
            return False
        try:
            self.container.reload()
        except Exception:
            return False
        return getattr(self.container, "status", None) == "running"

    def interrupt(self) -> None:
        with contextlib.suppress(Exception):
            if self.container is not None:
                self.container.kill()
        with contextlib.suppress(Exception):
            self.stream.close()

    def exit_code(self) -> int | None:
        return _container_exit_code(self.container)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.stream.write(json.dumps({"op": "shutdown"}) + "\n")
        with contextlib.suppress(Exception):
            self.stream.close()
        if self.container is not None:
            timeout = get_settings().docker.stop_timeout
            with contextlib.suppress(Exception):
                self.container.stop(timeout=timeout)
            with contextlib.suppress(Exception):
                self.container.remove(force=True)

    def oversize_dropped(self) -> int:
        return self.stream.oversize_lines


# -- the contract ----------------------------------------------------------


class Runner:
    """Sandbox-manager interface (see README architecture)."""

    def create(self, user_id: str, conversation_id: str) -> str:
        raise NotImplementedError

    def destroy(self, sandbox_id: str) -> None:
        raise NotImplementedError

    def destroy_if_running(self, sandbox_id: str, run_id: str) -> bool:
        """Destroy the sandbox only while *run_id* is still its live run.

        The escalation path for an ignored cancel.  Checking ownership
        atomically with the teardown matters because the sandbox is
        resident: by the time escalation fires, the cancelled run may
        have finalized and the conversation's NEXT turn may already own
        the same sandbox, and tearing it down then would kill an
        innocent successor run.

        Returns False when the sandbox has moved on (nothing done).
        Defaults to an unconditional destroy, since a runner that keeps
        no live-run slot cannot tell the difference.
        """
        self.destroy(sandbox_id)
        return True

    def touch(self, sandbox_id: str) -> None:
        """Mark the sandbox as used now, resetting its idle age.

        Called when a run CLAIMS a sandbox, not only when it starts
        executing in it: the claim happens on the request thread while
        the exec happens later on a worker, and in between the idle
        reaper is free to destroy a sandbox that is stale by age --
        which is exactly the state of a sandbox whose conversation the
        user is returning to.

        No-op by default (a runner with no idle tracking has nothing to
        reset).
        """

    def exists(self, sandbox_id: str) -> bool:
        """Whether this runner still holds live state for *sandbox_id*.

        The sandbox registry in the DB outlives the process that owned
        the sandbox, so a stored ``running`` row is only trustworthy if
        the runner still backs it.  Defaults to True: a runner that
        keeps no per-sandbox state cannot contradict the registry, so
        the row is trusted (the historical behaviour).
        """
        return True

    def capabilities(self, sandbox_id: str) -> frozenset[str]:
        """Features the sandbox's harness advertised at ``ready``.

        Empty when unknown.  Callers must treat an empty set as "cannot
        tell" rather than "nothing supported": the protocol features
        this host relies on all degrade gracefully, so behaviour
        follows what actually arrives on the wire.
        """
        return frozenset()

    def _tag(self, sandbox_id: str, op: dict[str, Any]) -> None:
        """Give *op* a correlation id, if the sandbox understands them.

        The protocol has no generic ack -- an op's effect on the stream
        is its acknowledgement -- but an effect that never happens
        cannot be correlated, so a refusal used to come back as a bare
        ``error`` with no link to the op that caused it.  The harness
        echoes this id on the error it emits, which is what lets a
        pipelined ``answer`` and ``cancel`` be told apart.

        Only sent to a build advertising ``op_id``.
        """
        if CAP_OP_ID in self.capabilities(sandbox_id):
            op["op_id"] = uuid.uuid4().hex[:12]

    def exec_run(
        self,
        sandbox_id: str,
        prompt: str,
        run_id: str,
        on_line: Any = None,
        timeout: float | None = None,
    ) -> ExecResult:
        raise NotImplementedError

    def cancel(self, sandbox_id: str, run_id: str) -> bool:
        """Best-effort graceful cancel of a live exec; False when the
        run is not running in this sandbox."""
        return False

    def ping(self, sandbox_id: str, timeout: float = 5.0) -> bool:
        """Liveness probe: True when the sandbox answered ``pong``.

        Distinguishes a resident process that is *wedged* from one that
        is merely idle.  A dead process is already caught by polling
        its exit status; one that is alive but no longer reading its
        stdin is not, and submitting to it would hang for the host
        timeout -- which is unbounded by default.

        True by default: a runner that cannot probe must never be
        reported as unhealthy.
        """
        return True

    def deliver_answer(
        self, sandbox_id: str, run_id: str, answers: list[str], ask_id: str | None = None
    ) -> bool:
        """Deliver a reply to the run's pending mid-run question.

        False when this runner has no mid-run Q&A (the caller maps that
        to a 409).  ``ask_id``, when known, names the question being
        answered so the harness can refuse a reply aimed at one that is
        no longer pending.
        """
        return False

    def reap_idle(self, ttl_seconds: float) -> list[str]:
        """Destroy sandboxes idle longer than TTL; return destroyed ids.

        Implementations MUST skip a sandbox with a live run regardless
        of its idle age: ``last_used`` is only touched when a run
        starts, so a long run looks increasingly idle while it is
        working, and reaping it would kill the resident process
        mid-run.  The TTL bounds idleness; it must never bound a run.
        """
        raise NotImplementedError


# -- the shared resident implementation -----------------------------------


class ResidentRunner(Runner):
    """One long-lived harness per sandbox, over the JSONL protocol.

    Owns the whole lifecycle: the sandbox registry and its locking, the
    ready handshake and version negotiation, liveness probing, the idle
    reaper, cancel/answer, and the per-run event pump.  Subclasses
    supply only a ``SandboxTransport`` and the metadata to build one.
    """

    def __init__(self) -> None:
        self._sandboxes: dict[str, dict[str, Any]] = {}
        self._transports: dict[str, SandboxTransport] = {}
        self._locks: dict[str, threading.Lock] = {}  # sandbox_id -> write lock
        self._live_run: dict[str, str | None] = {}  # sandbox_id -> active run_id
        self._lock = threading.Lock()
        # One exec per sandbox at a time: the live-run check in exec_run
        # is check-then-act, so concurrent execs on the SAME sandbox
        # (two turns racing) could both pass it and double-spawn.
        self._exec_locks: dict[str, threading.Lock] = {}

    # -- subclass hooks ---------------------------------------------------

    def _spawn_transport(self, sandbox_id: str, meta: dict[str, Any]) -> SandboxTransport:
        raise NotImplementedError

    def _new_meta(self, sandbox_id: str, user_id: str, conversation_id: str) -> dict[str, Any]:
        return {
            "sandbox_id": sandbox_id,
            "user_id": user_id,
            "conversation_id": conversation_id,
            "workspace": str(conversation_workspace(conversation_id)),
            "created_ts": time.time(),
            "last_used": time.time(),
            # bounded stderr tail, drained by one thread per transport
            "stderr_log": deque(maxlen=200),
        }

    def _label(self, sandbox_id: str) -> str:
        transport = self._transports.get(sandbox_id)
        return transport.label if transport is not None else "process"

    # -- sandbox lifecycle ------------------------------------------------

    def create(self, user_id: str, conversation_id: str) -> str:
        sandbox_id = f"sbx_{uuid.uuid4().hex}"
        meta = self._new_meta(sandbox_id, user_id, conversation_id)
        with self._lock:
            self._sandboxes[sandbox_id] = meta
        # Spawn eagerly so the first exec does not pay the startup
        # latency; a failed spawn is tolerated here and retried
        # (surfacing its error) at exec time.  The ready line is left in
        # the pipe for the first exec's handshake.
        transport: SandboxTransport | None
        try:
            transport = self._spawn_transport(sandbox_id, meta)
        except Exception:  # noqa: BLE001 - retried at exec, where it can be reported
            transport = None
        if transport is not None:
            with self._lock:
                self._register(sandbox_id, transport, fresh=True)
            transport.start_stderr_drain(meta["stderr_log"].append)
        return sandbox_id

    def _register(self, sandbox_id: str, transport: SandboxTransport, fresh: bool = False) -> None:
        """Record a transport for the sandbox.  Caller holds ``_lock``."""
        self._transports[sandbox_id] = transport
        self._locks[sandbox_id] = threading.Lock()
        if fresh:
            self._live_run[sandbox_id] = None
            self._exec_locks[sandbox_id] = threading.Lock()

    def _detach(self, sandbox_id: str) -> SandboxTransport | None:
        """Remove all registry state for the sandbox, under the lock.

        Clears every map unconditionally rather than bailing out when
        the meta is already gone: the maps can briefly disagree (a
        respawn re-registers a transport while a concurrent sweep
        removes the meta), and a ``destroy`` that skipped the teardown
        in that state would leak the sandbox for good.
        """
        with self._lock:
            self._sandboxes.pop(sandbox_id, None)
            transport = self._transports.pop(sandbox_id, None)
            self._locks.pop(sandbox_id, None)
            self._live_run.pop(sandbox_id, None)
            self._exec_locks.pop(sandbox_id, None)
        return transport

    def _detach_if(self, sandbox_id: str, predicate: Any) -> tuple[bool, SandboxTransport | None]:
        """Detach only when ``predicate(meta, live_run_id)`` holds.

        Returns ``(detached, transport)``; ``detached`` is False when
        the sandbox was left alone, which is distinct from detaching one
        that simply had no transport (a failed spawn at create).

        The predicate is evaluated INSIDE the lock that removes the
        sandbox.  That is what makes retiring a sandbox safe against a
        concurrent claim: deciding first and tearing down afterwards
        left a window in which the sandbox could change hands between
        the decision and the teardown.
        """
        with self._lock:
            meta = self._sandboxes.get(sandbox_id)
            if meta is None or not predicate(meta, self._live_run.get(sandbox_id)):
                return False, None
            self._sandboxes.pop(sandbox_id, None)
            transport = self._transports.pop(sandbox_id, None)
            self._locks.pop(sandbox_id, None)
            self._live_run.pop(sandbox_id, None)
            self._exec_locks.pop(sandbox_id, None)
        return True, transport

    def _detach_if_idle(
        self, sandbox_id: str, ttl_seconds: float, now: float
    ) -> tuple[bool, SandboxTransport | None]:
        """``_detach``, but only while the sandbox is idle AND stale.

        A live run is spared regardless of age: ``last_used`` is only
        touched when a run starts, so a long run looks ever more idle
        while it works, and tearing it down would kill it mid-run.
        """
        return self._detach_if(
            sandbox_id,
            lambda meta, live: (
                live is None and now - float(meta.get("last_used", 0.0)) > ttl_seconds
            ),
        )

    def _teardown(self, transport: SandboxTransport | None) -> None:
        if transport is not None:
            transport.close()

    def destroy(self, sandbox_id: str) -> None:
        self._teardown(self._detach(sandbox_id))

    def destroy_if_running(self, sandbox_id: str, run_id: str) -> bool:
        detached, transport = self._detach_if(sandbox_id, lambda meta, live: live == run_id)
        self._teardown(transport)
        return detached

    def _retire_transport(
        self, sandbox_id: str, meta: dict[str, Any], transport: SandboxTransport | None
    ) -> None:
        """Drop and stop *transport*, keeping the sandbox registered.

        Used when a spawned harness is unusable (an unsupported protocol
        version, a refused negotiation, a failed liveness probe).  The
        sandbox stays so the next run respawns and re-reads a fresh
        ``ready`` line -- reporting the same clear refusal.  Leaving the
        dead-but-registered transport behind would instead send the next
        exec into ``_read_ready`` on a stream whose single ready line is
        already consumed, where it blocks until the watchdog fires and
        reports a misleading "died before ready".
        """
        with self._lock:
            # Guarded on identity: evicting whatever happens to be
            # registered would retire a transport this call never saw
            # (a respawn may already have replaced it).
            if transport is not None and self._transports.get(sandbox_id) is transport:
                self._transports.pop(sandbox_id, None)
                self._locks.pop(sandbox_id, None)
        meta["ready_ok"] = False
        meta["greeted"] = False  # a fresh transport must renegotiate
        self._teardown(transport)

    def exists(self, sandbox_id: str) -> bool:
        with self._lock:
            return sandbox_id in self._sandboxes

    def touch(self, sandbox_id: str) -> None:
        with self._lock:
            if sandbox_id in self._sandboxes:
                self._sandboxes[sandbox_id]["last_used"] = time.time()

    def capabilities(self, sandbox_id: str) -> frozenset[str]:
        with self._lock:
            meta = self._sandboxes.get(sandbox_id)
            handshake = meta.get("handshake") if meta else None
        return handshake.capabilities if handshake is not None else frozenset()

    def reap_idle(self, ttl_seconds: float) -> list[str]:
        now = time.time()
        destroyed: list[str] = []
        with self._lock:
            candidates = list(self._sandboxes)
        for sandbox_id in candidates:
            # The idle/stale decision is re-made INSIDE the lock that
            # removes the sandbox (see _detach_if_idle), so a run that
            # claims and touches a stale sandbox concurrently keeps it.
            detached, transport = self._detach_if_idle(sandbox_id, ttl_seconds, now)
            if not detached:
                continue
            destroyed.append(sandbox_id)
            self._teardown(transport)
        return destroyed

    # -- live-run slot ----------------------------------------------------

    def _claim_live_run(self, sandbox_id: str, run_id: str) -> None:
        """Take the sandbox's live-run slot, or report it gone.

        The sandbox can be destroyed between ``exec_lock`` being taken
        and the claim (an escalated cancel, a sweep).  Writing the slot
        blindly would re-add a ``_live_run`` key for a sandbox with no
        meta, breaking the invariant that ``_live_run``'s keys are a
        subset of ``_sandboxes``' -- and leaking the entry, since
        nothing will ever destroy that id again.
        """
        with self._lock:
            if sandbox_id not in self._sandboxes:
                raise SandboxNotFoundError(sandbox_id)
            self._live_run[sandbox_id] = run_id

    def _release_live_run(self, sandbox_id: str) -> None:
        """Free the live-run slot, without resurrecting a dead sandbox."""
        with self._lock:
            if sandbox_id in self._sandboxes:
                self._live_run[sandbox_id] = None
            else:
                self._live_run.pop(sandbox_id, None)

    # -- protocol I/O -----------------------------------------------------

    def _send(
        self, transport: SandboxTransport | None, lock: threading.Lock | None, op: dict
    ) -> bool:
        """Write one op line to the resident harness (serialized).

        False when the transport is already gone or dead (the write
        raises) -- the caller reports the failure instead of waiting
        forever.
        """
        if transport is None:
            return False
        if lock is None:
            lock = threading.Lock()
        try:
            with lock:
                transport.write(json.dumps(op) + "\n")
        except (ValueError, OSError):
            return False
        return True

    def _await_line(
        self, transport: SandboxTransport, types: set[str], timeout: float
    ) -> dict[str, Any] | None:
        """Read until a line of one of *types* arrives.

        Only safe between runs: the pump owns the transport while a run
        is live.  Skips lines of other types rather than taking the
        first one, so an unrelated line left in the pipe cannot be
        mistaken for the reply.  A watchdog interrupts the transport on
        deadline -- a blocking read with no data cannot be unblocked by
        a flag.
        """
        answered = threading.Event()

        def _interrupt_on_deadline() -> None:
            if not answered.wait(timeout):
                transport.interrupt()

        threading.Thread(target=_interrupt_on_deadline, daemon=True).start()
        try:
            while True:
                line, _ = transport.read_line(get_settings().max_line_bytes)
                if not line:
                    return None
                try:
                    payload = json.loads(line)
                except ValueError:
                    continue
                if isinstance(payload, dict) and payload.get("type") in types:
                    return payload
        except (ValueError, OSError):
            return None
        finally:
            answered.set()

    def _read_ready(self, transport: SandboxTransport, timeout: float = 30.0) -> Handshake | None:
        """Consume the resident harness's ``ready`` line.

        Called right after spawn, before the first op: the handshake
        guarantees the runtime imported cleanly.  Returns the parsed
        handshake, or None when the sandbox died first.
        """
        payload = self._await_line(transport, {"ready"}, timeout)
        return parse_ready(payload) if payload is not None else None

    def _accept_handshake(
        self, sandbox_id: str, meta: dict[str, Any], handshake: Handshake
    ) -> str | None:
        """Record the handshake; return an error message if unusable.

        The protocol version is the one thing a host can only accept or
        reject wholesale, so an unknown one fails here, loudly and once
        per run, instead of being misparsed line by line for the rest
        of the run.  Capabilities are informational: every protocol
        feature this host uses degrades gracefully, so they are logged
        for diagnosis rather than gated on.
        """
        meta["handshake"] = handshake
        if not handshake.supported:
            return (
                f"unsupported harness protocol version {handshake.protocol_version} "
                f"(this build speaks {sorted(SUPPORTED_PROTOCOL_VERSIONS)}); "
                "upgrade the web layer or pin the harness image"
            )
        _log.info(
            "sandbox handshake: %s (conversation %s)",
            handshake.describe(),
            meta.get("conversation_id", "?"),
        )
        return None

    def _negotiate(
        self,
        transport: SandboxTransport,
        lock: threading.Lock | None,
        meta: dict[str, Any],
    ) -> str | None:
        """Tell the harness which versions this host can parse.

        ``ready`` is the harness announcing itself; this is the other
        half, so a version mismatch is settled before any run rather
        than surfacing as misparsed events.  Returns a refusal message
        when there is no shared version, else None.

        Sent once per TRANSPORT, not per run: the reply is consumed here
        rather than left in the pipe for the run's pump to trip over,
        and repeating it every turn would add a round-trip and an
        unrelated line to every run's stream.

        Only sent to a build advertising the capability -- an older
        harness answers an unknown op with an ``error`` line, which
        would land in the run's error trail for no reason.
        """
        handshake: Handshake | None = meta.get("handshake")
        if meta.get("greeted") or handshake is None or not handshake.has(CAP_HELLO):
            return None
        meta["greeted"] = True  # one attempt per transport, success or not
        op = {
            "op": "hello",
            "protocol_versions": sorted(SUPPORTED_PROTOCOL_VERSIONS),
            "op_id": uuid.uuid4().hex[:12],
        }
        if not self._send(transport, lock, op):
            return None  # dead pipe; the submit below reports it properly
        timeout = get_settings().sandbox.probe_timeout or 5.0
        reply = self._await_line(transport, {"hello", "error"}, timeout)
        if reply is None:
            # No answer (or the watchdog fired).  `ready` already told
            # us the version is one we accept, so degrade rather than
            # refuse a sandbox that is otherwise fine.
            _log.warning("sandbox did not answer hello; proceeding on the ready handshake")
            return None
        if reply.get("type") == "error":
            message = reply.get("message") or "version negotiation refused"
            return f"harness refused version negotiation: {message}"
        _log.info("negotiated protocol with sandbox: %s", reply.get("protocol_version"))
        return None

    def ping(self, sandbox_id: str, timeout: float = 5.0) -> bool:
        """Probe a warm sandbox with ``op:ping`` and await ``pong``.

        Only safe between runs, which is the only time it is called:
        the pump owns the transport while a run is live, so a
        concurrent read here would steal that run's events.  Reports
        True ("cannot tell") rather than probing if a run holds the
        sandbox.
        """
        if timeout <= 0:
            return True
        with self._lock:
            transport = self._transports.get(sandbox_id)
            lock = self._locks.get(sandbox_id)
            if self._live_run.get(sandbox_id) is not None:
                return True  # a run owns the transport; not ours to read
        if transport is None or not transport.alive():
            return False
        if not self._send(transport, lock, {"op": "ping"}):
            return False
        return self._await_line(transport, {"pong"}, timeout) is not None

    # -- ops --------------------------------------------------------------

    def cancel(self, sandbox_id: str, run_id: str) -> bool:
        """Protocol-level cancel: an ``op:cancel`` line to the resident
        harness (no signals).  False when the run is not live here."""
        with self._lock:
            transport = self._transports.get(sandbox_id)
            live = self._live_run.get(sandbox_id)
            lock = self._locks.get(sandbox_id)
        if transport is None or live != run_id or not transport.alive():
            return False
        self.touch(sandbox_id)
        op: dict[str, Any] = {"op": "cancel", "run_id": run_id}
        self._tag(sandbox_id, op)
        return self._send(transport, lock, op)

    def deliver_answer(
        self, sandbox_id: str, run_id: str, answers: list[str], ask_id: str | None = None
    ) -> bool:
        """Deliver the user's answer to a pending mid-run question.

        False when the run is not live in this sandbox (the caller maps
        that to 409/404); a protocol-level "no pending question" is
        still a delivery attempt -- the harness answers with an error
        line, which the event stream relays.
        """
        with self._lock:
            transport = self._transports.get(sandbox_id)
            live = self._live_run.get(sandbox_id)
            lock = self._locks.get(sandbox_id)
        if transport is None or live != run_id or not transport.alive():
            return False
        self.touch(sandbox_id)
        op: dict[str, Any] = {"op": "answer", "run_id": run_id, "answers": answers}
        if ask_id:
            # Correlate the reply with the question it answers.  Without
            # it the harness can only resolve "whatever is pending", so
            # a reply sent for a question that has since timed out would
            # silently answer the NEXT one -- a question the user never
            # saw.  The harness also refuses an uncorrelated answer once
            # any ask in the run has timed out.
            op["ask_id"] = ask_id
        self._tag(sandbox_id, op)
        return self._send(transport, lock, op)

    # -- run execution ----------------------------------------------------

    def exec_run(
        self,
        sandbox_id: str,
        prompt: str,
        run_id: str,
        on_line: Any = None,
        timeout: float | None = None,
    ) -> ExecResult:
        with self._lock:
            meta = self._sandboxes.get(sandbox_id)
            exec_lock = self._exec_locks.get(sandbox_id)
        if meta is None:
            raise SandboxNotFoundError(sandbox_id)
        if exec_lock is None:
            # Missing only when create()'s spawn also failed; make one
            # so a respawn-at-exec can still serialize.
            exec_lock = threading.Lock()
            with self._lock:
                self._exec_locks[sandbox_id] = exec_lock
        # Serialize the whole exec (live-check, respawn, submit, pump)
        # per sandbox: the live-run check alone is check-then-act and
        # racy under concurrent execs on the same sandbox.
        with exec_lock:
            return self._exec_run_locked(sandbox_id, prompt, run_id, on_line, timeout, meta)

    def _prepare_transport(
        self, sandbox_id: str, meta: dict[str, Any]
    ) -> tuple[SandboxTransport | None, threading.Lock | None, ExecResult | None]:
        """Get a handshaken transport for the run, respawning if needed.

        Returns ``(transport, lock, failure)``; exactly one of the
        transport or the failure is set.
        """
        with self._lock:
            transport = self._transports.get(sandbox_id)
            lock = self._locks.get(sandbox_id)

        if transport is None or not transport.alive():
            return self._respawn(sandbox_id, meta)

        if not meta.get("ready_ok"):
            # A transport spawned eagerly at create() still has its
            # ready line pending.
            handshake = self._read_ready(transport)
            if handshake is None:
                return None, None, self._died(transport, "before ready")
            refusal = self._accept_handshake(sandbox_id, meta, handshake)
            if refusal is not None:
                self._retire_transport(sandbox_id, meta, transport)
                return None, None, ExecResult(exit_code=None, stdout="", stderr=refusal)
            meta["ready_ok"] = True
            return transport, lock, None

        if not self.ping(sandbox_id, get_settings().sandbox.probe_timeout):
            # A warm sandbox from a previous turn that no longer answers
            # its own liveness op is wedged: alive, so the check above
            # saw nothing wrong, but it will never read our submit.
            # Without this the run hung for the host timeout, which is
            # unbounded by default.
            _log.warning("sandbox %s failed its liveness probe; respawning", sandbox_id)
            get_registry().counter(
                "paw_sandbox_probe_failures_total",
                help="Resident sandboxes that failed a liveness probe and were respawned.",
            )
            self._retire_transport(sandbox_id, meta, transport)
            return self._respawn(sandbox_id, meta)

        return transport, lock, None

    def _respawn(
        self, sandbox_id: str, meta: dict[str, Any]
    ) -> tuple[SandboxTransport | None, threading.Lock | None, ExecResult | None]:
        """Start a fresh transport and take it through the handshake."""
        try:
            transport = self._spawn_transport(sandbox_id, meta)
        except Exception as exc:  # noqa: BLE001 - reported as a failed run
            return None, None, ExecResult(exit_code=None, stdout="", stderr=f"exec failed: {exc}")
        meta["stderr_log"].clear()
        transport.start_stderr_drain(meta["stderr_log"].append)
        handshake = self._read_ready(transport)
        if handshake is None:
            return None, None, self._died(transport, "before ready")
        refusal = self._accept_handshake(sandbox_id, meta, handshake)
        if refusal is not None:
            self._retire_transport(sandbox_id, meta, transport)
            return None, None, ExecResult(exit_code=None, stdout="", stderr=refusal)
        with self._lock:
            vanished = sandbox_id not in self._sandboxes
            lock: threading.Lock | None = None
            if not vanished:
                self._register(sandbox_id, transport)
                lock = self._locks[sandbox_id]
        if vanished:
            # The sandbox was destroyed while we were spawning and
            # waiting for `ready` (the live-run slot is only claimed
            # later, so a sweep or an explicit destroy could still take
            # it).  Re-registering would leave a transport with no meta
            # -- an entry nothing owns.  Tear down what we started,
            # clear any leftovers for the id, and report it gone; the
            # caller retires the row and the next turn rebuilds.
            self._teardown(transport)
            self._teardown(self._detach(sandbox_id))
            raise SandboxNotFoundError(sandbox_id)
        meta["ready_ok"] = True
        return transport, lock, None

    def _died(self, transport: SandboxTransport, when: str) -> ExecResult:
        return ExecResult(
            exit_code=None, stdout="", stderr=f"harness {transport.label} died {when}"
        )

    def _exec_run_locked(
        self,
        sandbox_id: str,
        prompt: str,
        run_id: str,
        on_line: Any,
        timeout: float | None,
        meta: dict[str, Any],
    ) -> ExecResult:
        with self._lock:
            if self._live_run.get(sandbox_id) is not None:
                raise RuntimeError(f"sandbox {sandbox_id} already has a live run")

        transport, lock, failure = self._prepare_transport(sandbox_id, meta)
        if failure is not None:
            return failure
        assert transport is not None

        self.touch(sandbox_id)
        # stderr diagnostics: snapshot the position in the per-transport
        # drainer's buffer, so this exec only reports ITS OWN stderr
        err_from = len(meta["stderr_log"])

        refusal = self._negotiate(transport, lock, meta)
        if refusal is not None:
            self._retire_transport(sandbox_id, meta, transport)
            return ExecResult(exit_code=None, stdout="", stderr=refusal)

        self._claim_live_run(sandbox_id, run_id)
        submit: dict[str, Any] = {"op": "submit", "prompt": prompt, "run_id": run_id}
        self._tag(sandbox_id, submit)
        try:
            if not transport.alive() or not self._send(transport, lock, submit):
                return self._died(transport, "before submit")
            stdout_lines, timed_out, saw_result = self._pump_until_result(
                transport,
                on_line,
                timeout,
                cancel_op=lambda: self._send(transport, lock, {"op": "cancel", "run_id": run_id}),
            )
            stderr = "".join(list(meta["stderr_log"])[err_from:])
            if not saw_result and not timed_out:
                # EOF without a result line: the sandbox died mid-run
                # (write success was a race with exit).  Surface death
                # with the real exit code.
                return ExecResult(
                    exit_code=transport.exit_code(),
                    stdout="".join(stdout_lines),
                    stderr=stderr or f"harness {transport.label} died mid-run",
                )
        finally:
            self._release_live_run(sandbox_id)
        return ExecResult(
            exit_code=0 if not timed_out else None,
            stdout="".join(stdout_lines),
            stderr=stderr,
            timed_out=timed_out,
        )

    def _pump_until_result(
        self,
        transport: SandboxTransport,
        on_line: Any,
        timeout: float | None,
        cancel_op: Any = None,
    ) -> tuple[list[str], bool, bool]:
        """Read lines until the run's ``result`` line (or death).

        The sandbox stays alive after the result (the next turn reuses
        it), so the run's terminal marker is its parsed ``result`` line,
        never a substring match -- a raw line echoing result-JSON must
        not count.  Returns ``(stdout_lines, timed_out, saw_result)``.

        stderr is drained per transport, not here: the sandbox outlives
        the exec, so a per-exec reader would block forever and leak one
        thread per turn.

        The deadline is enforced by a watchdog, since a blocking read
        cannot be polled out of.  It sends the protocol cancel first
        (graceful: the run unwinds, history is retained) and interrupts
        the transport only if the run does not end.
        """
        stdout_lines: list[str] = []
        saw_result = False
        done = threading.Event()  # result line seen
        timed_out = threading.Event()  # watchdog fired

        if timeout is not None:

            def _escalate_on_deadline() -> None:
                if done.wait(timeout):
                    return
                timed_out.set()
                # Graceful first: the protocol cancel lets the resident
                # harness unwind its run (tools salvage, history kept
                # for the next turn).  Interrupt only if the run does
                # not actually end -- waiting on the RESULT, not on the
                # sandbox exiting, because a resident harness is
                # supposed to survive a cancel.
                if cancel_op is not None:
                    cancel_op()
                    if done.wait(_CANCEL_ESCALATION_TIMEOUT):
                        return  # unwound gracefully; leave it warm
                transport.interrupt()

            threading.Thread(target=_escalate_on_deadline, daemon=True).start()

        settings = get_settings()
        max_line = settings.max_line_bytes
        budget = settings.max_run_stdout_bytes
        kept = 0
        while True:
            line, oversize = transport.read_line(max_line)
            dropped = transport.take_oversize_drops()
            if dropped:
                _log.warning("sandbox discarded %d oversize line(s)", dropped)
                get_registry().counter(
                    "paw_protocol_oversize_lines_total",
                    value=dropped,
                    help="Harness lines discarded for exceeding the line cap.",
                )
            if oversize:
                # Unparseable by definition, and relaying the fragment
                # would just produce a malformed event.  The seq gap is
                # visible to a client either way.
                _log.warning("discarded an oversize harness line (> %d chars)", max_line)
                get_registry().counter(
                    "paw_protocol_oversize_lines_total",
                    help="Harness lines discarded for exceeding the line cap.",
                )
                continue
            if not line:  # EOF: died (crash, kill, or timeout)
                break
            line = line.rstrip("\n")
            try:
                payload = json.loads(line)
            except ValueError:
                payload = None
            # Retain the raw transcript only within budget; past it keep
            # just the lines that can still decide the outcome, so a
            # chatty run cannot hold the whole stream in memory but the
            # verdict and token counts are never lost.
            relevant = isinstance(payload, dict) and affects_outcome(payload)
            if budget <= 0 or kept < budget or relevant:
                stdout_lines.append(line + "\n")
                kept += len(line) + 1
            if on_line is not None:
                # a slow/broken subscriber must not kill the run
                with contextlib.suppress(Exception):
                    on_line(line)
            if isinstance(payload, dict) and payload.get("type") == "result":
                saw_result = True
                done.set()
                break
        return stdout_lines, timed_out.is_set(), saw_result


# -- host-subprocess runner ------------------------------------------------


class ServerRunner(ResidentRunner):
    """Resident runner: one ``harness serve`` process per sandbox.

    Fast and simple; suitable for a trusted single-node deployment. The
    agent shares the host environment, which is the thing
    ``DockerRunner`` exists to remove.
    """

    def __init__(self, harness: HarnessSettings | None = None) -> None:
        super().__init__()
        self._harness = harness or get_settings().harness

    def _command(self) -> list[str]:
        cmd = shlex.split(self._harness.cmd)
        cmd += ["serve"]
        cmd += serve_flags(self._harness)
        return cmd

    def _workspace_dir(self, conversation_id: str) -> str:
        return str(conversation_workspace(conversation_id))

    def _new_meta(self, sandbox_id: str, user_id: str, conversation_id: str) -> dict[str, Any]:
        meta = super()._new_meta(sandbox_id, user_id, conversation_id)
        meta["workspace"] = self._workspace_dir(conversation_id)
        return meta

    def _spawn_transport(self, sandbox_id: str, meta: dict[str, Any]) -> SandboxTransport:
        env = dict(os.environ)
        env.setdefault("PAW_NONINTERACTIVE", "1")
        proc = subprocess.Popen(  # noqa: S603 - cmd is admin-configured
            self._command(),
            cwd=meta.get("workspace") or None,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=(os.name == "posix"),
        )
        return _ProcessTransport(proc)

    # -- diagnostic view over the transports ------------------------------

    @property
    def _procs(self) -> dict[str, subprocess.Popen]:
        """Live processes by sandbox id (diagnostics and tests)."""
        with self._lock:
            return {sid: t.proc for sid, t in self._transports.items() if t.proc is not None}


# -- container runner ------------------------------------------------------


class _DockerStream:
    """A newline-reader / line-writer over a docker attach socket.

    Presents the ``readline()`` / ``write(str)`` surface the resident
    pumping code expects from a pipe.  A non-TTY attach multiplexes
    stdout/stderr into 8-byte-framed chunks; we demultiplex, routing
    stdout to the line buffer and stderr to an optional sink.

    The underlying object only needs ``recv``/``sendall``/``close`` (a
    real socket, or a fake in tests).
    """

    _HEADER = 8  # docker stream frame: [stream, 0,0,0, size(4, big-endian)]

    def __init__(self, sock: Any, stderr_sink: Any = None, max_line: int = 0) -> None:
        self._sock = sock
        self._stderr_sink = stderr_sink
        self._buf = b""  # raw framed bytes not yet demultiplexed
        self._pending = b""  # demultiplexed stdout bytes awaiting a newline
        self._closed = False
        # Longest line we will buffer.  Without it both buffers grow
        # unboundedly: `_pending` until a newline arrives, and `_buf`
        # until a frame's declared payload is complete.  Untrusted code
        # controls both, so each needs its own ceiling.
        self._max_line = max_line
        self._discarding = False  # mid-skip of an oversize line
        self._skip_payload = 0  # bytes of an oversize frame still to drop
        self.oversize_lines = 0

    def _demux(self, chunk: bytes) -> bytes:
        """Split a raw framed chunk into stdout bytes; feed stderr to the
        sink.  Frames can straddle recv boundaries, so leftover header/
        payload bytes are retained across calls.

        A frame whose declared payload alone exceeds the line cap is
        dropped as its bytes arrive rather than assembled: waiting for
        it to complete would buffer the whole thing in ``_buf``, which
        is the exhaustion the cap exists to prevent and happens one
        layer below ``_pending``.
        """
        self._buf += chunk
        out = b""
        while True:
            if self._skip_payload:
                drop = min(self._skip_payload, len(self._buf))
                region, self._buf = self._buf[:drop], self._buf[drop:]
                self._skip_payload -= drop
                # The oversize line ends at its newline, which may sit
                # inside the region we are dropping.  Without looking
                # for it, `_discarding` stayed set and swallowed the
                # next good line as if it were this one's tail.
                newline = region.find(b"\n")
                if self._discarding and newline != -1:
                    self._discarding = False
                    out += region[newline + 1 :]  # already the next line
                if self._skip_payload:
                    break  # need more bytes before this frame is behind us
                continue
            if len(self._buf) < self._HEADER:
                break
            stream_type = self._buf[0]
            size = int.from_bytes(self._buf[4:8], "big")
            if self._max_line > 0 and size > self._max_line:
                self._buf = self._buf[self._HEADER :]
                self._skip_payload = size
                self._note_oversize()
                continue
            if len(self._buf) < self._HEADER + size:
                break  # payload incomplete; wait for more
            payload = self._buf[self._HEADER : self._HEADER + size]
            self._buf = self._buf[self._HEADER + size :]
            if stream_type == 2 and self._stderr_sink is not None:  # stderr
                with contextlib.suppress(Exception):
                    self._stderr_sink(payload.decode("utf-8", "replace"))
            else:
                out += payload
        return out

    def _note_oversize(self) -> None:
        """Start skipping to the next line boundary (counted once)."""
        if not self._discarding:
            self._discarding = True
            self.oversize_lines += 1

    def readline(self, limit: int = -1) -> str:
        """One demultiplexed stdout line ('' on EOF), like a text pipe.

        Frames can carry several lines at once, so demultiplexed stdout
        is buffered and handed back one newline-terminated line per call
        -- returning a multi-line blob would break the JSONL parser that
        reads this stream one ``result``-bearing line at a time.

        Lines over ``max_line`` are dropped rather than returned, and
        the rest of an oversize line is skipped so its tail is not
        reparsed as fresh lines.  Iterative on purpose: recursing once
        per discarded line let a stream of them exhaust the stack.
        """
        while True:
            if b"\n" in self._pending:
                line, _, self._pending = self._pending.partition(b"\n")
                if self._discarding:
                    # that newline ended the line we were skipping, not
                    # a line the caller should see
                    self._discarding = False
                    continue
                if self._max_line > 0 and len(line) > self._max_line:
                    # arrived complete but still too big: drop it whole
                    self.oversize_lines += 1
                    continue
                return (line + b"\n").decode("utf-8", "replace")
            if self._max_line > 0 and len(self._pending) > self._max_line:
                # over the cap with no newline in sight: stop buffering
                # and skip forward to the next line boundary
                self._pending = b""
                self._note_oversize()
            try:
                chunk = self._sock.recv(4096)
            except (OSError, ValueError):
                chunk = b""
            if not chunk:
                self._closed = True
                # flush any trailing partial line at EOF
                if self._pending and not self._discarding:
                    line, self._pending = self._pending, b""
                    return line.decode("utf-8", "replace")
                self._pending = b""
                return ""
            self._pending += self._demux(chunk)

    def write(self, data: str) -> None:
        self._sock.sendall(data.encode("utf-8"))

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._sock.close()


class DockerRunner(ResidentRunner):
    """Isolated runner: one long-lived container per sandbox.

    Same protocol and same lifecycle as ``ServerRunner`` -- but across a
    container boundary, which is where the trust boundary becomes real:
    no host environment (only ``PAW_NONINTERACTIVE`` plus the owner's
    decrypted secrets, injected host-side at create), no host
    filesystem beyond the conversation's own workspace mount, no network
    by default, a read-only rootfs, dropped Linux capabilities, a
    non-root user, and hard memory/cpu/pid ceilings.  A hostile prompt
    therefore cannot read ``paw.db``, the app secret key, or another
    tenant's data, and a wedged run is resource-bounded.
    """

    def __init__(self, client: Any = None, secret_source: Any = None) -> None:
        super().__init__()
        # client / secret_source are injectable for tests; production
        # builds a real docker client lazily (so the SDK is only needed
        # when PAW_RUNNER=docker) and decrypts secrets host-side.
        self._client = client
        self._secret_source = secret_source

    # -- client / secrets -------------------------------------------------

    def _docker(self) -> Any:
        if self._client is None:
            import docker  # lazy: only required for PAW_RUNNER=docker

            self._client = docker.from_env()
        return self._client

    def _secrets_for(self, user_id: str) -> dict[str, str]:
        """Owner's decrypted secrets, host-side.  Empty when no source
        is wired (tests) or the user has none."""
        if self._secret_source is None:
            return {}
        try:
            return dict(self._secret_source(user_id))
        except Exception:  # a secrets failure must not brick sandbox create
            return {}

    def _container_env(self, user_id: str) -> dict[str, str]:
        """The container's ENTIRE environment: never the host env.

        Only the noninteractive flag and the owner's own decrypted
        secrets.  This is the line that closes the ``dict(os.environ)``
        leak that ``ServerRunner`` has.
        """
        env = {"PAW_NONINTERACTIVE": "1"}
        env.update(self._secrets_for(user_id))
        return env

    def _create_kwargs(self, meta: dict[str, Any]) -> dict[str, Any]:
        d = get_settings().docker
        return {
            "image": d.image,
            "command": ["serve", *serve_flags(get_settings().harness)],
            "environment": self._container_env(meta["user_id"]),
            "working_dir": d.workdir,
            "volumes": {meta["workspace"]: {"bind": d.workdir, "mode": "rw"}},
            "network_mode": d.network,
            "mem_limit": d.mem_limit,
            "nano_cpus": d.nano_cpus,
            "pids_limit": d.pids_limit,
            "user": d.user,
            "read_only": d.read_only_rootfs,
            "tmpfs": {"/tmp": f"size={d.tmpfs_size}"},
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges"],
            "stdin_open": True,
            "detach": True,
            "labels": {"paw.sandbox": meta["sandbox_id"], "paw.user": meta["user_id"]},
        }

    def _spawn_transport(self, sandbox_id: str, meta: dict[str, Any]) -> SandboxTransport:
        client = self._docker()
        container = client.containers.create(**self._create_kwargs(meta))
        container.start()
        sock = container.attach_socket(params={"stdin": 1, "stdout": 1, "stderr": 1, "stream": 1})
        # SDK wraps the raw socket; unwrap to the object with recv/sendall
        raw = getattr(sock, "_sock", sock)
        stream = _DockerStream(
            raw,
            stderr_sink=meta["stderr_log"].append,
            max_line=get_settings().max_line_bytes,
        )
        return _ContainerTransport(container, stream)

    # -- diagnostic views over the transports -----------------------------

    @property
    def _containers(self) -> dict[str, Any]:
        with self._lock:
            return {
                sid: t.container for sid, t in self._transports.items() if t.container is not None
            }

    @property
    def _streams(self) -> dict[str, Any]:
        with self._lock:
            return {sid: t.stream for sid, t in self._transports.items() if t.stream is not None}


def _container_exit_code(container: Any) -> int | None:
    """Exit code of a container whose stream reached EOF (or None)."""
    if container is None:
        return None
    try:
        container.reload()
        return container.attrs.get("State", {}).get("ExitCode")
    except Exception:
        return None


def get_runner() -> Runner:
    settings = get_settings()
    if settings.runner == "server":
        return ServerRunner()
    if settings.runner == "docker":
        return DockerRunner(secret_source=_default_secret_source)
    raise ValueError(f"unknown runner: {settings.runner!r}")


def _default_secret_source(user_id: str) -> dict[str, str]:
    """Host-side secret decryption for the docker runner's env injection.

    Opens its own session (the runner has no request scope) and returns
    the owner's ``{name: value}`` map.  The Fernet key stays in this
    trusted process; only the isolated container receives plaintext.
    """
    from ..controllers import secrets as secrets_controller
    from ..infra.db import get_session_factory

    with get_session_factory()() as db:
        return secrets_controller.decrypt_for_user(db, user_id)
