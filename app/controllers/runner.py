"""Sandbox runners: where the (untrusted) harness process executes.

``Runner`` is the sandbox-manager contract: create a sandbox, exec a
run inside it, destroy it.  ``ServerRunner`` implements the contract
with one resident ``python-agent-harness serve`` process per sandbox,
spoken to over the bidirectional JSONL protocol; ``DockerRunner``
implements the same interface against a per-sandbox container so the
trust boundary becomes real isolation (no host env, no host
filesystem, no network by default, resource-capped).
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
    is byte-identical to one with no budgets at all — runs stay
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
        reaper is free to destroy a sandbox that is stale by age — which
        is exactly the state of a sandbox whose conversation the user is
        returning to.  Touching at claim time keeps the reaper off it.

        No-op by default (a runner with no idle tracking has nothing to
        reset).
        """

    def exists(self, sandbox_id: str) -> bool:
        """Whether this runner still holds live state for *sandbox_id*.

        The sandbox registry in the DB outlives the process that owned
        the sandbox, so a stored ``running`` row is only trustworthy if
        the runner still backs it.  Defaults to True: a runner that
        keeps no per-sandbox state cannot contradict the registry, so
        the row is trusted (the historical behaviour).  The resident
        runners override this with their real in-memory view.
        """
        return True

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

    def reap_idle(self, ttl_seconds: float) -> list[str]:
        """Destroy sandboxes idle longer than TTL; return destroyed ids.

        Implementations MUST skip a sandbox with a live run regardless
        of its idle age: ``last_used`` is only touched when a run
        starts, so a long run looks increasingly idle while it is
        working, and reaping it would kill the resident process
        mid-run — losing the conversation's history and the run's
        usage.  The TTL bounds idleness; it must never bound a run.
        """
        raise NotImplementedError


class ServerRunner(Runner):
    """Resident runner: one long-lived ``harness serve`` process per
    sandbox, spoken to over the bidirectional JSONL protocol.

    The resident process keeps conversation history between turns
    (multi-turn memory), avoids the per-turn interpreter spawn, and can
    receive mid-run answers (``deliver_answer``) and cancels as
    protocol messages instead of signals.  The trust boundary is
    unchanged: the harness still runs as its own (containerizable)
    process.

    ``exec_run`` submits the prompt, streams stdout JSON lines through
    *on_line*, and returns when the harness's ``result`` line arrives
    (the process stays alive for the next turn).  A host-side timeout
    falls back to killing the process, which surfaces as a failed run.
    """

    def __init__(self, harness: HarnessSettings | None = None) -> None:
        self._harness = harness or get_settings().harness
        self._sandboxes: dict[str, dict[str, Any]] = {}
        self._procs: dict[str, subprocess.Popen] = {}  # sandbox_id -> proc
        self._locks: dict[str, threading.Lock] = {}  # sandbox_id -> write lock
        self._live_run: dict[str, str | None] = {}  # sandbox_id -> active run_id
        self._lock = threading.Lock()
        # One exec per sandbox at a time: the live-run check in
        # exec_run is check-then-act, so concurrent execs on the SAME
        # sandbox (two turns racing) could both pass it and double-spawn.
        self._exec_locks: dict[str, threading.Lock] = {}

    # -- sandbox lifecycle ------------------------------------------------

    def _command(self) -> list[str]:
        cmd = shlex.split(self._harness.cmd)
        cmd += ["serve"]
        cmd += serve_flags(self._harness)
        return cmd

    def _workspace_dir(self, conversation_id: str) -> str:
        return str(conversation_workspace(conversation_id))

    def _spawn(self, sandbox_id: str, meta: dict[str, Any]) -> subprocess.Popen:
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
        return proc

    def create(self, user_id: str, conversation_id: str) -> str:
        sandbox_id = f"sbx_{uuid.uuid4().hex}"
        meta = {
            "user_id": user_id,
            "conversation_id": conversation_id,
            "workspace": self._workspace_dir(conversation_id),
            "created_ts": time.time(),
            "last_used": time.time(),
            # per-process stderr tail (bounded); drained by one thread
            # per process — see _ensure_err_drain
            "stderr_log": deque(maxlen=200),
        }
        with self._lock:
            self._sandboxes[sandbox_id] = meta
        # spawn eagerly so the first exec does not pay the interpreter
        # startup latency; a failed spawn is tolerated here and retried
        # (surfacing its OSError) at exec time.  The ready line is left
        # in the pipe for the first exec's handshake.
        try:
            proc = self._spawn(sandbox_id, meta)
        except OSError:
            proc = None  # type: ignore[assignment]
        with self._lock:
            if proc is not None:
                self._procs[sandbox_id] = proc
                self._locks[sandbox_id] = threading.Lock()
                self._live_run[sandbox_id] = None
                self._exec_locks[sandbox_id] = threading.Lock()
        if proc is not None:
            self._ensure_err_drain(meta, proc)
        return sandbox_id

    def _ensure_err_drain(self, meta: dict[str, Any], proc: subprocess.Popen) -> None:
        """One stderr drainer per PROCESS, not per exec.

        A resident process never EOFs its stderr between turns, so a
        per-exec readline thread would leak one blocked thread per turn
        (LocalRunner could join its pumps because the process died;
        here the process lives on).  The drainer appends to a bounded
        buffer on the sandbox meta and dies with the process.
        """
        assert proc.stderr is not None
        if meta.get("_err_drain_for") == id(proc):
            return
        meta["_err_drain_for"] = id(proc)

        def _drain() -> None:
            assert proc.stderr is not None
            try:
                for chunk in iter(proc.stderr.readline, ""):
                    meta["stderr_log"].append(chunk)
            except (ValueError, OSError):
                pass  # stream closed at teardown

        threading.Thread(target=_drain, daemon=True, name="serve-stderr").start()

    def _detach(self, sandbox_id: str) -> subprocess.Popen | None:
        """Remove all registry state for the sandbox, under the lock.

        Returns the process to tear down, or None when there was none.
        Split out so the decision to retire a sandbox and its removal
        from the registry are a single atomic step -- see
        ``_detach_if_idle``.

        Clears every map unconditionally rather than bailing out when
        the meta is already gone: the maps can briefly disagree (a
        respawn re-registers a process while a concurrent sweep removes
        the meta), and a ``destroy`` that skipped the teardown in that
        state would leak the process for good.
        """
        with self._lock:
            self._sandboxes.pop(sandbox_id, None)
            proc = self._procs.pop(sandbox_id, None)
            self._locks.pop(sandbox_id, None)
            self._live_run.pop(sandbox_id, None)
            self._exec_locks.pop(sandbox_id, None)
        return proc

    def _detach_if(self, sandbox_id: str, predicate: Any) -> tuple[bool, subprocess.Popen | None]:
        """Detach only when ``predicate(meta, live_run_id)`` holds.

        Returns ``(detached, proc)``; ``detached`` is False when the
        sandbox was left alone, which is distinct from detaching one
        that simply had no process (a failed spawn at create).

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
            proc = self._procs.pop(sandbox_id, None)
            self._locks.pop(sandbox_id, None)
            self._live_run.pop(sandbox_id, None)
            self._exec_locks.pop(sandbox_id, None)
        return True, proc

    def _detach_if_idle(
        self, sandbox_id: str, ttl_seconds: float, now: float
    ) -> tuple[bool, subprocess.Popen | None]:
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

    def _teardown(self, proc: subprocess.Popen | None) -> None:
        """Stop a detached process: graceful stdin close, then kill."""
        if proc is None:
            return
        # graceful first (stdin close ends serve_forever), then kill
        with contextlib.suppress(ValueError, OSError):
            if proc.stdin is not None:
                proc.stdin.close()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=5)
        for stream in (proc.stdout, proc.stderr):
            if stream is None:  # pragma: no cover - Popen with PIPE
                continue
            with contextlib.suppress(ValueError, OSError):
                stream.close()

    def destroy(self, sandbox_id: str) -> None:
        self._teardown(self._detach(sandbox_id))

    def exists(self, sandbox_id: str) -> bool:
        with self._lock:
            return sandbox_id in self._sandboxes

    def touch(self, sandbox_id: str) -> None:
        with self._lock:
            if sandbox_id in self._sandboxes:
                self._sandboxes[sandbox_id]["last_used"] = time.time()

    def cancel(self, sandbox_id: str, run_id: str) -> bool:
        """Protocol-level cancel: an ``op:cancel`` line to the resident
        process (no signals).  False when the run is not live here."""
        with self._lock:
            proc = self._procs.get(sandbox_id)
            live = self._live_run.get(sandbox_id)
            lock = self._locks.get(sandbox_id)
        if proc is None or live != run_id or proc.poll() is not None:
            return False
        self.touch(sandbox_id)
        return self._send(proc, lock, {"op": "cancel", "run_id": run_id})

    def destroy_if_running(self, sandbox_id: str, run_id: str) -> bool:
        """Destroy the sandbox only while *run_id* still owns it.

        Ownership is checked inside the lock that removes the sandbox,
        so the sandbox cannot change hands between the check and the
        teardown — which is the whole point, since the cancelled run
        may finalize and the conversation's next turn may claim the
        same resident sandbox at any moment.
        """
        detached, proc = self._detach_if(sandbox_id, lambda meta, live: live == run_id)
        self._teardown(proc)
        return detached

    def deliver_answer(self, sandbox_id: str, run_id: str, answers: list[str]) -> bool:
        """Deliver the user's answer to a pending mid-run question.

        False when the run is not live in this sandbox (the caller maps
        that to 409/404); a protocol-level "no pending question" is
        still a delivery attempt — the harness answers with an error
        line, which the event stream relays.
        """
        with self._lock:
            proc = self._procs.get(sandbox_id)
            live = self._live_run.get(sandbox_id)
            lock = self._locks.get(sandbox_id)
        if proc is None or live != run_id or proc.poll() is not None:
            return False
        self.touch(sandbox_id)
        return self._send(proc, lock, {"op": "answer", "run_id": run_id, "answers": answers})

    def reap_idle(self, ttl_seconds: float) -> list[str]:
        now = time.time()
        destroyed: list[str] = []
        with self._lock:
            candidates = list(self._sandboxes)
        for sandbox_id in candidates:
            # The idle/stale decision is re-made INSIDE the lock that
            # removes the sandbox (see _detach_if_idle), so a run that
            # claims and touches a stale sandbox concurrently keeps it.
            # A live run is skipped regardless of age: last_used is only
            # touched at run start, so a long run looks ever MORE idle
            # while it works, and tearing it down would kill it mid-run.
            detached, proc = self._detach_if_idle(sandbox_id, ttl_seconds, now)
            if not detached:
                continue
            destroyed.append(sandbox_id)
            self._teardown(proc)
        return destroyed

    # -- process I/O --------------------------------------------------------

    def _send(self, proc: subprocess.Popen, lock: threading.Lock | None, op: dict) -> bool:
        """Write one op line to the resident process (serialized).

        False when the process is already dead (write raises) — the
        caller reports the failure instead of waiting forever.
        """
        if lock is None:
            lock = threading.Lock()
        try:
            with lock:
                assert proc.stdin is not None
                proc.stdin.write(json.dumps(op) + "\n")
                proc.stdin.flush()
        except (ValueError, OSError):
            return False
        return True

    # -- run execution ------------------------------------------------------

    def _read_ready(self, proc: subprocess.Popen, timeout: float = 30.0) -> bool:
        """Consume the resident process's ``ready`` line.

        Called right after spawn (before the first submit): the ready
        handshake guarantees the interpreter + harness imported cleanly
        before the first op is sent.

        A watchdog kills the process when the deadline passes: readline
        blocks with no data, so only closing the pipe (via kill) can
        unblock a startup hang (bad config, missing module, ...).
        """
        assert proc.stdout is not None
        ready = threading.Event()

        def _kill_on_deadline() -> None:
            if not ready.wait(timeout):
                with contextlib.suppress(OSError):
                    proc.kill()

        watchdog = threading.Thread(target=_kill_on_deadline, daemon=True)
        watchdog.start()
        try:
            while True:
                line = proc.stdout.readline()
                if not line:  # EOF: died before ready
                    return False
                try:
                    payload = json.loads(line)
                except ValueError:
                    continue
                if isinstance(payload, dict) and payload.get("type") == "ready":
                    return True
        finally:
            ready.set()  # retire the watchdog either way

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
            proc = self._procs.get(sandbox_id)
            lock = self._locks.get(sandbox_id)
            live_slot = self._live_run.get(sandbox_id)
            exec_lock = self._exec_locks.get(sandbox_id)
        if meta is None or exec_lock is None:
            # exec_lock is missing only when create()'s spawn also
            # failed; both mean the sandbox cannot take ops
            raise SandboxNotFoundError(sandbox_id)
        # Serialize the whole exec (live-check, respawn, submit, pump)
        # per sandbox: the live-run check alone is check-then-act and
        # racy under concurrent execs on the same sandbox.
        with exec_lock:
            return self._exec_run_locked(
                sandbox_id, prompt, run_id, on_line, timeout, meta, proc, lock, live_slot
            )

    def _exec_run_locked(
        self,
        sandbox_id: str,
        prompt: str,
        run_id: str,
        on_line: Any,
        timeout: float | None,
        meta: dict[str, Any],
        proc: subprocess.Popen | None,
        lock: threading.Lock | None,
        live_slot: str | None,
    ) -> ExecResult:
        if live_slot is not None:
            raise RuntimeError(f"sandbox {sandbox_id} already has a live run")
        # respawn a crashed/never-started process; a fresh process must
        # pass the ready handshake before it can take ops.  A process
        # spawned eagerly at create() still has its ready line pending.
        if proc is None or proc.poll() is not None:
            try:
                proc = self._spawn(sandbox_id, meta)
            except OSError as exc:
                return ExecResult(exit_code=None, stdout="", stderr=f"exec failed: {exc}")
            meta["stderr_log"].clear()
            self._ensure_err_drain(meta, proc)
            if not self._read_ready(proc):
                return ExecResult(
                    exit_code=None, stdout="", stderr="harness process died before ready"
                )
            with self._lock:
                vanished = sandbox_id not in self._sandboxes
                if not vanished:
                    self._procs[sandbox_id] = proc
                    self._locks[sandbox_id] = threading.Lock()
                    lock = self._locks[sandbox_id]
            if vanished:
                # The sandbox was destroyed while we were spawning and
                # waiting for `ready` (the live-run slot is only claimed
                # below, so a sweep or an explicit destroy could still
                # take it).  Re-registering here would leave a process
                # in _procs with no meta -- an entry nothing owns.  Tear
                # down the process we just started, clear any leftover
                # entries for the id, and report the sandbox as gone;
                # the caller retires the row and the next turn rebuilds.
                self._teardown(proc)
                self._teardown(self._detach(sandbox_id))
                raise SandboxNotFoundError(sandbox_id)
            # the freshly respawned process has now passed its ready
            # handshake; mark it so a later exec on this live process does
            # not call _read_ready again (its single ready line is gone,
            # and a second read would block until the watchdog kills it)
            meta["ready_ok"] = True
        elif not meta.get("ready_ok"):
            if not self._read_ready(proc):
                return ExecResult(
                    exit_code=None, stdout="", stderr="harness process died before ready"
                )
            meta["ready_ok"] = True
        self.touch(sandbox_id)
        # stderr diagnostics: snapshot position in the per-process
        # drainer's buffer, so this exec only reports ITS OWN stderr
        err_from = len(meta["stderr_log"])
        self._claim_live_run(sandbox_id, run_id)
        try:
            if proc.poll() is not None or not self._send(
                proc, lock, {"op": "submit", "prompt": prompt, "run_id": run_id}
            ):
                return ExecResult(
                    exit_code=None, stdout="", stderr="harness process died before submit"
                )
            stdout_lines, timed_out, saw_result = self._pump_until_result(
                proc,
                on_line,
                timeout,
                cancel_op=lambda: self._send(proc, lock, {"op": "cancel", "run_id": run_id}),
            )
            if not saw_result and not timed_out:
                # EOF without a result line: the process died mid-run
                # (write success was a race with exit).  Surface death,
                # with the real exit code -- poll() here would race the
                # child's reaping and often report None (see
                # exit_status).
                return ExecResult(
                    exit_code=exit_status(proc),
                    stdout="".join(stdout_lines),
                    stderr="".join(list(meta["stderr_log"])[err_from:])
                    or "harness process died mid-run",
                )
        finally:
            self._release_live_run(sandbox_id)
        return ExecResult(
            exit_code=0 if not timed_out else None,
            stdout="".join(stdout_lines),
            stderr="".join(list(meta["stderr_log"])[err_from:]),
            timed_out=timed_out,
        )

    def _pump_until_result(
        self,
        proc: subprocess.Popen,
        on_line: Any,
        timeout: float | None,
        cancel_op: Any = None,
    ) -> tuple[list[str], bool, bool]:
        """Read stdout lines until the run's ``result`` line (or death).

        The resident process stays alive after the result (next turn
        reuses it), so we cannot wait() — the run's terminal marker is
        its result line.  Returns ``(stdout_lines, timed_out, saw_result)``;
        ``saw_result`` comes from parsing, never substring matching (a
        raw line echoing result-JSON must not count).

        stderr is drained per process (see ``_ensure_err_drain``), not
        here: the resident process outlives the exec, so a per-exec
        reader would block forever and leak one thread per turn.

        The deadline is enforced by a watchdog: readline blocks with no
        data, so polling the clock around it could never recover.  The
        watchdog first sends the protocol cancel (graceful: the run
        unwinds, history is retained) and kills only if the process
        ignores it.
        """
        stdout_lines: list[str] = []
        saw_result = False
        assert proc.stdout is not None
        done = threading.Event()  # result line seen
        timed_out = threading.Event()  # watchdog fired

        if timeout is not None:

            def _kill_on_deadline() -> None:
                if not done.wait(timeout):
                    timed_out.set()
                    # graceful first: the protocol cancel lets the
                    # resident process unwind its run (tools salvage,
                    # history retained for the next turn).  Kill only
                    # if it ignores the cancel.
                    if cancel_op is not None:
                        cancel_op()
                        try:
                            proc.wait(timeout=10)
                            return  # exited gracefully
                        except subprocess.TimeoutExpired:
                            pass  # ignored the cancel: escalate
                    with contextlib.suppress(OSError):
                        proc.kill()

            watchdog = threading.Thread(target=_kill_on_deadline, daemon=True)
            watchdog.start()

        while True:
            line = proc.stdout.readline()
            if not line:  # EOF: process died (crash, kill, or timeout)
                break
            line = line.rstrip("\n")
            stdout_lines.append(line + "\n")
            if on_line is not None:
                # a slow/broken subscriber must not kill the run
                with contextlib.suppress(Exception):
                    on_line(line)
            try:
                payload = json.loads(line)
            except ValueError:
                continue
            if isinstance(payload, dict) and payload.get("type") == "result":
                saw_result = True
                done.set()
                break
        return stdout_lines, timed_out.is_set(), saw_result


class _DockerStream:
    """A newline-reader / line-writer over a docker attach socket.

    Presents the same ``readline()`` / ``write(str)`` surface the
    resident-process pumping code expects from a pipe, so
    ``DockerRunner`` can reuse the ready-handshake and pump-until-result
    logic unchanged.  A non-TTY attach multiplexes stdout/stderr into
    8-byte-framed chunks; we demultiplex, routing stdout to the line
    buffer and stderr to an optional sink.

    The underlying object only needs ``recv``/``sendall``/``close`` (a
    real socket, or a fake in tests).
    """

    _HEADER = 8  # docker stream frame: [stream, 0,0,0, size(4, big-endian)]

    def __init__(self, sock: Any, stderr_sink: Any = None) -> None:
        self._sock = sock
        self._stderr_sink = stderr_sink
        self._buf = b""  # raw framed bytes not yet demultiplexed
        self._pending = b""  # demultiplexed stdout bytes awaiting a newline
        self._closed = False

    def _demux(self, chunk: bytes) -> bytes:
        """Split a raw framed chunk into stdout bytes; feed stderr to the
        sink.  Frames can straddle recv boundaries, so leftover header/
        payload bytes are retained across calls."""
        self._buf += chunk
        out = b""
        while len(self._buf) >= self._HEADER:
            stream_type = self._buf[0]
            size = int.from_bytes(self._buf[4:8], "big")
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

    def readline(self) -> str:
        """One demultiplexed stdout line ('' on EOF), like a text pipe.

        Frames can carry several lines at once, so demultiplexed stdout
        is buffered and handed back one newline-terminated line per call
        -- returning a multi-line blob would break the JSONL parser that
        reads this stream one ``result``-bearing line at a time.
        """
        while b"\n" not in self._pending:
            try:
                chunk = self._sock.recv(4096)
            except (OSError, ValueError):
                chunk = b""
            if not chunk:
                self._closed = True
                # flush any trailing partial line at EOF
                if self._pending:
                    line, self._pending = self._pending, b""
                    return line.decode("utf-8", "replace")
                return ""
            self._pending += self._demux(chunk)
        line, _, self._pending = self._pending.partition(b"\n")
        return (line + b"\n").decode("utf-8", "replace")

    def write(self, data: str) -> None:
        self._sock.sendall(data.encode("utf-8"))

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._sock.close()


class DockerRunner(Runner):
    """Isolated runner: one long-lived container per sandbox, running
    ``harness serve`` and spoken to over the same bidirectional JSONL
    protocol as ``ServerRunner`` -- but across a container boundary.

    The trust boundary is real here: the container gets no host
    environment (only ``PAW_NONINTERACTIVE`` plus the owner's decrypted
    secrets, injected host-side at create), no host filesystem beyond
    the conversation's own workspace mount, no network by default, a
    read-only rootfs, dropped Linux capabilities, a non-root user, and
    hard memory/cpu/pid ceilings.  A hostile prompt therefore cannot
    read ``paw.db``, the app secret key, or another tenant's data, and a
    wedged run is resource-bounded.

    The resident model is unchanged from ``ServerRunner``: the container
    survives between turns (multi-turn memory), cancel/answer are
    protocol lines written to its stdin, and ``exec_run`` streams stdout
    JSONL through *on_line* until the ``result`` line.
    """

    def __init__(self, client: Any = None, secret_source: Any = None) -> None:
        # client / secret_source are injectable for tests; production
        # builds a real docker client lazily (so the SDK is only needed
        # when PAW_RUNNER=docker) and decrypts secrets host-side.
        self._client = client
        self._secret_source = secret_source
        self._sandboxes: dict[str, dict[str, Any]] = {}
        self._streams: dict[str, _DockerStream] = {}
        self._containers: dict[str, Any] = {}
        self._locks: dict[str, threading.Lock] = {}  # per-sandbox write lock
        self._live_run: dict[str, str | None] = {}
        self._exec_locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

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

    # -- sandbox lifecycle ------------------------------------------------

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

    def _spawn(self, meta: dict[str, Any]) -> tuple[Any, _DockerStream]:
        client = self._docker()
        container = client.containers.create(**self._create_kwargs(meta))
        container.start()
        sock = container.attach_socket(params={"stdin": 1, "stdout": 1, "stderr": 1, "stream": 1})
        # SDK wraps the raw socket; unwrap to the object with recv/sendall
        raw = getattr(sock, "_sock", sock)
        stream = _DockerStream(raw, stderr_sink=meta["stderr_log"].append)
        return container, stream

    def create(self, user_id: str, conversation_id: str) -> str:
        sandbox_id = f"sbx_{uuid.uuid4().hex}"
        meta = {
            "sandbox_id": sandbox_id,
            "user_id": user_id,
            "conversation_id": conversation_id,
            "workspace": str(conversation_workspace(conversation_id)),
            "created_ts": time.time(),
            "last_used": time.time(),
            "stderr_log": deque(maxlen=200),
        }
        with self._lock:
            self._sandboxes[sandbox_id] = meta
        try:
            container, stream = self._spawn(meta)
        except Exception:
            # tolerate a failed spawn like ServerRunner: retried at exec
            container, stream = None, None
        with self._lock:
            if container is not None and stream is not None:
                self._containers[sandbox_id] = container
                self._streams[sandbox_id] = stream
                self._locks[sandbox_id] = threading.Lock()
                self._live_run[sandbox_id] = None
                self._exec_locks[sandbox_id] = threading.Lock()
        return sandbox_id

    def _container_alive(self, container: Any) -> bool:
        if container is None:
            return False
        try:
            container.reload()
        except Exception:
            return False
        return getattr(container, "status", None) == "running"

    def _detach(self, sandbox_id: str) -> tuple[Any, _DockerStream | None]:
        """Remove all registry state for the sandbox, under the lock."""
        with self._lock:
            self._sandboxes.pop(sandbox_id, None)
            container = self._containers.pop(sandbox_id, None)
            stream = self._streams.pop(sandbox_id, None)
            self._locks.pop(sandbox_id, None)
            self._live_run.pop(sandbox_id, None)
            self._exec_locks.pop(sandbox_id, None)
        return container, stream

    def _detach_if(self, sandbox_id: str, predicate: Any) -> tuple[bool, Any, _DockerStream | None]:
        """Detach only when ``predicate(meta, live_run_id)`` holds.

        Mirrors ``ServerRunner._detach_if``: the predicate runs inside
        the lock that removes the sandbox, so it cannot change hands
        between the decision and the teardown.
        """
        with self._lock:
            meta = self._sandboxes.get(sandbox_id)
            if meta is None or not predicate(meta, self._live_run.get(sandbox_id)):
                return False, None, None
            self._sandboxes.pop(sandbox_id, None)
            container = self._containers.pop(sandbox_id, None)
            stream = self._streams.pop(sandbox_id, None)
            self._locks.pop(sandbox_id, None)
            self._live_run.pop(sandbox_id, None)
            self._exec_locks.pop(sandbox_id, None)
        return True, container, stream

    def _detach_if_idle(
        self, sandbox_id: str, ttl_seconds: float, now: float
    ) -> tuple[bool, Any, _DockerStream | None]:
        """``_detach``, but only while idle AND stale (see ServerRunner)."""
        return self._detach_if(
            sandbox_id,
            lambda meta, live: (
                live is None and now - float(meta.get("last_used", 0.0)) > ttl_seconds
            ),
        )

    def _claim_live_run(self, sandbox_id: str, run_id: str) -> None:
        """Take the live-run slot, or report the sandbox gone."""
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

    def _teardown(self, container: Any, stream: _DockerStream | None) -> None:
        """Stop and remove a detached container."""
        if stream is not None:
            stream.close()
        if container is not None:
            timeout = get_settings().docker.stop_timeout
            with contextlib.suppress(Exception):
                container.stop(timeout=timeout)
            with contextlib.suppress(Exception):
                container.remove(force=True)

    def destroy(self, sandbox_id: str) -> None:
        self._teardown(*self._detach(sandbox_id))

    def exists(self, sandbox_id: str) -> bool:
        with self._lock:
            return sandbox_id in self._sandboxes

    def touch(self, sandbox_id: str) -> None:
        with self._lock:
            if sandbox_id in self._sandboxes:
                self._sandboxes[sandbox_id]["last_used"] = time.time()

    def cancel(self, sandbox_id: str, run_id: str) -> bool:
        with self._lock:
            stream = self._streams.get(sandbox_id)
            live = self._live_run.get(sandbox_id)
            lock = self._locks.get(sandbox_id)
        if stream is None or live != run_id:
            return False
        self.touch(sandbox_id)
        return self._send(stream, lock, {"op": "cancel", "run_id": run_id})

    def destroy_if_running(self, sandbox_id: str, run_id: str) -> bool:
        """Destroy the container only while *run_id* still owns it."""
        detached, container, stream = self._detach_if(sandbox_id, lambda meta, live: live == run_id)
        self._teardown(container, stream)
        return detached

    def deliver_answer(self, sandbox_id: str, run_id: str, answers: list[str]) -> bool:
        with self._lock:
            stream = self._streams.get(sandbox_id)
            live = self._live_run.get(sandbox_id)
            lock = self._locks.get(sandbox_id)
        if stream is None or live != run_id:
            return False
        self.touch(sandbox_id)
        return self._send(stream, lock, {"op": "answer", "run_id": run_id, "answers": answers})

    def reap_idle(self, ttl_seconds: float) -> list[str]:
        now = time.time()
        destroyed: list[str] = []
        with self._lock:
            candidates = list(self._sandboxes)
        for sandbox_id in candidates:
            detached, container, stream = self._detach_if_idle(sandbox_id, ttl_seconds, now)
            if not detached:
                continue  # live run, or freshly claimed (see ServerRunner)
            destroyed.append(sandbox_id)
            self._teardown(container, stream)
        return destroyed

    # -- process I/O ------------------------------------------------------

    def _send(self, stream: _DockerStream | None, lock: threading.Lock | None, op: dict) -> bool:
        if stream is None:
            return False
        if lock is None:
            lock = threading.Lock()
        try:
            with lock:
                stream.write(json.dumps(op) + "\n")
        except (ValueError, OSError):
            return False
        return True

    def _read_ready(self, stream: _DockerStream, timeout: float = 30.0) -> bool:
        """Consume the container's ``ready`` line before the first op.

        readline blocks on the socket with no data, so a watchdog closes
        the stream on deadline to unblock a startup hang (bad image,
        import error, ...).
        """
        ready = threading.Event()

        def _close_on_deadline() -> None:
            if not ready.wait(timeout):
                stream.close()

        watchdog = threading.Thread(target=_close_on_deadline, daemon=True)
        watchdog.start()
        try:
            while True:
                line = stream.readline()
                if not line:
                    return False
                try:
                    payload = json.loads(line)
                except ValueError:
                    continue
                if isinstance(payload, dict) and payload.get("type") == "ready":
                    return True
        finally:
            ready.set()

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
        # exec_lock is missing only when create()'s spawn also failed;
        # make one so a respawn-at-exec can still serialize.
        if exec_lock is None:
            exec_lock = threading.Lock()
            with self._lock:
                self._exec_locks[sandbox_id] = exec_lock
        with exec_lock:
            return self._exec_run_locked(sandbox_id, prompt, run_id, on_line, timeout, meta)

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
            container = self._containers.get(sandbox_id)
            stream = self._streams.get(sandbox_id)
            lock = self._locks.get(sandbox_id)
            live_slot = self._live_run.get(sandbox_id)
        if live_slot is not None:
            raise RuntimeError(f"sandbox {sandbox_id} already has a live run")
        # respawn a dead/never-started container; a fresh one must pass
        # the ready handshake before it can take ops.
        if not self._container_alive(container):
            try:
                container, stream = self._spawn(meta)
            except Exception as exc:
                return ExecResult(exit_code=None, stdout="", stderr=f"exec failed: {exc}")
            meta["stderr_log"].clear()
            if not self._read_ready(stream):
                return ExecResult(
                    exit_code=None, stdout="", stderr="harness container died before ready"
                )
            lock = threading.Lock()
            with self._lock:
                vanished = sandbox_id not in self._sandboxes
                if not vanished:
                    self._containers[sandbox_id] = container
                    self._streams[sandbox_id] = stream
                    self._locks[sandbox_id] = lock
            if vanished:
                # Destroyed while we were starting the container and
                # waiting for `ready`; re-registering would leave state
                # with no meta (see ServerRunner).  Tear it back down
                # and report the sandbox as gone.
                self._teardown(container, stream)
                self._teardown(*self._detach(sandbox_id))
                raise SandboxNotFoundError(sandbox_id)
            # respawned container passed its ready handshake; mark it so a
            # later exec on this live container does not re-read ready (its
            # single ready line is consumed) and block until the watchdog
            meta["ready_ok"] = True
        elif not meta.get("ready_ok"):
            if stream is None or not self._read_ready(stream):
                return ExecResult(
                    exit_code=None, stdout="", stderr="harness container died before ready"
                )
            meta["ready_ok"] = True
        if stream is None:
            # alive container with no stream handle is a corrupt state;
            # fail the run clearly rather than proceed with None
            return ExecResult(exit_code=None, stdout="", stderr="sandbox stream unavailable")
        self.touch(sandbox_id)
        err_from = len(meta["stderr_log"])
        self._claim_live_run(sandbox_id, run_id)
        try:
            if not self._send(stream, lock, {"op": "submit", "prompt": prompt, "run_id": run_id}):
                return ExecResult(
                    exit_code=None, stdout="", stderr="harness container died before submit"
                )
            stdout_lines, timed_out, saw_result = self._pump_until_result(
                stream,
                container,
                on_line,
                timeout,
                cancel_op=lambda: self._send(stream, lock, {"op": "cancel", "run_id": run_id}),
            )
            if not saw_result and not timed_out:
                return ExecResult(
                    exit_code=_container_exit_code(container),
                    stdout="".join(stdout_lines),
                    stderr="".join(list(meta["stderr_log"])[err_from:])
                    or "harness container died mid-run",
                )
        finally:
            self._release_live_run(sandbox_id)
        return ExecResult(
            exit_code=0 if not timed_out else None,
            stdout="".join(stdout_lines),
            stderr="".join(list(meta["stderr_log"])[err_from:]),
            timed_out=timed_out,
        )

    def _pump_until_result(
        self,
        stream: _DockerStream,
        container: Any,
        on_line: Any,
        timeout: float | None,
        cancel_op: Any = None,
    ) -> tuple[list[str], bool, bool]:
        """Read stdout lines until the run's ``result`` line (or death).

        Mirrors ``ServerRunner._pump_until_result``: the container is
        resident, so the run's terminal marker is its parsed ``result``
        line, never a substring match.  On timeout the watchdog sends
        the protocol cancel first (graceful unwind, history retained)
        and kills the container only if the cancel is ignored.
        """
        stdout_lines: list[str] = []
        saw_result = False
        done = threading.Event()
        timed_out = threading.Event()

        if timeout is not None:

            def _kill_on_deadline() -> None:
                if not done.wait(timeout):
                    timed_out.set()
                    if cancel_op is not None:
                        cancel_op()
                        if done.wait(10):
                            return  # unwound gracefully
                    with contextlib.suppress(Exception):
                        if container is not None:
                            container.kill()
                    stream.close()

            threading.Thread(target=_kill_on_deadline, daemon=True).start()

        while True:
            line = stream.readline()
            if not line:  # EOF: container died / killed / timed out
                break
            line = line.rstrip("\n")
            stdout_lines.append(line + "\n")
            if on_line is not None:
                with contextlib.suppress(Exception):
                    on_line(line)
            try:
                payload = json.loads(line)
            except ValueError:
                continue
            if isinstance(payload, dict) and payload.get("type") == "result":
                saw_result = True
                done.set()
                break
        return stdout_lines, timed_out.is_set(), saw_result


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
