"""Sandbox runners: where the (untrusted) harness process executes.

``Runner`` is the sandbox-manager contract: create a sandbox, exec a
run inside it, destroy it.  ``ServerRunner`` implements the contract
with one resident ``python-agent-harness serve`` process per sandbox,
spoken to over the bidirectional JSONL protocol; ``DockerRunner``
(later) will implement the same interface against container images so
the trust boundary becomes real isolation.
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


class SandboxNotFoundError(KeyError):
    pass


class Runner:
    """Sandbox-manager interface (see README architecture)."""

    def create(self, user_id: str, conversation_id: str) -> str:
        raise NotImplementedError

    def destroy(self, sandbox_id: str) -> None:
        raise NotImplementedError

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
        """Destroy sandboxes idle longer than TTL; return destroyed ids."""
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

    def destroy(self, sandbox_id: str) -> None:
        with self._lock:
            self._sandboxes.pop(sandbox_id, None)
            proc = self._procs.pop(sandbox_id, None)
            self._locks.pop(sandbox_id, None)
            self._live_run.pop(sandbox_id, None)
            self._exec_locks.pop(sandbox_id, None)
        if proc is not None:
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
            for sandbox_id, meta in list(self._sandboxes.items()):
                if now - float(meta.get("last_used", 0.0)) > ttl_seconds:
                    destroyed.append(sandbox_id)
        for sandbox_id in destroyed:
            self.destroy(sandbox_id)
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
                self._procs[sandbox_id] = proc
                self._locks[sandbox_id] = threading.Lock()
                lock = self._locks[sandbox_id]
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
        with self._lock:
            self._live_run[sandbox_id] = run_id
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
                # (write success was a race with exit).  Surface death.
                return ExecResult(
                    exit_code=proc.poll(),
                    stdout="".join(stdout_lines),
                    stderr="".join(list(meta["stderr_log"])[err_from:])
                    or "harness process died mid-run",
                )
        finally:
            with self._lock:
                self._live_run[sandbox_id] = None
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


def get_runner() -> Runner:
    settings = get_settings()
    if settings.runner == "server":
        return ServerRunner()
    raise ValueError(f"unknown runner: {settings.runner!r} (docker runner pending)")
