"""Sandbox runners: where the (untrusted) harness process executes.

``Runner`` is the sandbox-manager contract: create a sandbox, exec a
run inside it, destroy it.  ``LocalRunner`` implements the contract
with plain subprocesses on this host — fine for dev and single-user;
``DockerRunner`` (later) will implement the same interface against
container images so the trust boundary becomes real isolation.
"""

from __future__ import annotations

import contextlib
import os
import shlex
import signal
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

from ..core.config import HarnessSettings, get_settings


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


class LocalRunner(Runner):
    """Subprocess-per-exec on the local host, one workspace dir per
    sandbox.  No real isolation — the trusted/untrusted boundary is a
    process boundary only; swap in DockerRunner for production."""

    def __init__(self, harness: HarnessSettings | None = None) -> None:
        self._harness = harness or get_settings().harness
        self._sandboxes: dict[str, dict[str, Any]] = {}
        self._live_procs: dict[str, subprocess.Popen] = {}
        self._lock = threading.Lock()

    # -- sandbox lifecycle ------------------------------------------------

    def create(self, user_id: str, conversation_id: str) -> str:
        sandbox_id = f"sbx_{uuid.uuid4().hex}"
        with self._lock:
            self._sandboxes[sandbox_id] = {
                "user_id": user_id,
                "conversation_id": conversation_id,
                "workspace": self._harness.cwd or os.getcwd(),
                "created_ts": time.time(),
                "last_used": time.time(),
            }
        return sandbox_id

    def destroy(self, sandbox_id: str) -> None:
        with self._lock:
            self._sandboxes.pop(sandbox_id, None)

    def exists(self, sandbox_id: str) -> bool:
        with self._lock:
            return sandbox_id in self._sandboxes

    def touch(self, sandbox_id: str) -> None:
        with self._lock:
            if sandbox_id in self._sandboxes:
                self._sandboxes[sandbox_id]["last_used"] = time.time()

    def cancel(self, sandbox_id: str, run_id: str) -> bool:
        """Signal the harness process group: SIGINT on POSIX (the
        harness's documented graceful-cancel path, giving tools a
        salvage window); CTRL_BREAK_EVENT on win32."""
        with self._lock:
            proc = self._live_procs.get(run_id)
            meta = self._sandboxes.get(sandbox_id)
        if proc is None or meta is None or proc.poll() is not None:
            return False
        self.touch(sandbox_id)
        try:
            if os.name == "posix":
                pgid = os.getpgid(proc.pid)
                if pgid == proc.pid:
                    os.killpg(pgid, signal.SIGINT)
                else:
                    proc.send_signal(signal.SIGINT)
            else:  # win32: CTRL_BREAK to the shared console group
                proc.send_signal(signal.CTRL_BREAK_EVENT)
        except (ProcessLookupError, OSError):
            return False  # process just exited; the pump will finish the run
        return True

    def reap_idle(self, ttl_seconds: float) -> list[str]:
        now = time.time()
        destroyed: list[str] = []
        with self._lock:
            for sandbox_id, meta in list(self._sandboxes.items()):
                if now - float(meta.get("last_used", 0.0)) > ttl_seconds:
                    self._sandboxes.pop(sandbox_id, None)
                    destroyed.append(sandbox_id)
        return destroyed

    # -- run execution ----------------------------------------------------

    def _command(self, prompt: str, run_id: str) -> list[str]:
        cmd = shlex.split(self._harness.cmd)
        cmd += ["headless", prompt, "--json", "--run-id", run_id]
        if self._harness.max_rounds is not None:
            cmd += ["--max-rounds", str(self._harness.max_rounds)]
        if self._harness.timeout is not None:
            cmd += ["--timeout", str(self._harness.timeout)]
        return cmd

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
        if meta is None:
            raise SandboxNotFoundError(sandbox_id)
        self.touch(sandbox_id)
        env = dict(os.environ)
        env.setdefault("PAW_NONINTERACTIVE", "1")
        try:
            proc = subprocess.Popen(  # noqa: S603 - cmd is admin-configured
                self._command(prompt, run_id),
                cwd=meta.get("workspace") or None,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                start_new_session=(os.name == "posix"),
            )
        except OSError as exc:
            return ExecResult(exit_code=None, stdout="", stderr=f"exec failed: {exc}")
        with self._lock:
            self._live_procs[run_id] = proc

        stdout_lines: list[str] = []
        stderr_chunks: list[str] = []
        timed_out = threading.Event()
        assert proc.stdout is not None and proc.stderr is not None

        def _pump(stream: Any, sink: list, callback: Any) -> None:
            # Reads until EOF (or the process is killed, which closes the
            # pipe); runs on a thread so the caller can enforce the timeout.
            for chunk in iter(stream.readline, ""):
                sink.append(chunk)
                if callback is not None:
                    # a slow/broken subscriber must not kill the run
                    with contextlib.suppress(Exception):
                        callback(chunk.rstrip("\n"))

        stdout_thread = threading.Thread(
            target=_pump, args=(proc.stdout, stdout_lines, on_line), daemon=True
        )
        stderr_thread = threading.Thread(
            target=_pump, args=(proc.stderr, stderr_chunks, None), daemon=True
        )
        stdout_thread.start()
        stderr_thread.start()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out.set()
            proc.kill()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=5)
        finally:
            with self._lock:
                self._live_procs.pop(run_id, None)
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        for stream in (proc.stdout, proc.stderr):
            # a reader thread may still hold it briefly on Windows
            with contextlib.suppress(ValueError, OSError):
                stream.close()
        return ExecResult(
            exit_code=proc.returncode,
            stdout="".join(stdout_lines),
            stderr="".join(stderr_chunks),
            timed_out=timed_out.is_set(),
        )


def get_runner() -> Runner:
    settings = get_settings()
    if settings.runner == "local":
        return LocalRunner()
    raise ValueError(f"unknown runner: {settings.runner!r} (docker runner pending)")
