"""LocalRunner: sandbox lifecycle + harness subprocess exec."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import threading
import time

import pytest

from app.controller.runner import LocalRunner, SandboxNotFoundError


@pytest.fixture()
def runner() -> LocalRunner:
    return LocalRunner()


class TestLifecycle:
    def test_create_touch_destroy(self, runner: LocalRunner) -> None:
        sandbox = runner.create("usr_1", "cnv_1")
        assert runner.exists(sandbox)
        runner.touch(sandbox)
        runner.destroy(sandbox)
        assert not runner.exists(sandbox)

    def test_destroy_unknown_is_noop(self, runner: LocalRunner) -> None:
        runner.destroy("sbx_missing")

    def test_reap_idle(self, runner: LocalRunner) -> None:
        fresh = runner.create("usr_1", "cnv_1")
        stale = runner.create("usr_1", "cnv_2")
        runner._sandboxes[stale]["last_used"] = 0.0
        destroyed = runner.reap_idle(ttl_seconds=60)
        assert destroyed == [stale]
        assert runner.exists(fresh)


class TestExecRun:
    def test_sandbox_required(self, runner: LocalRunner) -> None:
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run("sbx_missing", "hi", "run_1")

    def test_runs_harness_with_args(self, tmp_path) -> None:
        script = tmp_path / "fake_harness.py"
        script.write_text(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                json.dump({"argv": args}, sys.stdout)
                """
            )
        )
        runner = LocalRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        result = runner.exec_run(sandbox, "hello world", "run_9", timeout=30)
        assert result.exit_code == 0
        argv = json.loads(result.stdout)["argv"]
        assert argv[:3] == ["headless", "hello world", "--json"]
        assert "--run-id" in argv and "run_9" in argv

    def test_on_line_callback(self, tmp_path) -> None:
        script = tmp_path / "echo_harness.py"
        script.write_text("print('line-one'); print('line-two')")
        runner = LocalRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        lines: list[str] = []
        sandbox = runner.create("u", "c")
        result = runner.exec_run(sandbox, "p", "r", on_line=lines.append, timeout=30)
        assert lines == ["line-one", "line-two"]
        assert result.exit_code == 0

    def test_broken_callback_ignored(self, tmp_path) -> None:
        script = tmp_path / "echo_harness.py"
        script.write_text("print('ok')")
        runner = LocalRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")

        def boom(_line: str) -> None:
            raise RuntimeError("subscriber died")

        result = runner.exec_run(sandbox, "p", "r", on_line=boom, timeout=30)
        assert result.exit_code == 0

    def test_stderr_captured(self, tmp_path) -> None:
        script = tmp_path / "noisy.py"
        script.write_text("import sys; sys.stderr.write('warn\\n'); print('out')")
        runner = LocalRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        result = runner.exec_run(sandbox, "p", "r", timeout=30)
        assert result.stderr == "warn\n"
        assert result.stdout == "out\n"

    def test_timeout_kills(self, tmp_path) -> None:
        script = tmp_path / "sleeper.py"
        script.write_text("import time; time.sleep(30)")
        runner = LocalRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        result = runner.exec_run(sandbox, "p", "r", timeout=1)
        assert result.timed_out is True

    def test_missing_binary(self) -> None:
        runner = LocalRunner()
        runner._harness.cmd = "/nonexistent/harness-binary-xyz"
        sandbox = runner.create("u", "c")
        result = runner.exec_run(sandbox, "p", "r", timeout=5)
        assert result.exit_code is None
        assert "exec failed" in result.stderr

    def test_budget_flags_forwarded(self, tmp_path) -> None:
        script = tmp_path / "fake_harness.py"
        script.write_text("import json, sys; json.dump({'argv': sys.argv[1:]}, sys.stdout)")
        runner = LocalRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        runner._harness.max_rounds = 7
        runner._harness.timeout = 42.0
        sandbox = runner.create("u", "c")
        result = runner.exec_run(sandbox, "p", "r", timeout=30)
        argv = json.loads(result.stdout)["argv"]
        assert argv[argv.index("--max-rounds") + 1] == "7"
        assert argv[argv.index("--timeout") + 1] == "42.0"


class TestCancel:
    def test_cancel_delivered_to_live_process(self, tmp_path) -> None:
        """SIGINT reaches the harness; the process actually exits."""
        script = tmp_path / "sigint_harness.py"
        script.write_text(
            textwrap.dedent(
                """
                import signal, sys, time
                signal.signal(signal.SIGINT, lambda *_: (print("cancelled"),
                                                         sys.exit(130)))
                print("ready", flush=True)
                time.sleep(30)
                """
            )
        )
        runner = LocalRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        started = threading.Event()
        result_box: dict = {}

        def _run() -> None:
            started.set()  # wait until child prints "ready" instead
            result = runner.exec_run(sandbox, "p", "r", timeout=15)
            result_box["result"] = result

        # start exec on a thread, cancel once the child is up
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        deadline = time.time() + 10
        while time.time() < deadline and "r" not in runner._live_procs:
            time.sleep(0.02)
        assert runner.cancel(sandbox, "r") is True
        thread.join(timeout=15)
        result = result_box["result"]
        assert result.exit_code == 130
        assert "cancelled" in result.stdout

    def test_cancel_before_start_returns_false(self, runner: LocalRunner) -> None:
        assert runner.cancel("sbx_missing", "run_x") is False

    def test_cancel_after_exit_returns_false(self, tmp_path) -> None:
        script = tmp_path / "quick.py"
        script.write_text("print('done')")
        runner = LocalRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        runner.exec_run(sandbox, "p", "r", timeout=10)
        assert runner.cancel(sandbox, "r") is False

    def test_cancel_signal_race_returns_false(self, monkeypatch) -> None:
        """Process exits between the poll() check and the signal call."""

        def _raise(pgid: int, sig: int) -> None:
            raise ProcessLookupError

        runner = LocalRunner()
        runner._sandboxes["sbx_x"] = {"last_used": time.time()}
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
        try:
            with runner._lock:
                runner._live_procs["r"] = proc
            monkeypatch.setattr("app.controller.runner.os.getpgid", lambda pid: proc.pid)
            monkeypatch.setattr("app.controller.runner.os.killpg", _raise)
            assert runner.cancel("sbx_x", "r") is False
        finally:
            proc.kill()
            proc.wait()
