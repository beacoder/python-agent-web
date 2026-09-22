"""ServerRunner: resident harness process per sandbox (serve protocol).

Uses a fake ``serve`` implementation (a small Python script speaking
the same protocol as ``python-agent-harness serve``) so the runner is
exercised end to end without an LLM key: ready → submit → result, ask
→ answer, cancel, and process lifecycle.  The real harness's protocol
contract is covered by the harness repo's own suite (entry/server).
"""

from __future__ import annotations

import json
import sys
import textwrap
import threading
import time

import pytest

from app.controllers.runner import SandboxNotFoundError, ServerRunner

# A minimal, protocol-faithful fake of `harness serve`: handles submit
# (echoes an answer result), answer (acks via a log event), cancel
# (emits a cancelled result), ping/shutdown.
FAKE_SERVE = textwrap.dedent(
    """
    import json, sys

    def out(payload):
        sys.stdout.write(json.dumps(payload) + "\\n")
        sys.stdout.flush()

    out({"type": "ready", "pid": 1234})
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        op = json.loads(line)
        name = op.get("op")
        if name == "submit":
            run_id = op["run_id"]
            out({"seq": 1, "type": "start", "prompt": op["prompt"], "warnings": [], "run_id": run_id})
            out({"seq": 2, "type": "notify", "kind": "ask", "data": {"kind": "ask", "questions": [{"question": "color?"}]}, "run_id": run_id})
            out({"seq": 3, "type": "result", "answer": "answered: " + op["prompt"],
                 "errors": [], "cancelled": False, "model": "fake", "run_id": run_id})
        elif name == "answer":
            out({"seq": 4, "type": "log", "message": "answer received: " + ",".join(op["answers"]), "run_id": op["run_id"]})
        elif name == "cancel":
            out({"type": "error", "error": "run ... is not active"})
        elif name == "ping":
            out({"type": "pong"})
        elif name == "shutdown":
            break
    """
)


@pytest.fixture()
def runner(tmp_path) -> ServerRunner:
    script = tmp_path / "fake_serve.py"
    script.write_text(FAKE_SERVE)
    runner = ServerRunner()
    runner._harness.cmd = f"{sys.executable} {script}"
    return runner


class TestLifecycle:
    def test_create_destroy(self, runner: ServerRunner) -> None:
        sandbox = runner.create("u", "c")
        assert runner.exists(sandbox)
        runner.destroy(sandbox)
        assert not runner.exists(sandbox)

    def test_reap_idle_destroys(self, runner: ServerRunner) -> None:
        fresh = runner.create("u", "c")
        stale = runner.create("u", "c2")
        with runner._lock:
            runner._sandboxes[stale]["last_used"] = 0.0
        destroyed = runner.reap_idle(ttl_seconds=60)
        assert destroyed == [stale]
        assert runner.exists(fresh)
        runner.destroy(fresh)

    def test_unknown_sandbox_raises(self, runner: ServerRunner) -> None:
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run("sbx_missing", "hi", "run_1")

    def test_destroy_kills_process(self, runner: ServerRunner) -> None:
        sandbox = runner.create("u", "c")
        proc = runner._procs[sandbox]
        runner.destroy(sandbox)
        deadline = time.time() + 10
        while time.time() < deadline and proc.poll() is None:
            time.sleep(0.05)
        assert proc.poll() is not None


class TestExecRun:
    def test_full_round_trip(self, runner: ServerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            lines: list[str] = []
            result = runner.exec_run(sandbox, "hello", "run_1", on_line=lines.append, timeout=30)
            assert result.exit_code == 0
            assert not result.timed_out
            payloads = [json.loads(line) for line in lines]
            # the ready line is consumed by create(); the exec stream
            # covers this run's lines only
            assert [p["type"] for p in payloads] == ["start", "notify", "result"]
            assert payloads[0]["run_id"] == "run_1"
            assert payloads[-1]["answer"] == "answered: hello"
            # the process survives the run (resident)
            assert runner._procs[sandbox].poll() is None
        finally:
            runner.destroy(sandbox)

    def test_two_turns_same_process(self, runner: ServerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            first = runner.exec_run(sandbox, "one", "run_1", timeout=30)
            pid = runner._procs[sandbox].pid
            second = runner.exec_run(sandbox, "two", "run_2", timeout=30)
            assert runner._procs[sandbox].pid == pid
            assert "answered: one" in first.stdout
            assert "answered: two" in second.stdout
        finally:
            runner.destroy(sandbox)

    def test_concurrent_exec_rejected(self, runner: ServerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            with runner._lock:
                runner._live_run[sandbox] = "run_busy"
            with pytest.raises(RuntimeError, match="already has a live run"):
                runner.exec_run(sandbox, "hi", "run_other", timeout=5)
        finally:
            runner.destroy(sandbox)

    def test_process_death_respawns(self, runner: ServerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            runner._procs[sandbox].kill()
            runner._procs[sandbox].wait(timeout=5)
            result = runner.exec_run(sandbox, "after-crash", "run_9", timeout=30)
            assert result.exit_code == 0
            assert "answered: after-crash" in result.stdout
        finally:
            runner.destroy(sandbox)

    def test_timeout_kills_and_reports(self, tmp_path) -> None:
        """A harness that ignores the cancel op is escalated to kill.

        The fake sleeps inside the submit branch (never reads stdin),
        so the graceful cancel cannot land: the watchdog waits the
        10s grace, kills, and the run reports timed_out with the
        start line as the only output."""
        script = tmp_path / "stuck_serve.py"
        script.write_text(
            textwrap.dedent(
                """
                import json, sys, time
                def out(p):
                    sys.stdout.write(json.dumps(p) + "\\n"); sys.stdout.flush()
                out({"type": "ready", "pid": 1})
                while True:
                    line = sys.stdin.readline()
                    if not line:
                        break
                    op = json.loads(line)
                    if op.get("op") == "submit":
                        out({"seq": 1, "type": "start", "run_id": op["run_id"], "prompt": op["prompt"], "warnings": []})
                        time.sleep(60)  # never finishes
                """
            )
        )
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        try:
            result = runner.exec_run(sandbox, "hi", "run_1", timeout=2)
            assert result.timed_out
        finally:
            runner.destroy(sandbox)


class TestMidRunOps:
    def test_cancel_live_and_dead_runs(self, runner: ServerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            assert runner.cancel(sandbox, "run_none") is False
            with runner._lock:
                runner._live_run[sandbox] = "run_1"
            assert runner.cancel(sandbox, "run_1") is True
        finally:
            runner.destroy(sandbox)
        # after destroy: no proc -> False
        assert runner.cancel(sandbox, "run_1") is False

    def test_deliver_answer_routes_op(self, runner: ServerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            assert runner.deliver_answer(sandbox, "run_none", ["x"]) is False
            with runner._lock:
                runner._live_run[sandbox] = "run_1"
            assert runner.deliver_answer(sandbox, "run_1", ["blue"]) is True
        finally:
            runner.destroy(sandbox)

    def test_answer_reaches_harness_mid_run(self, runner: ServerRunner) -> None:
        """The answer op is consumed by the harness and echoed as a log
        event on a SUBSEQUENT run's stream (proves stdin wiring)."""
        sandbox = runner.create("u", "c")
        try:
            with runner._lock:
                runner._live_run[sandbox] = "run_0"
            assert runner.deliver_answer(sandbox, "run_0", ["blue"]) is True
            with runner._lock:
                runner._live_run[sandbox] = None
            lines: list[str] = []
            runner.exec_run(sandbox, "next", "run_1", on_line=lines.append, timeout=30)
            logs = [json.loads(line) for line in lines if json.loads(line).get("type") == "log"]
            # the fake harness's run_1 result arrives after the buffered
            # "answer received" log for run_0 (seq 4 > 3)
            assert any("answer received: blue" in item.get("message", "") for item in logs)
        finally:
            runner.destroy(sandbox)


class TestRunnerEdgePaths:
    """Branch coverage: failed spawns, dead pipes, watchdog fires."""

    def test_create_spawn_failure_tolerated(self, tmp_path) -> None:
        runner = ServerRunner()
        runner._harness.cmd = "/nonexistent/harness-binary-xyz"
        sandbox = runner.create("u", "c")
        # spawn failed at create (no proc/exec_lock registered); exec
        # reports the sandbox as unusable rather than crashing
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run(sandbox, "hi", "run_1", timeout=10)
        runner.destroy(sandbox)

    def test_send_dead_process_returns_false(self, tmp_path) -> None:
        runner = ServerRunner()
        runner._harness.cmd = "/nonexistent/harness-binary-xyz"
        sandbox = runner.create("u", "c")
        runner.destroy(sandbox)
        # no proc at all: exec raises SandboxNotFound? no — create
        # registered the sandbox; destroy removed it
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run(sandbox, "hi", "run_1")

    def test_exec_after_process_death_before_ready(self, tmp_path) -> None:
        script = tmp_path / "die_fast.py"
        script.write_text("import sys; sys.exit(3)")
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        result = runner.exec_run(sandbox, "hi", "run_1", timeout=10)
        assert result.exit_code is None
        assert "died before ready" in result.stderr
        runner.destroy(sandbox)

    def test_submit_death_after_spawn(self, tmp_path) -> None:
        """Process passes ready then exits: submit must fail, not hang
        or report success."""
        script = tmp_path / "die_after_ready.py"
        script.write_text(
            "import json, sys\n"
            "sys.stdout.write(json.dumps({'type': 'ready', 'pid': 1}) + '\\n')\n"
            "sys.stdout.flush()\n"
            "sys.exit(0)\n"
        )
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        result = runner.exec_run(sandbox, "hi", "run_1", timeout=10)
        assert result.exit_code == 0  # the process's own exit code
        assert "died mid-run" in result.stderr
        assert result.stdout == ""
        runner.destroy(sandbox)

    def test_unknown_runner_setting(self, monkeypatch) -> None:
        from app.controllers import runner as runner_mod
        from app.infra.config import Settings

        monkeypatch.setattr(runner_mod, "get_settings", lambda: Settings(runner="bogus"))
        with pytest.raises(ValueError, match="unknown runner"):
            runner_mod.get_runner()


class TestBranchCompletions:
    """Cover the remaining defensive branches of ServerRunner."""

    def test_destroy_graceful_then_kill(self, tmp_path) -> None:
        """A serve that ignores stdin EOF is force-killed by destroy()."""
        script = tmp_path / "stubborn_serve.py"
        script.write_text("import time\nprint('ready-ish', flush=True)\ntime.sleep(60)")
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        proc = runner._procs[sandbox]
        runner.destroy(sandbox)
        deadline = time.time() + 10
        while time.time() < deadline and proc.poll() is None:
            time.sleep(0.05)
        assert proc.poll() is not None

    def test_send_returns_false_on_broken_pipe(self, tmp_path) -> None:
        import threading as _t

        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} -c 'import time; time.sleep(60)'"
        sandbox = runner.create("u", "c")
        proc = runner._procs[sandbox]
        proc.kill()
        proc.wait(timeout=5)
        # stdin write to a dead process raises -> False
        assert runner._send(proc, _t.Lock(), {"op": "ping"}) is False
        runner.destroy(sandbox)

    def test_read_ready_tolerates_garbage_then_eof(self, tmp_path) -> None:
        script = tmp_path / "garbage.py"
        script.write_text("print('not json'); import sys; sys.exit(0)")
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        result = runner.exec_run(sandbox, "hi", "run_1", timeout=10)
        assert result.exit_code is None
        assert "died before ready" in result.stderr
        runner.destroy(sandbox)

    def test_exec_unknown_sandbox_raises_before_any_spawn(self, tmp_path) -> None:
        runner = ServerRunner()
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run("sbx_nope", "hi", "run_1")

    def test_died_before_submit_path(self, tmp_path) -> None:
        """A dead process is reported as a failed run, never a hang.

        exec_run respawns dead processes by design; here the respawn
        dies again after ready (exit 5), so the run must surface the
        death rather than report success."""
        script = tmp_path / "die_after_ready.py"
        script.write_text(
            "import json, sys, time\n"
            "sys.stdout.write(json.dumps({'type': 'ready', 'pid': 1}) + '\\n')\n"
            "sys.stdout.flush()\n"
            "time.sleep(0.3)\n"
            "sys.exit(5)\n"
        )
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        time.sleep(0.6)  # let it print ready AND exit
        result = runner.exec_run(sandbox, "hi", "run_1", timeout=10)
        assert result.exit_code == 5  # the respawned process's exit code
        assert "died mid-run" in result.stderr
        runner.destroy(sandbox)


class TestReviewFixes:
    """Regressions for the code-review findings."""

    def test_startup_hang_is_killed_by_ready_watchdog(self, tmp_path, monkeypatch) -> None:
        """A harness that never prints ready cannot hang the handshake:
        the watchdog kills it and exec reports died-before-ready."""
        script = tmp_path / "hang_startup.py"
        script.write_text("import time; time.sleep(60)")
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        runner._procs[sandbox].kill()  # force the respawn path
        runner._procs[sandbox].wait(timeout=5)
        # the production default (30s, cold-start headroom) is too slow
        # for the suite; exercise the same watchdog with a short deadline
        monkeypatch.setattr(
            runner,
            "_read_ready",
            lambda proc: ServerRunner._read_ready(runner, proc, timeout=2),
        )
        started = time.time()
        result = runner.exec_run(sandbox, "hi", "run_1", timeout=10)
        elapsed = time.time() - started
        assert result.exit_code is None
        assert "died before ready" in result.stderr
        assert elapsed < 11  # watchdog fired, no indefinite block
        runner.destroy(sandbox)

    def test_error_line_surfaces_in_run_outcome(self) -> None:
        """A protocol error line is a first-class event, folded into the
        run's error trail (not a malformed pseudo-log)."""
        from app.controllers.protocol import parse_line, parse_stream

        event = parse_line('{"type": "error", "error": "run x is not active"}')
        assert event is not None
        assert event.type == "error"
        assert not event.malformed
        outcome_text = '{"type": "result", "answer": "a", "errors": []}\n'
        text = outcome_text + '{"type": "error", "error": "stale answer"}\n'
        from app.controllers.protocol import RunOutcome, apply_event

        outcome = RunOutcome()
        for e in parse_stream(text):
            apply_event(outcome, e)
        assert "stale answer" in outcome.errors

    def test_result_errors_merge_not_replace(self) -> None:
        """The result line's own errors must not wipe protocol errors
        folded from earlier lines (stale-answer scenario)."""
        from app.controllers.protocol import RunOutcome, apply_event, parse_stream

        text = (
            '{"type": "error", "error": "stale answer"}\n'
            '{"type": "result", "answer": "a", "errors": ["llm 500"], "cancelled": false}\n'
        )
        outcome = RunOutcome()
        for e in parse_stream(text):
            apply_event(outcome, e)
        assert outcome.errors == ["stale answer", "llm 500"]

    def test_timeout_cancels_before_kill(self, tmp_path) -> None:
        """The watchdog sends the protocol cancel first; only a process
        that ignores it is killed (history-preserving timeout).

        The fake mirrors the real serve architecture: one reader loop
        dispatching ops while the run executes on its own thread, so
        the cancel op is consumed mid-run."""
        script = tmp_path / "slow_but_polite.py"
        script.write_text(
            textwrap.dedent(
                """
                import json, sys, threading, time
                def out(p):
                    sys.stdout.write(json.dumps(p) + "\\n"); sys.stdout.flush()
                out({"type": "ready", "pid": 1})
                done = threading.Event()
                def fake_run(run_id):
                    out({"seq": 1, "type": "start", "run_id": run_id,
                         "prompt": "x", "warnings": []})
                    done.wait(60)  # "working"; unblocked by cancel
                    out({"seq": 2, "type": "result", "run_id": run_id,
                         "answer": "", "errors": [], "cancelled": True,
                         "model": "fake"})
                while True:
                    line = sys.stdin.readline()
                    if not line:
                        break
                    op = json.loads(line)
                    if op.get("op") == "submit":
                        threading.Thread(target=fake_run, args=(op["run_id"],), daemon=True).start()
                    elif op.get("op") == "cancel":
                        done.set()
                    elif op.get("op") == "shutdown":
                        break
                """
            )
        )
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        try:
            result = runner.exec_run(sandbox, "hi", "run_1", timeout=3)
            assert result.timed_out
            # the graceful cancel produced a cancelled result; the
            # watchdog did NOT escalate to kill (process survived)
            assert runner._procs[sandbox].poll() is None
            assert '"cancelled": true' in result.stdout
        finally:
            runner.destroy(sandbox)

    def test_result_substring_in_raw_line_does_not_mask_death(self, tmp_path) -> None:
        """saw_result must come from parsing: a raw (non-JSON) line
        echoing the marker text must not be taken as the run's result
        and mask a process death."""
        script = tmp_path / "echoes_result_marker.py"
        script.write_text(
            "import json, sys\n"
            "sys.stdout.write(json.dumps({'type': 'ready', 'pid': 1}) + '\\n')\n"
            "sys.stdout.flush()\n"
            "sys.stdin.readline()  # consume the submit op\n"
            'sys.stdout.write(\'echo: "type": "result" is the marker\\n\')\n'
            "sys.stdout.flush()\n"
            "sys.exit(7)\n"
        )
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        try:
            result = runner.exec_run(sandbox, "hi", "run_1", timeout=10)
            assert result.exit_code == 7  # death surfaced, not masked
            assert "died mid-run" in result.stderr
        finally:
            runner.destroy(sandbox)

    def test_stderr_is_captured_and_sliced_per_turn(self, tmp_path) -> None:
        """stderr diagnostics: one per-process drainer feeds a bounded
        buffer; each exec reports only its own slice, and the resident
        process staying alive does not lose the tail."""
        script = tmp_path / "noisy_serve.py"
        script.write_text(
            textwrap.dedent(
                """
                import json, sys, time
                def out(p):
                    sys.stdout.write(json.dumps(p) + "\\n"); sys.stdout.flush()
                out({"type": "ready", "pid": 1})
                while True:
                    line = sys.stdin.readline()
                    if not line:
                        break
                    op = json.loads(line)
                    if op.get("op") == "submit":
                        sys.stderr.write("warn:" + op["prompt"] + "\\n")
                        sys.stderr.flush()
                        time.sleep(0.3)  # let the drainer capture it
                        out({"seq": 1, "type": "result", "run_id": op["run_id"],
                             "answer": "ok", "errors": [], "cancelled": False})
                """
            )
        )
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        sandbox = runner.create("u", "c")
        try:
            first = runner.exec_run(sandbox, "one", "run_1", timeout=10)
            second = runner.exec_run(sandbox, "two", "run_2", timeout=10)
            assert "warn:one" in first.stderr
            assert "warn:two" in second.stderr
            assert "warn:one" not in second.stderr  # sliced per exec
        finally:
            runner.destroy(sandbox)

    def test_stderr_drainer_never_leaks_per_turn(self, runner: ServerRunner) -> None:
        """One drainer thread per resident process, not per exec: a
        per-exec readline thread would block forever (the resident
        process never EOFs its stderr between turns) and leak one
        thread per turn."""

        def drainers() -> list[threading.Thread]:
            return [t for t in threading.enumerate() if t.name == "serve-stderr"]

        sandbox = runner.create("u", "c")
        try:
            # drainers of processes destroyed by earlier tests exit on
            # EOF; give stragglers a moment so the count is meaningful
            deadline = time.time() + 5
            while time.time() < deadline and len(drainers()) != 1:
                time.sleep(0.02)
            assert len(drainers()) == 1  # one resident process -> one drainer
            for i in range(3):
                runner.exec_run(sandbox, f"turn {i}", f"run_{i}", timeout=30)
            assert len(drainers()) == 1  # still one after three more turns
        finally:
            runner.destroy(sandbox)
