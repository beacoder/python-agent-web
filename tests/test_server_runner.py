"""ServerRunner: resident harness process per sandbox (serve protocol).

Uses a fake ``serve`` implementation (a small Python script speaking
the same protocol as ``python-agent-harness serve``) so the runner is
exercised end to end without an LLM key: ready → submit → result, ask
→ answer, cancel, and process lifecycle.  The real harness's protocol
contract is covered by the harness repo's own suite (entry/server).
"""

from __future__ import annotations

import json
import shlex
import subprocess
import sys
import textwrap
import threading
import time

import pytest

from app.controllers.runner import SandboxNotFoundError, ServerRunner, exit_status

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

    def test_reap_idle_spares_a_sandbox_with_a_live_run(self, runner: ServerRunner) -> None:
        """A live run must survive the reaper however idle it looks.

        ``last_used`` is only touched when a run STARTS, so a long run
        looks ever more idle while it works.  Reaping it would destroy
        the resident process mid-run — killing the conversation's
        history and the run's usage — which would make the idle TTL an
        implicit run bound.
        """
        busy = runner.create("u", "c")
        with runner._lock:
            runner._sandboxes[busy]["last_used"] = 0.0  # maximally stale
            runner._live_run[busy] = "run_live"

        assert runner.reap_idle(ttl_seconds=60) == []
        assert runner.exists(busy)

        # once the run ends, the same sandbox is reapable
        with runner._lock:
            runner._live_run[busy] = None
        assert runner.reap_idle(ttl_seconds=60) == [busy]
        assert not runner.exists(busy)


class TestServeBudgetFlags:
    """Spawn-time budget flags: present only when configured.

    The harness refuses to read budgets off the wire (a submit must not
    be able to raise its own ceiling), so the spawn command line is the
    only place an operator can set them.  Defaults must stay unlimited.
    """

    def test_default_command_carries_no_budget_flags(self, runner: ServerRunner) -> None:
        cmd = runner._command()
        assert cmd[-1] == "serve"
        for flag in ("--max-rounds", "--timeout", "--answer-timeout"):
            assert flag not in cmd

    def test_configured_budgets_become_flags(self, runner: ServerRunner) -> None:
        runner._harness.max_rounds = 12
        runner._harness.sandbox_timeout = 90.0
        runner._harness.answer_timeout = 30.0
        cmd = runner._command()
        assert cmd[cmd.index("--max-rounds") + 1] == "12"
        assert cmd[cmd.index("--timeout") + 1] == "90.0"
        assert cmd[cmd.index("--answer-timeout") + 1] == "30.0"
        assert cmd.index("serve") < cmd.index("--max-rounds")

    def test_flags_are_independent(self, runner: ServerRunner) -> None:
        runner._harness.max_rounds = 5
        cmd = runner._command()
        assert "--max-rounds" in cmd
        assert "--timeout" not in cmd
        assert "--answer-timeout" not in cmd

    def test_zero_is_normalized_to_omitted(self, runner: ServerRunner) -> None:
        """0 means "unlimited" to the harness, so emit nothing.

        Passing ``--max-rounds 0`` through would read like a ceiling of
        zero on a process listing while actually meaning the opposite,
        and would break if a future harness gave 0 a meaning.
        """
        runner._harness.max_rounds = 0
        runner._harness.sandbox_timeout = 0
        runner._harness.answer_timeout = 0
        assert runner._command() == [*shlex.split(runner._harness.cmd), "serve"]

    def test_host_timeout_is_not_a_sandbox_flag(self, runner: ServerRunner) -> None:
        """``harness.timeout`` is this host's watchdog, not the sandbox's.

        Passing it through as ``--timeout`` would silently duplicate the
        budget and change what the existing setting means.
        """
        runner._harness.timeout = 42.0
        assert "--timeout" not in runner._command()

    def test_a_spawned_process_accepts_the_flags(self, tmp_path) -> None:
        """The flag names must be ones ``serve`` actually parses."""
        script = tmp_path / "flagcheck.py"
        script.write_text(
            textwrap.dedent(
                """
                import argparse, json, sys
                p = argparse.ArgumentParser()
                p.add_argument("command")
                p.add_argument("--answer-timeout", type=float, default=0.0)
                p.add_argument("--max-rounds", type=int, default=None)
                p.add_argument("--timeout", type=float, default=None)
                a = p.parse_args()
                sys.stdout.write(json.dumps({"type": "ready", "argv": vars(a)}) + "\\n")
                sys.stdout.flush()
                sys.stdin.readline()
                """
            )
        )
        runner = ServerRunner()
        runner._harness.cmd = f"{sys.executable} {script}"
        runner._harness.max_rounds = 7
        runner._harness.sandbox_timeout = 15.0
        runner._harness.answer_timeout = 5.0
        sandbox = runner.create("u", "c")
        try:
            proc = runner._procs[sandbox]
            payload = json.loads(proc.stdout.readline())
            assert payload["argv"] == {
                "command": "serve",
                "answer_timeout": 5.0,
                "max_rounds": 7,
                "timeout": 15.0,
            }
        finally:
            runner.destroy(sandbox)

    def test_unknown_sandbox_raises(self, runner: ServerRunner) -> None:
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run("sbx_missing", "hi", "run_1")

    def test_destroy_if_running_requires_ownership(self, runner: ServerRunner) -> None:
        """Teardown-on-ignored-cancel must be guarded by ownership.

        The sandbox is resident, so the cancelled run can finalize and
        the conversation's next turn can claim the same sandbox before
        escalation fires; a blind destroy would kill that successor.
        """
        sandbox = runner.create("u", "c")
        with runner._lock:
            runner._live_run[sandbox] = "run_a"

        # a different run owns it now -> decline, leave it alone
        assert runner.destroy_if_running(sandbox, "run_stale") is False
        assert runner.exists(sandbox)
        # nothing owns it -> also declined (there is nothing to release)
        with runner._lock:
            runner._live_run[sandbox] = None
        assert runner.destroy_if_running(sandbox, "run_a") is False
        assert runner.exists(sandbox)
        # the owner asks -> torn down
        with runner._lock:
            runner._live_run[sandbox] = "run_a"
        assert runner.destroy_if_running(sandbox, "run_a") is True
        assert not runner.exists(sandbox)

    def test_destroy_if_running_on_unknown_sandbox_is_a_noop(self, runner: ServerRunner) -> None:
        assert runner.destroy_if_running("sbx_missing", "run_a") is False

    def test_claim_live_run_reports_a_destroyed_sandbox(self, runner: ServerRunner) -> None:
        """Claiming must fail loudly rather than register a dead sandbox.

        ``_live_run``'s keys have to stay a subset of ``_sandboxes``':
        a key for a sandbox with no meta is never cleaned up (nothing
        will destroy that id again) and would be a slow leak.
        """
        sandbox = runner.create("u", "c")
        runner.destroy(sandbox)
        with pytest.raises(SandboxNotFoundError):
            runner._claim_live_run(sandbox, "run_1")
        assert sandbox not in runner._live_run

    def test_release_live_run_does_not_resurrect_a_dead_sandbox(self, runner: ServerRunner) -> None:
        sandbox = runner.create("u", "c")
        runner._claim_live_run(sandbox, "run_1")
        runner.destroy(sandbox)  # pops the slot along with everything else
        runner._release_live_run(sandbox)  # the exec's finally, after the fact
        assert sandbox not in runner._live_run

    def test_exec_on_a_sandbox_destroyed_just_before_the_claim(self, runner: ServerRunner) -> None:
        """The real path for the above: the sandbox can be destroyed
        between ``exec_lock`` being taken and the live-run claim (an
        escalated cancel, a sweep)."""
        sandbox = runner.create("u", "c")
        original = runner.touch

        def _destroy_then_touch(sandbox_id: str) -> None:
            runner.destroy(sandbox_id)  # vanishes right before the claim
            original(sandbox_id)

        runner.touch = _destroy_then_touch
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run(sandbox, "hi", "run_1")
        assert runner._live_run == {}  # no leaked slot

    def test_reap_loses_to_a_concurrent_claim(self, runner: ServerRunner) -> None:
        """A claim that lands mid-sweep keeps its sandbox.

        The staleness decision is re-made inside the lock that removes
        the sandbox, so a run which claimed (and touched) a stale
        sandbox cannot have it torn out from under it.  Checking first
        and tearing down afterwards left exactly that window — and it
        opened on the request that revived an idle conversation, since
        such a sandbox is stale by definition.
        """
        sandbox = runner.create("u", "c")
        with runner._lock:
            runner._sandboxes[sandbox]["last_used"] = 0.0  # stale

        # simulate the claim landing between the check and the teardown
        original = runner._detach_if_idle

        def _claim_then_detach(sandbox_id, ttl_seconds, now):
            runner.touch(sandbox_id)  # a run claims it right now
            return original(sandbox_id, ttl_seconds, now)

        runner._detach_if_idle = _claim_then_detach
        try:
            assert runner.reap_idle(ttl_seconds=60) == []
            assert runner.exists(sandbox)  # the claim won
        finally:
            runner._detach_if_idle = original
            runner.destroy(sandbox)

    def test_reap_still_collects_a_genuinely_idle_sandbox(self, runner: ServerRunner) -> None:
        """The atomic re-check must not make the reaper a no-op."""
        sandbox = runner.create("u", "c")
        with runner._lock:
            runner._sandboxes[sandbox]["last_used"] = 0.0
        assert runner.reap_idle(ttl_seconds=60) == [sandbox]
        assert not runner.exists(sandbox)

    def test_reap_collects_a_sandbox_whose_spawn_failed(self, tmp_path) -> None:
        """A sandbox with no process is still reapable (not skipped as
        'nothing detached')."""
        runner = ServerRunner()
        runner._harness.cmd = str(tmp_path / "does-not-exist")
        sandbox = runner.create("u", "c")  # spawn fails, meta still registered
        assert runner._procs.get(sandbox) is None
        with runner._lock:
            runner._sandboxes[sandbox]["last_used"] = 0.0
        assert runner.reap_idle(ttl_seconds=60) == [sandbox]
        assert not runner.exists(sandbox)

    def test_destroy_tears_down_a_process_with_no_meta(self, runner: ServerRunner) -> None:
        """``destroy`` must clear every map, not bail on a missing meta.

        The maps can briefly disagree -- a respawn registers a process
        while a concurrent sweep removes the meta -- and a destroy that
        skipped the teardown in that state would leak the process for
        the life of the web process.
        """
        sandbox = runner.create("u", "c")
        proc = runner._procs[sandbox]
        with runner._lock:
            runner._sandboxes.pop(sandbox)  # meta gone, process still registered
        runner.destroy(sandbox)
        assert runner._procs.get(sandbox) is None
        assert proc.wait(timeout=10) is not None  # actually stopped

    def test_respawn_onto_a_vanished_sandbox_leaves_no_orphan(self, runner: ServerRunner) -> None:
        """A sandbox destroyed during the ready handshake must not end up
        with a re-registered process nothing owns.

        The live-run slot is only claimed after the handshake, so a
        sweep or an explicit destroy can still take the sandbox while a
        respawn is in flight.  Re-registering then would leave a
        process in ``_procs`` with no meta.
        """
        sandbox = runner.create("u", "c")
        runner._procs[sandbox].kill()  # dead process -> exec_run respawns
        runner._procs[sandbox].wait(timeout=10)

        original = runner._read_ready

        def _read_ready_then_vanish(proc, timeout=30.0):
            ok = original(proc, timeout)
            with runner._lock:
                runner._sandboxes.pop(sandbox, None)  # swept mid-handshake
            return ok

        runner._read_ready = _read_ready_then_vanish
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run(sandbox, "hi", "run_1")
        assert runner._procs.get(sandbox) is None  # no orphan under that id

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

    def test_exec_after_respawn_does_not_reread_ready(self, runner: ServerRunner) -> None:
        """Regression: after a respawn consumes the fresh process's single
        ``ready`` line, the NEXT exec on the live process must not call
        _read_ready again (there is no second ready line) -- doing so
        would block until the watchdog kills a healthy process.  A short
        timeout makes the bug show up as a timed-out/failed run."""
        sandbox = runner.create("u", "c")
        try:
            runner._procs[sandbox].kill()
            runner._procs[sandbox].wait(timeout=5)
            first = runner.exec_run(sandbox, "respawn", "run_a", timeout=30)
            assert first.exit_code == 0
            # second exec on the now-live respawned process: must complete
            # promptly, not hang on a phantom ready read
            second = runner.exec_run(sandbox, "again", "run_b", timeout=10)
            assert second.exit_code == 0
            assert not second.timed_out
            assert "answered: again" in second.stdout
            assert runner._sandboxes[sandbox].get("ready_ok") is True
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

    def test_death_exit_code_is_not_lost_to_the_reaping_race(self, tmp_path) -> None:
        """The dead process's exit code must be reported every time.

        EOF on stdout only means the child closed the pipe, not that it
        has been reaped, so reading the status with ``poll()`` raced the
        exit and returned None on roughly a quarter of runs -- throwing
        away the one diagnostic the death path exists to provide.  A
        single exec could pass that by luck, so this repeats.
        """
        script = tmp_path / "exits_with_code.py"
        script.write_text(
            "import json, sys\n"
            "sys.stdout.write(json.dumps({'type': 'ready', 'pid': 1}) + '\\n')\n"
            "sys.stdout.flush()\n"
            "sys.stdin.readline()  # consume the submit op\n"
            "sys.exit(9)\n"
        )
        seen = set()
        for _ in range(12):
            runner = ServerRunner()
            runner._harness.cmd = f"{sys.executable} {script}"
            sandbox = runner.create("u", "c")
            try:
                seen.add(runner.exec_run(sandbox, "hi", "run_1", timeout=10).exit_code)
            finally:
                runner.destroy(sandbox)
        assert seen == {9}, f"exit code lost to the reaping race: {seen}"

    def test_exit_status_does_not_hang_on_a_process_that_keeps_running(self) -> None:
        """Closing stdout does not oblige a process to exit, so the wait
        is bounded: a live child degrades to None instead of blocking
        the request thread."""
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdout=subprocess.PIPE,
        )
        try:
            started = time.time()
            assert exit_status(proc, timeout=0.5) is None
            assert time.time() - started < 5  # bounded, not a 30s block
        finally:
            proc.kill()
            proc.wait()

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
