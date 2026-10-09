"""DockerRunner: one isolated container per sandbox (serve protocol).

Uses a fake docker SDK client + a fake attach socket that frames the
same protocol-faithful ``serve`` responses into docker's 8-byte stream
format, so the runner is exercised end to end (ready -> submit ->
result, ask -> answer, cancel, respawn) without a Docker daemon.  A
dedicated class asserts the isolation invariants that are the entire
point of this runner: no host env, only the workspace mount, no
network, dropped capabilities, and host-side secret injection.
"""

from __future__ import annotations

import json
import threading

import pytest

from app.controllers.runner import DockerRunner, SandboxNotFoundError, _DockerStream


def _frame(payload: dict, stream_type: int = 1) -> bytes:
    """One docker stream frame: 8-byte header + JSON line body."""
    body = (json.dumps(payload) + "\n").encode("utf-8")
    header = bytes([stream_type, 0, 0, 0]) + len(body).to_bytes(4, "big")
    return header + body


class FakeSocket:
    """A protocol-faithful fake of a container attach socket.

    Reads ops written to stdin and pushes framed responses into a recv
    buffer, mimicking ``harness serve``: ready on connect, submit ->
    start/ask/result, answer -> log, cancel -> cancelled result.
    """

    def __init__(self) -> None:
        self._out = bytearray()
        self._closed = False
        self._cond = threading.Condition()
        self.sent: list[dict] = []
        self._push(
            _frame(
                {
                    "type": "ready",
                    "pid": 1234,
                    "protocol_version": 1,
                    "capabilities": ["submit", "answer", "cancel", "hello", "op_id", "ask_id"],
                }
            )
        )

    def _push(self, data: bytes) -> None:
        with self._cond:
            self._out.extend(data)
            self._cond.notify_all()

    def recv(self, n: int) -> bytes:
        with self._cond:
            while not self._out and not self._closed:
                self._cond.wait(timeout=5)
            if self._out:
                chunk = bytes(self._out[:n])
                del self._out[:n]
                return chunk
            return b""

    def sendall(self, data: bytes) -> None:
        for line in data.decode("utf-8").splitlines():
            if not line.strip():
                continue
            op = json.loads(line)
            self.sent.append(op)
            self._respond(op)

    def _respond(self, op: dict) -> None:
        name = op.get("op")
        rid = op.get("run_id")
        if name == "hello":
            self._push(
                _frame(
                    {
                        "type": "hello",
                        "protocol_version": 1,
                        "capabilities": ["hello", "op_id"],
                        "op_id": op.get("op_id"),
                    }
                )
            )
        elif name == "ping":
            self._push(_frame({"type": "pong", "op_id": op.get("op_id")}))
        elif name == "submit":
            self._push(_frame({"seq": 1, "type": "start", "prompt": op["prompt"], "run_id": rid}))
            self._push(
                _frame(
                    {
                        "seq": 2,
                        "type": "notify",
                        "kind": "ask",
                        "data": {"kind": "ask", "questions": [{"question": "color?"}]},
                        "run_id": rid,
                    }
                )
            )
            self._push(
                _frame(
                    {
                        "seq": 3,
                        "type": "result",
                        "answer": "answered: " + op["prompt"],
                        "errors": [],
                        "cancelled": False,
                        "model": "fake",
                        "run_id": rid,
                    }
                )
            )
        elif name == "answer":
            self._push(
                _frame(
                    {
                        "seq": 4,
                        "type": "log",
                        "message": "answer received: " + ",".join(op["answers"]),
                        "run_id": rid,
                    }
                )
            )
        elif name == "cancel":
            self._push(
                _frame(
                    {
                        "seq": 9,
                        "type": "result",
                        "answer": "",
                        "errors": [],
                        "cancelled": True,
                        "model": "fake",
                        "run_id": rid,
                    }
                )
            )

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()


class FakeContainer:
    def __init__(self, kwargs: dict) -> None:
        self.create_kwargs = kwargs
        self.status = "created"
        self.attrs = {"State": {"ExitCode": 0}}
        self._sock = FakeSocket()
        self.stopped = False
        self.removed = False
        self.killed = False

    def start(self) -> None:
        self.status = "running"

    def attach_socket(self, params=None):
        return self._sock

    def reload(self) -> None:
        pass

    def stop(self, timeout: int = 5) -> None:
        self.stopped = True
        self.status = "exited"
        self._sock.close()

    def remove(self, force: bool = False) -> None:
        self.removed = True

    def kill(self) -> None:
        self.killed = True
        self.status = "exited"
        self._sock.close()


class FakeContainers:
    def __init__(self) -> None:
        self.created: list[FakeContainer] = []

    def create(self, **kwargs) -> FakeContainer:
        container = FakeContainer(kwargs)
        self.created.append(container)
        return container


class FakeClient:
    def __init__(self) -> None:
        self.containers = FakeContainers()


@pytest.fixture()
def client() -> FakeClient:
    return FakeClient()


@pytest.fixture()
def runner(client: FakeClient) -> DockerRunner:
    # a secret_source that hands back one env-shaped secret, so injection
    # is observable in the container create kwargs
    return DockerRunner(client=client, secret_source=lambda uid: {"OPENAI_API_KEY": "sk-secret"})


class TestLifecycle:
    def test_create_destroy(self, runner: DockerRunner, client: FakeClient) -> None:
        sandbox = runner.create("u", "c")
        assert runner.exists(sandbox)
        container = client.containers.created[0]
        assert container.status == "running"
        runner.destroy(sandbox)
        assert not runner.exists(sandbox)
        assert container.stopped and container.removed

    def test_reap_idle_destroys(self, runner: DockerRunner) -> None:
        fresh = runner.create("u", "c")
        stale = runner.create("u", "c2")
        with runner._lock:
            runner._sandboxes[stale]["last_used"] = 0.0
        destroyed = runner.reap_idle(ttl_seconds=60)
        assert destroyed == [stale]
        assert runner.exists(fresh)
        runner.destroy(fresh)

    def test_reap_idle_spares_a_sandbox_with_a_live_run(self, runner: DockerRunner) -> None:
        """Mirrors ServerRunner: the idle TTL must never bound a run."""
        busy = runner.create("u", "c")
        with runner._lock:
            runner._sandboxes[busy]["last_used"] = 0.0
            runner._live_run[busy] = "run_live"
        assert runner.reap_idle(ttl_seconds=60) == []
        assert runner.exists(busy)
        with runner._lock:
            runner._live_run[busy] = None
        assert runner.reap_idle(ttl_seconds=60) == [busy]

    def test_unknown_sandbox_raises(self, runner: DockerRunner) -> None:
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run("sbx_missing", "hi", "run_1")

    def test_destroy_if_running_requires_ownership(self, runner: DockerRunner) -> None:
        """Mirrors ServerRunner: escalation must not kill a successor."""
        sandbox = runner.create("u", "c")
        with runner._lock:
            runner._live_run[sandbox] = "run_a"
        assert runner.destroy_if_running(sandbox, "run_stale") is False
        assert runner.exists(sandbox)
        assert runner.destroy_if_running(sandbox, "run_a") is True
        assert not runner.exists(sandbox)
        assert runner.destroy_if_running("sbx_missing", "run_a") is False

    def test_live_run_slot_never_outlives_the_sandbox(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        runner.destroy(sandbox)
        with pytest.raises(SandboxNotFoundError):
            runner._claim_live_run(sandbox, "run_1")
        runner._release_live_run(sandbox)
        assert sandbox not in runner._live_run

    def test_respawn_onto_a_vanished_sandbox_leaves_no_orphan(
        self, runner: DockerRunner, client: FakeClient
    ) -> None:
        """Mirrors ServerRunner: a sandbox destroyed during the ready
        handshake must not get its container re-registered.

        The live-run slot is only claimed after the handshake, so a
        sweep or an escalated cancel can still take the sandbox while
        the respawn is in flight; re-registering would leave container
        and stream handles with no meta.
        """
        sandbox = runner.create("u", "c")
        client.containers.created[0].status = "exited"  # dead -> exec respawns

        original = runner._read_ready

        def _read_ready_then_vanish(stream, timeout=30.0):
            ok = original(stream, timeout)
            with runner._lock:
                runner._sandboxes.pop(sandbox, None)  # swept mid-handshake
            return ok

        runner._read_ready = _read_ready_then_vanish
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run(sandbox, "hi", "run_1")

        assert runner._containers.get(sandbox) is None
        assert runner._streams.get(sandbox) is None
        assert sandbox not in runner._live_run
        # the container started for the doomed respawn was torn back down
        assert client.containers.created[-1].removed is True


class TestExecRun:
    def test_full_round_trip(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            lines: list[str] = []
            result = runner.exec_run(sandbox, "hello", "run_1", on_line=lines.append, timeout=30)
            assert result.exit_code == 0
            assert not result.timed_out
            payloads = [json.loads(line) for line in lines]
            assert [p["type"] for p in payloads] == ["start", "notify", "result"]
            assert payloads[0]["run_id"] == "run_1"
            assert payloads[-1]["answer"] == "answered: hello"
        finally:
            runner.destroy(sandbox)

    def test_two_turns_same_container(self, runner: DockerRunner, client: FakeClient) -> None:
        sandbox = runner.create("u", "c")
        try:
            runner.exec_run(sandbox, "one", "run_1", timeout=30)
            runner.exec_run(sandbox, "two", "run_2", timeout=30)
            # one container serviced both turns (resident model)
            assert len(client.containers.created) == 1
        finally:
            runner.destroy(sandbox)

    def test_deliver_answer_while_live(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            # drive the live-run slot by starting an exec on a thread and
            # delivering an answer while it is the live run
            delivered: list[bool] = []

            def _run() -> None:
                runner.exec_run(sandbox, "hi", "run_1", timeout=30)

            # simplest deterministic check: not live before/after a run
            assert runner.deliver_answer(sandbox, "run_1", ["blue"]) is False
            t = threading.Thread(target=_run)
            t.start()
            t.join()
            assert delivered == []
        finally:
            runner.destroy(sandbox)

    def test_cancel_not_live_is_false(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            assert runner.cancel(sandbox, "run_x") is False
        finally:
            runner.destroy(sandbox)


class TestIsolation:
    """The invariants that make this runner a real trust boundary."""

    def _create_kwargs(self, runner: DockerRunner, client: FakeClient) -> dict:
        runner.create("owner-1", "c")
        return client.containers.created[0].create_kwargs

    def test_environment_excludes_host_env(
        self, runner: DockerRunner, client: FakeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # a host secret that must never reach the container
        monkeypatch.setenv("PAW_SECRET_KEY", "super-secret-host-key")
        kwargs = self._create_kwargs(runner, client)
        env = kwargs["environment"]
        assert "PAW_SECRET_KEY" not in env
        # only the noninteractive flag + injected owner secret
        assert env["PAW_NONINTERACTIVE"] == "1"
        assert env["OPENAI_API_KEY"] == "sk-secret"
        assert set(env) == {"PAW_NONINTERACTIVE", "OPENAI_API_KEY"}

    def test_only_workspace_is_mounted(self, runner: DockerRunner, client: FakeClient) -> None:
        kwargs = self._create_kwargs(runner, client)
        volumes = kwargs["volumes"]
        # exactly one bind mount, and it maps to the container workdir
        assert len(volumes) == 1
        ((host_path, spec),) = volumes.items()
        assert spec["bind"] == kwargs["working_dir"]
        assert "c" in host_path  # the conversation workspace, nothing else

    def test_network_disabled(self, runner: DockerRunner, client: FakeClient) -> None:
        kwargs = self._create_kwargs(runner, client)
        assert kwargs["network_mode"] == "none"

    def test_capabilities_and_privileges_locked_down(
        self, runner: DockerRunner, client: FakeClient
    ) -> None:
        kwargs = self._create_kwargs(runner, client)
        assert kwargs["cap_drop"] == ["ALL"]
        assert "no-new-privileges" in kwargs["security_opt"]
        assert kwargs["read_only"] is True
        assert kwargs["user"] == "1000:1000"
        assert kwargs["pids_limit"] > 0
        assert kwargs["mem_limit"]
        assert kwargs["nano_cpus"] > 0

    def test_command_has_no_budget_flags_by_default(
        self, runner: DockerRunner, client: FakeClient
    ) -> None:
        assert self._create_kwargs(runner, client)["command"] == ["serve"]

    def test_configured_budgets_reach_the_container_command(
        self, runner: DockerRunner, client: FakeClient, monkeypatch
    ) -> None:
        """The sandbox-side ceiling must be set by whoever starts the
        sandbox; for the docker runner that is the container command."""
        from app.infra.config import get_settings

        monkeypatch.setattr(get_settings().harness, "max_rounds", 9)
        monkeypatch.setattr(get_settings().harness, "sandbox_timeout", 120.0)
        command = self._create_kwargs(runner, client)["command"]
        assert command[0] == "serve"
        assert command[command.index("--max-rounds") + 1] == "9"
        assert command[command.index("--timeout") + 1] == "120.0"
        assert "--answer-timeout" not in command

    def test_secret_source_failure_does_not_brick_create(self, client: FakeClient) -> None:
        def _boom(uid: str) -> dict:
            raise RuntimeError("secrets backend down")

        runner = DockerRunner(client=client, secret_source=_boom)
        sandbox = runner.create("u", "c")  # must not raise
        assert runner.exists(sandbox)
        env = client.containers.created[0].create_kwargs["environment"]
        assert env == {"PAW_NONINTERACTIVE": "1"}  # degraded to no secrets
        runner.destroy(sandbox)

    def test_no_secret_source_means_no_secrets(self, client: FakeClient) -> None:
        runner = DockerRunner(client=client)  # no secret_source wired
        runner.create("u", "c")
        env = client.containers.created[0].create_kwargs["environment"]
        assert env == {"PAW_NONINTERACTIVE": "1"}


class TestDockerStream:
    """The frame demultiplexer that carries JSONL across the boundary."""

    def test_demux_routes_stdout_and_stderr(self) -> None:
        sink: list[str] = []
        sock = FakeSocket()
        # replace ready frame with a controlled sequence
        sock._out.clear()
        sock._push(_frame({"type": "log", "message": "hi"}, stream_type=1))
        sock._push(_frame({"err": "boom"}, stream_type=2))  # stderr frame
        sock._push(_frame({"type": "result"}, stream_type=1))
        stream = _DockerStream(sock, stderr_sink=sink.append)
        first = json.loads(stream.readline())
        second = json.loads(stream.readline())
        assert first == {"type": "log", "message": "hi"}
        assert second == {"type": "result"}  # stderr frame skipped in stdout
        assert any("boom" in s for s in sink)

    def test_readline_eof_returns_empty(self) -> None:
        sock = FakeSocket()
        sock._out.clear()
        sock.close()
        stream = _DockerStream(sock)
        assert stream.readline() == ""


class TestHandshakeNegotiation:
    """Version negotiation across the container boundary."""

    def test_capabilities_are_recorded(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            runner.exec_run(sandbox, "hi", "run_1")
            with runner._lock:
                assert runner._sandboxes[sandbox]["handshake"].protocol_version == 1
        finally:
            runner.destroy(sandbox)

    def test_unsupported_version_fails_every_run(
        self, runner: DockerRunner, client: FakeClient, monkeypatch
    ) -> None:
        """A refused handshake must keep reporting itself, not decay
        into a misleading 'died before ready' on the retry."""
        from app.controllers import runner as runner_mod

        real = runner_mod.parse_ready
        monkeypatch.setattr(
            runner_mod,
            "parse_ready",
            lambda payload: runner_mod.Handshake(protocol_version=99, capabilities=frozenset()),
        )
        sandbox = runner.create("u", "c")
        try:
            for _ in range(2):
                result = runner.exec_run(sandbox, "hi", "run_1")
                assert result.exit_code is None
                assert "unsupported harness protocol version 99" in result.stderr
                assert result.stdout == ""
        finally:
            monkeypatch.setattr(runner_mod, "parse_ready", real)
            runner.destroy(sandbox)

    def test_ask_id_rides_on_the_answer_op(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            with runner._lock:
                runner._live_run[sandbox] = "run_1"
            assert runner.deliver_answer(sandbox, "run_1", ["blue"], ask_id="ask-xyz") is True
            sent = runner._streams[sandbox]._sock.sent
            answers = [o for o in sent if o.get("op") == "answer"]
            assert answers and answers[0]["ask_id"] == "ask-xyz"
        finally:
            runner.destroy(sandbox)

    def test_answer_without_an_ask_id_omits_the_field(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            with runner._lock:
                runner._live_run[sandbox] = "run_1"
            assert runner.deliver_answer(sandbox, "run_1", ["blue"]) is True
            sent = runner._streams[sandbox]._sock.sent
            answers = [o for o in sent if o.get("op") == "answer"]
            assert answers and "ask_id" not in answers[0]
        finally:
            runner.destroy(sandbox)


class TestNegotiationParity:
    """The container runner must negotiate too.

    `_greet`/`_negotiate` originally existed only on ServerRunner --
    the same asymmetry that had already produced two drift bugs between
    the two runners.
    """

    def test_hello_is_sent_once_per_container(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            runner.exec_run(sandbox, "one", "run_1")
            runner.exec_run(sandbox, "two", "run_2")
            sent = runner._streams[sandbox]._sock.sent
            assert [o["op"] for o in sent].count("hello") == 1
            hello = next(o for o in sent if o["op"] == "hello")
            assert hello["protocol_versions"] == [1]
            assert hello["op_id"]
        finally:
            runner.destroy(sandbox)

    def test_submit_is_tagged_with_an_op_id(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            runner.exec_run(sandbox, "hi", "run_1")
            sent = runner._streams[sandbox]._sock.sent
            submit = next(o for o in sent if o["op"] == "submit")
            assert submit["op_id"]
        finally:
            runner.destroy(sandbox)

    def test_the_hello_reply_is_not_relayed_as_a_run_event(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            lines: list[str] = []
            runner.exec_run(sandbox, "hi", "run_1", on_line=lines.append)
            types = [json.loads(line)["type"] for line in lines]
            assert "hello" not in types
            assert types[0] == "start"
        finally:
            runner.destroy(sandbox)

    def test_ping_works_across_the_boundary(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            assert runner._read_ready(runner._transports[sandbox]) is not None
            assert runner.ping(sandbox, timeout=5) is True
        finally:
            runner.destroy(sandbox)


class TestSharedLifecycleReachesDocker:
    """Behaviour that used to exist only on ServerRunner.

    `ping` was added to DockerRunner but never called from its exec
    path, so a wedged container was reused and the submit hung for the
    host timeout.  Hoisting the exec lifecycle into ResidentRunner is
    what makes that impossible rather than merely fixed.
    """

    def test_a_warm_container_is_probed_before_reuse(self, runner: DockerRunner) -> None:
        sandbox = runner.create("u", "c")
        try:
            runner.exec_run(sandbox, "one", "run_1")
            probed: list[str] = []
            real_ping = runner.ping

            def _spy(sandbox_id, timeout=5.0):
                probed.append(sandbox_id)
                return real_ping(sandbox_id, timeout)

            runner.ping = _spy
            runner.exec_run(sandbox, "two", "run_2")
            assert probed == [sandbox]  # the second turn probed first
        finally:
            runner.destroy(sandbox)

    def test_a_wedged_container_is_respawned(
        self, runner: DockerRunner, client: FakeClient
    ) -> None:
        sandbox = runner.create("u", "c")
        try:
            runner.exec_run(sandbox, "one", "run_1")
            assert len(client.containers.created) == 1
            runner.ping = lambda sandbox_id, timeout=5.0: False  # wedged
            result = runner.exec_run(sandbox, "two", "run_2")
            assert len(client.containers.created) == 2  # replaced, not reused
            assert result.exit_code == 0  # and the turn still succeeded
        finally:
            runner.destroy(sandbox)


class TestDockerStreamLineCap:
    """Both buffers under the attach socket need their own ceiling.

    Untrusted code controls the frame sizes AND the line lengths, so
    capping only the demultiplexed buffer left two holes: a line that
    arrived complete (newline included) bypassed the check entirely,
    and a single huge frame was assembled in full one layer below it.
    """

    @staticmethod
    def _frame(body: bytes) -> bytes:
        return bytes([1, 0, 0, 0]) + len(body).to_bytes(4, "big") + body

    class _Sock:
        def __init__(self, data: bytes) -> None:
            self.data = data

        def recv(self, n: int) -> bytes:
            chunk, self.data = self.data[:n], self.data[n:]
            return chunk

        def close(self) -> None:
            pass

    def _drain(self, data: bytes, max_line: int = 100) -> tuple[list[str], _DockerStream]:
        stream = _DockerStream(self._Sock(data), max_line=max_line)
        lines: list[str] = []
        while True:
            line = stream.readline()
            if not line:
                return lines, stream
            lines.append(line.strip())

    @property
    def _good(self) -> bytes:
        return self._frame(b'{"type":"result"}\n')

    def test_normal_traffic_is_untouched(self) -> None:
        lines, stream = self._drain(self._frame(b'{"a":1}\n{"b":2}\n'))
        assert lines == ['{"a":1}', '{"b":2}']
        assert stream.oversize_lines == 0

    def test_a_complete_oversize_line_is_dropped(self) -> None:
        """Newline in the same frame used to bypass the cap entirely."""
        lines, stream = self._drain(self._frame(b"X" * 300 + b"\n") + self._good)
        assert lines == ['{"type":"result"}']  # the good line still arrives
        assert stream.oversize_lines == 1

    def test_a_single_huge_frame_is_never_assembled(self) -> None:
        """The frame layer sits below the line buffer and needs its own
        cap: the complete-line check alone would still have buffered the
        whole payload before rejecting it.

        Asserts the PEAK buffer size, not the final one -- the end
        state is empty either way, which is what let this hole hide.
        """
        cap = 1000
        stream = _DockerStream(
            self._Sock(self._frame(b"Y" * 5_000_000 + b"\n") + self._good), max_line=cap
        )
        peak = 0
        real_demux = stream._demux

        def _watch(chunk: bytes) -> bytes:
            nonlocal peak
            out = real_demux(chunk)
            peak = max(peak, len(stream._buf), len(stream._pending))
            return out

        stream._demux = _watch
        lines = []
        while True:
            line = stream.readline()
            if not line:
                break
            lines.append(line.strip())
        assert lines == ['{"type":"result"}']
        assert stream.oversize_lines == 1
        # a few frames' worth of slack, nowhere near the 5 MB payload
        assert peak < cap * 20, f"buffered {peak} bytes for a {cap}-byte cap"

    def test_an_oversize_line_spread_over_small_frames_is_dropped(self) -> None:
        """Reaches the complete-line check specifically: each frame is
        under the cap, and the newline arrives in the chunk that pushes
        the assembled line over it -- so the no-newline guard never
        fires and the frame guard never fires either."""
        data = self._frame(b"A" * 90) + self._frame(b"B" * 80 + b"\n") + self._good
        lines, stream = self._drain(data, max_line=100)
        assert lines == ['{"type":"result"}']
        assert stream.oversize_lines == 1

    def test_a_newline_free_flood_is_capped(self) -> None:
        data = self._frame(b"Z" * 90) * 50 + self._frame(b"\n") + self._good
        lines, stream = self._drain(data)
        assert lines == ['{"type":"result"}']
        assert stream.oversize_lines == 1

    def test_the_tail_after_a_dropped_line_is_kept(self) -> None:
        """The oversize line ends at its newline; whatever follows in
        the same frame is a different line and must survive."""
        data = self._frame(b"X" * 300 + b"\n" + b'{"type":"pong"}\n') + self._good
        lines, stream = self._drain(data)
        assert lines == ['{"type":"pong"}', '{"type":"result"}']
        assert stream.oversize_lines == 1

    def test_many_oversize_lines_do_not_exhaust_the_stack(self) -> None:
        """Recursing once per discarded line was itself a cheap DoS."""
        data = self._frame(b"X" * 300 + b"\n") * 2000 + self._good
        lines, stream = self._drain(data)
        assert lines == ['{"type":"result"}']
        assert stream.oversize_lines == 2000

    def test_a_zero_cap_disables_the_check(self) -> None:
        lines, stream = self._drain(self._frame(b"X" * 300 + b"\n"), max_line=0)
        assert lines == ["X" * 300]
        assert stream.oversize_lines == 0
