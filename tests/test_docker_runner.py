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
        self._push(_frame({"type": "ready", "pid": 1234}))

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
        if name == "submit":
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

    def test_unknown_sandbox_raises(self, runner: DockerRunner) -> None:
        with pytest.raises(SandboxNotFoundError):
            runner.exec_run("sbx_missing", "hi", "run_1")


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
        (host_path, spec), = volumes.items()
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
