"""Run lifecycle + SSE streaming through the HTTP API (stub runner)."""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.controller import manager as manager_mod
from app.controller.runner import ExecResult, Runner


class ScriptedRunner(Runner):
    """Deterministic runner: emits a canned stdout blob per exec."""

    stdout = (
        '{"type": "start", "seq": 1}\n'
        '{"type": "notify", "seq": 2, "kind": "tool_start", "data": ["Read"]}\n'
        '{"type": "delta", "seq": 3, "text": "he"}\n'
        '{"type": "result", "seq": 4, "answer": "hello", "errors": [], '
        '"usage": {"input": 7, "output": 4, "rounds": 1}, "model": "m-api"}\n'
    )

    def __init__(self, raise_error: Exception | None = None) -> None:
        self.raise_error = raise_error

    def create(self, user_id: str, conversation_id: str) -> str:
        return "sbx_api"

    def destroy(self, sandbox_id: str) -> None:
        pass

    def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
        if self.raise_error is not None:
            raise self.raise_error
        for line in self.stdout.splitlines():
            if on_line is not None:
                on_line(line)
        return ExecResult(exit_code=0, stdout=self.stdout, stderr="")

    def reap_idle(self, ttl_seconds: float) -> list[str]:
        return []


@pytest.fixture()
def _scripted_runner(monkeypatch: pytest.MonkeyPatch) -> None:

    monkeypatch.setattr(manager_mod, "_controller", None)
    monkeypatch.setattr(manager_mod, "get_runner", lambda: ScriptedRunner(), raising=True)
    yield
    monkeypatch.setattr(manager_mod, "_controller", None, raising=False)


def _setup(client: TestClient) -> tuple[dict, str]:
    tokens = client.post(
        "/auth/register", json={"email": "runs@example.com", "password": "password-1"}
    ).json()
    headers = {"Authorization": f"Bearer {tokens['access_token']}"}
    conversation_id = client.post("/conversations", json={"title": "runs"}, headers=headers).json()[
        "id"
    ]
    return headers, conversation_id


@pytest.mark.usefixtures("_scripted_runner")
class TestRunRoutes:
    def test_start_get_and_ledger(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        started = client.post(
            f"/conversations/{conversation_id}/runs",
            json={"prompt": "hi"},
            headers=headers,
        )
        assert started.status_code == 202
        run_id = started.json()["id"]
        assert started.json()["status"] == "running"

        deadline = time.time() + 5
        final = None
        while time.time() < deadline:
            poll = client.get(f"/conversations/{conversation_id}/runs/{run_id}", headers=headers)
            if poll.json()["status"] != "running":
                final = poll
                break
            time.sleep(0.02)
        assert final is not None and final.status_code == 200
        body = final.json()
        assert body["answer"] == "hello"
        assert body["exit_code"] == 0

        summary = client.get("/usage/summary", headers=headers).json()
        assert summary["input_tokens"] == 7
        assert summary["output_tokens"] == 4
        assert summary["rounds"] == 1
        assert summary["runs"] == 1

    def test_second_concurrent_run_conflicts(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        # a deliberately slow runner keeps run #1 in "running" state
        import json as _json

        class SlowRunner(ScriptedRunner):
            def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
                time.sleep(0.5)
                return ExecResult(
                    exit_code=0,
                    stdout=_json.dumps({"type": "result", "answer": "x", "errors": []}) + "\n",
                    stderr="",
                )

        from app.controller import manager as mgr

        real = mgr._controller
        mgr._controller = mgr.Controller(runner=SlowRunner())
        try:
            first = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "one"},
                headers=headers,
            )
            assert first.status_code == 202
            second = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "two"},
                headers=headers,
            )
            assert second.status_code == 409
        finally:
            mgr._controller = real

    def test_run_on_missing_conversation_404(self, client: TestClient) -> None:
        headers, _ = _setup(client)
        res = client.post("/conversations/cnv_missing/runs", json={"prompt": "x"}, headers=headers)
        assert res.status_code == 404

    def test_get_run_of_other_conversation_404(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        other = client.post("/conversations", json={}, headers=headers).json()["id"]
        run_id = client.post(
            f"/conversations/{conversation_id}/runs", json={"prompt": "x"}, headers=headers
        ).json()["id"]
        res = client.get(f"/conversations/{other}/runs/{run_id}", headers=headers)
        assert res.status_code == 404

    def test_stream_relays_events_and_closes(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        run_id = client.post(
            f"/conversations/{conversation_id}/runs", json={"prompt": "x"}, headers=headers
        ).json()["id"]
        with client.stream(
            "GET", f"/conversations/{conversation_id}/runs/{run_id}/stream", headers=headers
        ) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            events = []
            for line in response.iter_lines():
                if line.startswith("data: "):
                    events.append(line[6:])
        types = [__import__("json").loads(e).get("type") for e in events]
        assert types[0] == "start"
        assert "notify" in types and "delta" in types
        assert types[-1] == "run"  # terminal lifecycle event closes the stream

    def test_stream_after_finish_replays_transcript(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        run_id = client.post(
            f"/conversations/{conversation_id}/runs", json={"prompt": "x"}, headers=headers
        ).json()["id"]
        deadline = time.time() + 5
        while time.time() < deadline:
            poll = client.get(f"/conversations/{conversation_id}/runs/{run_id}", headers=headers)
            if poll.json()["status"] != "running":
                break
            time.sleep(0.02)
        replay = client.get(
            f"/conversations/{conversation_id}/runs/{run_id}/stream", headers=headers
        )
        assert replay.status_code == 200
        assert '"state": "done"' in replay.text
        assert "hello" in replay.text

    def test_stream_requires_auth(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        run_id = client.post(
            f"/conversations/{conversation_id}/runs", json={"prompt": "x"}, headers=headers
        ).json()["id"]
        deadline = time.time() + 5
        while time.time() < deadline:
            poll = client.get(f"/conversations/{conversation_id}/runs/{run_id}", headers=headers)
            if poll.json()["status"] != "running":
                break
            time.sleep(0.02)
        res = client.get(f"/conversations/{conversation_id}/runs/{run_id}/stream")
        assert res.status_code == 401

    def test_stream_other_users_run_404(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        run_id = client.post(
            f"/conversations/{conversation_id}/runs", json={"prompt": "x"}, headers=headers
        ).json()["id"]
        deadline = time.time() + 5
        while time.time() < deadline:
            poll = client.get(f"/conversations/{conversation_id}/runs/{run_id}", headers=headers)
            if poll.json()["status"] != "running":
                break
            time.sleep(0.02)
        other = client.post(
            "/auth/register", json={"email": "other@example.com", "password": "password-1"}
        ).json()
        res = client.get(
            f"/conversations/{conversation_id}/runs/{run_id}/stream",
            headers={"Authorization": f"Bearer {other['access_token']}"},
        )
        assert res.status_code == 404

    def test_live_stream_with_slow_runner(self, client: TestClient) -> None:
        """The live generator path: subscribe before the run finishes."""
        import json as _json

        class SlowRunner(ScriptedRunner):
            def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
                for line in self.stdout.splitlines():
                    if on_line is not None:
                        on_line(line)
                    time.sleep(0.05)
                return ExecResult(exit_code=0, stdout=self.stdout, stderr="")

        from app.controller import manager as mgr

        real = mgr._controller
        mgr._controller = mgr.Controller(runner=SlowRunner())
        try:
            headers, conversation_id = _setup(client)
            run_id = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "x"},
                headers=headers,
            ).json()["id"]
            with client.stream(
                "GET",
                f"/conversations/{conversation_id}/runs/{run_id}/stream",
                headers=headers,
            ) as response:
                assert response.status_code == 200
                types = []
                for line in response.iter_lines():
                    if line.startswith("data: "):
                        types.append(_json.loads(line[6:]).get("type"))
                    if types and types[-1] == "run":
                        break
            assert types[0] == "start"
            assert types[-1] == "run"
            assert "delta" in types
        finally:
            mgr._controller = real

    def test_runner_crash_marks_error(self, client: TestClient) -> None:
        from app.controller import manager as mgr
        from app.controller.runner import SandboxNotFoundError

        real = mgr._controller
        mgr._controller = mgr.Controller(
            runner=ScriptedRunner(raise_error=SandboxNotFoundError("sbx_api"))
        )
        try:
            headers, conversation_id = _setup(client)
            run_id = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "x"},
                headers=headers,
            ).json()["id"]
            deadline = time.time() + 5
            while time.time() < deadline:
                poll = client.get(
                    f"/conversations/{conversation_id}/runs/{run_id}", headers=headers
                )
                if poll.json()["status"] != "running":
                    break
                time.sleep(0.02)
            assert poll.json()["status"] == "error"
            assert "sandbox gone" in poll.json()["error"]
        finally:
            mgr._controller = real

    def test_runner_generic_error_marks_error(self, client: TestClient) -> None:
        from app.controller import manager as mgr

        real = mgr._controller
        mgr._controller = mgr.Controller(runner=ScriptedRunner(raise_error=RuntimeError("boom")))
        try:
            headers, conversation_id = _setup(client)
            run_id = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "x"},
                headers=headers,
            ).json()["id"]
            deadline = time.time() + 5
            while time.time() < deadline:
                poll = client.get(
                    f"/conversations/{conversation_id}/runs/{run_id}", headers=headers
                )
                if poll.json()["status"] != "running":
                    break
                time.sleep(0.02)
            assert poll.json()["status"] == "error"
            assert "runner error: boom" in poll.json()["error"]
        finally:
            mgr._controller = real

    def test_missing_binary_marks_error(self, client: TestClient) -> None:
        """A harness binary that cannot exec produces an error run."""
        from app.controller import manager as mgr
        from app.controller.runner import ExecResult

        class DeadRunner(ScriptedRunner):
            def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
                return ExecResult(
                    exit_code=None,
                    stdout="",
                    stderr="exec failed: OSError(8, 'Exec format error')",
                )

        real = mgr._controller
        mgr._controller = mgr.Controller(runner=DeadRunner())
        try:
            headers, conversation_id = _setup(client)
            run_id = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "x"},
                headers=headers,
            ).json()["id"]
            deadline = time.time() + 5
            while time.time() < deadline:
                poll = client.get(
                    f"/conversations/{conversation_id}/runs/{run_id}", headers=headers
                )
                if poll.json()["status"] != "running":
                    break
                time.sleep(0.02)
            assert poll.json()["status"] == "error"
            assert "harness exited without result" in poll.json()["error"]
        finally:
            mgr._controller = real

    def test_stream_query_param_token_accepted(self, client: TestClient) -> None:
        """EventSource cannot set headers; the access_token query param
        must authenticate the stream request."""
        import json as _json

        class SlowRunner(ScriptedRunner):
            def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
                for line in self.stdout.splitlines():
                    if on_line is not None:
                        on_line(line)
                    time.sleep(0.05)
                return ExecResult(exit_code=0, stdout=self.stdout, stderr="")

        from app.controller import manager as mgr

        real = mgr._controller
        mgr._controller = mgr.Controller(runner=SlowRunner())
        try:
            tokens = client.post(
                "/auth/register",
                json={"email": "qparam@example.com", "password": "password-1"},
            ).json()
            headers = {"Authorization": f"Bearer {tokens['access_token']}"}
            conversation_id = client.post(
                "/conversations", json={"title": "q"}, headers=headers
            ).json()["id"]
            run_id = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "x"},
                headers=headers,
            ).json()["id"]
            with client.stream(
                "GET",
                f"/conversations/{conversation_id}/runs/{run_id}/stream"
                f"?access_token={tokens['access_token']}",
            ) as response:
                assert response.status_code == 200
                types = []
                for line in response.iter_lines():
                    if line.startswith("data: "):
                        types.append(_json.loads(line[6:]).get("type"))
                    if types and types[-1] == "run":
                        break
            assert types[0] == "start"
            assert types[-1] == "run"
        finally:
            mgr._controller = real

    def test_stream_rejects_bad_query_token(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        run_id = client.post(
            f"/conversations/{conversation_id}/runs", json={"prompt": "x"}, headers=headers
        ).json()["id"]
        deadline = time.time() + 5
        while time.time() < deadline:
            poll = client.get(f"/conversations/{conversation_id}/runs/{run_id}", headers=headers)
            if poll.json()["status"] != "running":
                break
            time.sleep(0.02)
        res = client.get(
            f"/conversations/{conversation_id}/runs/{run_id}/stream?access_token=garbage"
        )
        assert res.status_code == 401

    def test_cancel_unknown_run_is_404(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        res = client.post(
            f"/conversations/{conversation_id}/runs/run_missing/cancel",
            headers=headers,
        )
        assert res.status_code == 404

    def test_cancel_finished_run_is_noop(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        run_id = client.post(
            f"/conversations/{conversation_id}/runs", json={"prompt": "x"}, headers=headers
        ).json()["id"]
        deadline = time.time() + 5
        while time.time() < deadline:
            poll = client.get(f"/conversations/{conversation_id}/runs/{run_id}", headers=headers)
            if poll.json()["status"] != "running":
                break
            time.sleep(0.02)
        res = client.post(f"/conversations/{conversation_id}/runs/{run_id}/cancel", headers=headers)
        assert res.status_code == 200
        assert res.json() == {"cancelled": False}

    def test_cancel_running_run(self, client: TestClient) -> None:
        """Cancelling an in-flight run is accepted and marked on the row."""
        import json as _json

        class SlowRunner(ScriptedRunner):
            def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
                time.sleep(1.5)
                return ExecResult(
                    exit_code=0,
                    stdout=_json.dumps({"type": "result", "answer": "x", "errors": []}) + "\n",
                    stderr="",
                )

        from app.controller import manager as mgr

        real = mgr._controller
        mgr._controller = mgr.Controller(runner=SlowRunner())
        try:
            headers, conversation_id = _setup(client)
            run_id = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "x"},
                headers=headers,
            ).json()["id"]
            res = client.post(
                f"/conversations/{conversation_id}/runs/{run_id}/cancel",
                headers=headers,
            )
            assert res.status_code == 200
            assert res.json() == {"cancelled": True}
            row = client.get(
                f"/conversations/{conversation_id}/runs/{run_id}", headers=headers
            ).json()
            assert row["cancelled"] is True
        finally:
            mgr._controller = real


class TestAnswerRun:
    """POST .../answer: forwards the reply to a pending mid-run question."""

    def _swap_controller(self, runner):
        from app.controller import manager as mgr

        real = mgr._controller
        mgr._controller = mgr.Controller(runner=runner)
        return real

    def test_answer_unknown_run_is_404(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        res = client.post(
            f"/conversations/{conversation_id}/runs/run_missing/answer",
            json={"answers": ["x"]},
            headers=headers,
        )
        assert res.status_code == 404

    def test_answer_requires_auth(self, client: TestClient) -> None:
        res = client.post("/conversations/cnv_x/runs/run_x/answer", json={"answers": ["x"]})
        assert res.status_code == 401

    def test_answer_not_live_is_409(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A finished run no longer accepts answers (409).

        Swaps in a scripted runner WITHOUT ``deliver_answer``: once the
        run leaves the active set, the route must reject with 409."""
        from app.controller import manager as mgr

        real = mgr._controller
        mgr._controller = mgr.Controller(runner=ScriptedRunner())
        try:
            headers, conversation_id = _setup(client)
            run_id = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "x"},
                headers=headers,
            ).json()["id"]
            deadline = time.time() + 5
            while time.time() < deadline:
                poll = client.get(
                    f"/conversations/{conversation_id}/runs/{run_id}", headers=headers
                )
                if poll.json()["status"] != "running":
                    break
                time.sleep(0.02)
            res = client.post(
                f"/conversations/{conversation_id}/runs/{run_id}/answer",
                json={"answers": ["blue"]},
                headers=headers,
            )
            assert res.status_code == 409
        finally:
            mgr._controller = real

    def test_answer_delivered_on_live_run(self, client: TestClient) -> None:
        import json as _json

        from app.controller.runner import ExecResult, Runner

        class AnsweringRunner(Runner):
            """Resident-style runner: accepts answers while running."""

            def __init__(self) -> None:
                self.answers: list[tuple[str, list[str]]] = []

            def create(self, user_id, conversation_id):
                return "sbx_0"

            def destroy(self, sandbox_id):
                pass

            def cancel(self, sandbox_id, run_id):
                return True

            def deliver_answer(self, sandbox_id, run_id, answers):
                self.answers.append((run_id, answers))
                return True

            def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
                time.sleep(0.5)
                return ExecResult(
                    exit_code=0,
                    stdout=_json.dumps({"type": "result", "answer": "x", "errors": []}) + "\n",
                    stderr="",
                )

            def reap_idle(self, ttl_seconds):
                return []

        runner = AnsweringRunner()
        real = self._swap_controller(runner)
        try:
            headers, conversation_id = _setup(client)
            run_id = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "x"},
                headers=headers,
            ).json()["id"]
            res = client.post(
                f"/conversations/{conversation_id}/runs/{run_id}/answer",
                json={"answers": ["blue", "red"]},
                headers=headers,
            )
            assert res.status_code == 200
            assert res.json() == {"delivered": True}
            assert runner.answers == [(run_id, ["blue", "red"])]
        finally:
            from app.controller import manager as mgr

            mgr._controller = real

    def test_answer_requires_nonempty_answers(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        res = client.post(
            f"/conversations/{conversation_id}/runs/run_x/answer",
            json={"answers": []},
            headers=headers,
        )
        assert res.status_code == 422
