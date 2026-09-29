"""Conversation file upload/list/delete through the HTTP API."""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.infra.config import get_settings


def _setup(client: TestClient) -> tuple[dict, str]:
    tokens = client.post(
        "/auth/register", json={"email": "files@example.com", "password": "password-1"}
    ).json()
    headers = {"Authorization": f"Bearer {tokens['access_token']}"}
    conversation_id = client.post(
        "/conversations", json={"title": "files"}, headers=headers
    ).json()["id"]
    return headers, conversation_id


class TestFileRoutes:
    def test_upload_list_delete(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        res = client.post(
            f"/conversations/{conversation_id}/files",
            headers=headers,
            files={"file": ("sales.xlsx", b"fake-xlsx-bytes", "application/octet-stream")},
        )
        assert res.status_code == 201, res.text
        body = res.json()
        assert body["filename"] == "sales.xlsx"
        assert body["size"] == len(b"fake-xlsx-bytes")

        detail = client.get(f"/conversations/{conversation_id}", headers=headers).json()
        assert [f["filename"] for f in detail["files"]] == ["sales.xlsx"]

        # the file must exist on disk inside the conversation workspace
        workspace = Path(get_settings().workspace_root) / conversation_id
        assert len(list(workspace.iterdir())) == 1

        res = client.delete(f"/conversations/{conversation_id}/files/{body['id']}", headers=headers)
        assert res.status_code == 204
        detail = client.get(f"/conversations/{conversation_id}", headers=headers).json()
        assert detail["files"] == []

    def test_upload_durably_persisted(self, client: TestClient) -> None:
        """Upload writes through to the durable store, and delete removes
        it from there too (not just the workspace)."""
        from app.controllers.files import durable_key
        from app.infra.storage import get_storage

        headers, conversation_id = _setup(client)
        res = client.post(
            f"/conversations/{conversation_id}/files",
            headers=headers,
            files={"file": ("data.csv", b"a,b,c\n1,2,3\n", "application/octet-stream")},
        )
        file_id = res.json()["id"]
        # the stored_name is the single file present in the workspace
        workspace = Path(get_settings().workspace_root) / conversation_id
        stored_name = next(workspace.iterdir()).name

        store = get_storage()
        key = durable_key(conversation_id, stored_name)
        assert store.exists(key), "upload was not written through to durable storage"
        assert store.get_bytes(key) == b"a,b,c\n1,2,3\n"

        client.delete(f"/conversations/{conversation_id}/files/{file_id}", headers=headers)
        assert not store.exists(key), "durable copy was not removed on delete"

    def test_artifact_durability_sync_and_rehydrate(self, client: TestClient) -> None:
        """Agent outputs are synced to durable storage and can be
        rehydrated into a fresh workspace from there (pod-loss / other
        instance)."""
        from app.controllers import files as files_controller
        from app.controllers.files import durable_key
        from app.infra.config import conversation_workspace
        from app.infra.db import get_session_factory
        from app.infra.storage import get_storage
        from app.models import Conversation as Conv

        headers, conversation_id = _setup(client)
        ws = conversation_workspace(conversation_id)
        (ws / "final.xlsx").write_bytes(b"PK\x03\x04agent-output")

        db = get_session_factory()()
        try:
            conv = db.get(Conv, conversation_id)
            synced = files_controller.sync_artifacts(db, conv)
            assert "final.xlsx" in synced
            store = get_storage()
            assert store.exists(durable_key(conversation_id, "final.xlsx"))
            # simulate pod loss / other instance: remove the local copy
            (ws / "final.xlsx").unlink()
            # artifact_path rehydrates from durable storage
            path = files_controller.artifact_path(conv, "final.xlsx")
            assert path.read_bytes() == b"PK\x03\x04agent-output"
        finally:
            db.close()

    def test_sync_artifacts_skips_unchanged(self, client: TestClient) -> None:
        """A second sync must not re-upload an artifact whose durable copy
        already matches by size (avoids O(all files) re-upload per run)."""
        from app.controllers import files as files_controller
        from app.infra.config import conversation_workspace
        from app.infra.db import get_session_factory
        from app.models import Conversation as Conv

        headers, conversation_id = _setup(client)
        ws = conversation_workspace(conversation_id)
        (ws / "out.csv").write_bytes(b"a,b\n1,2\n")

        db = get_session_factory()()
        try:
            conv = db.get(Conv, conversation_id)
            first = files_controller.sync_artifacts(db, conv)
            assert first == ["out.csv"]
            # unchanged: second sync skips it
            second = files_controller.sync_artifacts(db, conv)
            assert second == []
            # change the file: it is synced again
            (ws / "out.csv").write_bytes(b"a,b\n1,2\n3,4\n")
            third = files_controller.sync_artifacts(db, conv)
            assert third == ["out.csv"]
        finally:
            db.close()

    def test_upload_requires_auth(self, client: TestClient) -> None:
        conversation_id = "cnv_missing"
        res = client.post(
            f"/conversations/{conversation_id}/files",
            files={"file": ("a.xlsx", b"x", "application/octet-stream")},
        )
        assert res.status_code == 401

    def test_upload_foreign_conversation_404(self, client: TestClient) -> None:
        headers, _ = _setup(client)
        res = client.post(
            "/conversations/cnv_nobody/files",
            headers=headers,
            files={"file": ("a.xlsx", b"x", "application/octet-stream")},
        )
        assert res.status_code == 404

    def test_upload_sanitizes_filename(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        res = client.post(
            f"/conversations/{conversation_id}/files",
            headers=headers,
            files={"file": ("../../etc/passwd.xlsx", b"x", "application/octet-stream")},
        )
        assert res.status_code == 201, res.text
        assert res.json()["filename"] == "passwd.xlsx"

    def test_artifacts_list_and_download(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        client.post(
            f"/conversations/{conversation_id}/files",
            headers=headers,
            files={"file": ("input.xlsx", b"upload-bytes", "application/octet-stream")},
        )
        workspace = Path(get_settings().workspace_root) / conversation_id
        (workspace / "result.xlsx").write_bytes(b"agent-output")

        res = client.get(f"/conversations/{conversation_id}/artifacts", headers=headers)
        assert res.status_code == 200, res.text
        names = [a["name"] for a in res.json()]
        # the upload is excluded; the agent output is listed
        assert names == ["result.xlsx"]

        res = client.get(f"/conversations/{conversation_id}/artifacts/result.xlsx", headers=headers)
        assert res.status_code == 200
        assert res.content == b"agent-output"
        assert "result.xlsx" in res.headers["content-disposition"]

    def test_artifacts_path_traversal_rejected(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        res = client.get(
            f"/conversations/{conversation_id}/artifacts/..%2F..%2Fetc%2Fpasswd",
            headers=headers,
        )
        assert res.status_code in (400, 404)

    def test_artifacts_unknown_404(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        res = client.get(f"/conversations/{conversation_id}/artifacts/nope.xlsx", headers=headers)
        assert res.status_code == 404

    def test_artifacts_foreign_conversation_404(self, client: TestClient) -> None:
        headers, _ = _setup(client)
        res = client.get("/conversations/cnv_nobody/artifacts", headers=headers)
        assert res.status_code == 404

    def test_delete_unknown_file_404(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        res = client.delete(f"/conversations/{conversation_id}/files/file_nope", headers=headers)
        assert res.status_code == 404

    def test_upload_over_limit_413_and_leaves_no_partial(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The size cap is enforced while streaming, and the partial file
        is removed -- a half-written upload must not linger in the
        agent's cwd where it could be mistaken for real input."""
        monkeypatch.setattr(get_settings(), "max_upload_bytes", 8)
        headers, conversation_id = _setup(client)
        res = client.post(
            f"/conversations/{conversation_id}/files",
            headers=headers,
            files={"file": ("big.xlsx", b"x" * 64, "application/octet-stream")},
        )
        assert res.status_code == 413, res.text
        workspace = Path(get_settings().workspace_root) / conversation_id
        assert list(workspace.iterdir()) == []
        detail = client.get(f"/conversations/{conversation_id}", headers=headers).json()
        assert detail["files"] == []

    def test_artifact_empty_name_is_not_a_listing(self, client: TestClient) -> None:
        """A trailing slash must not be read as "download the workspace"."""
        headers, conversation_id = _setup(client)
        res = client.get(f"/conversations/{conversation_id}/artifacts/", headers=headers)
        assert res.status_code in (307, 400, 404), res.text

    def test_run_prompt_includes_uploaded_files(self, client: TestClient) -> None:
        from app.controllers import manager as manager_mod
        from app.controllers.runner import ExecResult, Runner

        class CapturingRunner(Runner):
            captured: list[tuple[str, str]] = []

            def create(self, user_id: str, conversation_id: str) -> str:
                return "sbx_files"

            def destroy(self, sandbox_id: str) -> None:
                pass

            def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
                CapturingRunner.captured.append((prompt, run_id))
                return ExecResult(
                    exit_code=0,
                    stdout='{"type": "result", "answer": "ok", "errors": []}\n',
                    stderr="",
                )

            def reap_idle(self, ttl_seconds: float) -> list[str]:
                return []

        monkeypatched = pytest.MonkeyPatch()
        monkeypatched.setattr(manager_mod, "_controller", None)
        monkeypatched.setattr(manager_mod, "get_runner", lambda: CapturingRunner(), raising=True)
        try:
            headers, conversation_id = _setup(client)
            client.post(
                f"/conversations/{conversation_id}/files",
                headers=headers,
                files={"file": ("sales.xlsx", b"x", "application/octet-stream")},
            )
            started = client.post(
                f"/conversations/{conversation_id}/runs",
                json={"prompt": "summarize"},
                headers=headers,
            )
            assert started.status_code == 202, started.text
            deadline = time.time() + 5
            while time.time() < deadline:
                if not CapturingRunner.captured:
                    time.sleep(0.02)
                    continue
                break
            assert CapturingRunner.captured, "run never executed"
            harness_prompt, run_id = CapturingRunner.captured[0]
            assert "sales.xlsx" in harness_prompt
            assert "Save any result files" in harness_prompt
            # the Run row keeps the user's verbatim prompt, not the
            # augmented harness prompt (the UI echoes it)
            from app.infra.db import get_session_factory
            from app.models import Run

            db = get_session_factory()()
            try:
                assert db.get(Run, run_id).prompt == "summarize"
            finally:
                db.close()
        finally:
            monkeypatched.undo()

    def test_delete_conversation_removes_files(self, client: TestClient) -> None:
        headers, conversation_id = _setup(client)
        client.post(
            f"/conversations/{conversation_id}/files",
            headers=headers,
            files={"file": ("a.xlsx", b"x", "application/octet-stream")},
        )
        res = client.delete(f"/conversations/{conversation_id}", headers=headers)
        assert res.status_code == 204
        workspace = Path(get_settings().workspace_root) / conversation_id
        assert not workspace.is_dir() or not any(workspace.iterdir())
