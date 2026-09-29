"""Controller-level tests: business rules without an HTTP request.

These exist to prove the split is real -- the rules they exercise used
to live inside route handlers and could only be reached through a
TestClient.  Anything here that needs FastAPI has not been separated
properly.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from app.controllers import files, runs
from app.controllers.errors import (
    Conflict,
    DomainError,
    InvalidRequest,
    NotFound,
    PayloadTooLarge,
    Unauthorized,
)


@dataclass
class _Upload:
    """Stand-in for a ConversationFile row (only stored_name matters)."""

    stored_name: str


class TestHarnessPrompt:
    """The prompt the agent receives is assembled, not passed through."""

    def test_no_files_still_gets_the_save_hint(self) -> None:
        out = runs.build_harness_prompt("summarize", [])
        assert out.startswith("summarize")
        assert "Save any result files" in out
        assert "Uploaded files" not in out

    def test_uploads_are_named_by_stored_name(self) -> None:
        """stored_name, not filename: the on-disk name carries a unique
        prefix and is the one the agent can actually open."""
        out = runs.build_harness_prompt("go", [_Upload("ab12cd34_sales.xlsx")])
        assert "ab12cd34_sales.xlsx" in out
        assert "Uploaded files in your working directory" in out

    def test_multiple_uploads_are_comma_joined(self) -> None:
        out = runs.build_harness_prompt("go", [_Upload("a_1.xlsx"), _Upload("b_2.csv")])
        assert "a_1.xlsx, b_2.csv" in out

    def test_user_prompt_is_never_mutated(self) -> None:
        prompt = "summarize the sheet"
        out = runs.build_harness_prompt(prompt, [_Upload("x_1.xlsx")])
        assert out.startswith(prompt)
        assert prompt == "summarize the sheet"


class TestSafeFilename:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("sales.xlsx", "sales.xlsx"),
            ("../../etc/passwd", "passwd"),
            ("/abs/path/report.csv", "report.csv"),
            ("we!rd  na@me.txt", "we_rd_na_me.txt"),
            ("", "upload"),
            ("...", "upload"),
            ("..", "upload"),
        ],
    )
    def test_sanitizing(self, raw: str, expected: str) -> None:
        assert files.safe_filename(raw) == expected

    def test_name_is_length_capped(self) -> None:
        assert len(files.safe_filename("a" * 500 + ".xlsx")) == 200

    def test_no_result_can_escape_a_directory(self) -> None:
        for raw in ("../../x", "/etc/shadow", "..\\..\\win.ini", "a/b/c"):
            cleaned = files.safe_filename(raw)
            assert "/" not in cleaned and "\\" not in cleaned
            assert cleaned not in ("", ".", "..")


class TestDomainErrors:
    """Status codes belong to the error type, so routes need no mapping."""

    @pytest.mark.parametrize(
        ("exc", "code"),
        [
            (InvalidRequest, 400),
            (Unauthorized, 401),
            (NotFound, 404),
            (Conflict, 409),
            (PayloadTooLarge, 413),
        ],
    )
    def test_status_codes(self, exc: type[DomainError], code: int) -> None:
        assert exc("nope").status_code == code

    def test_detail_is_the_message(self) -> None:
        err = NotFound("conversation not found")
        assert err.detail == "conversation not found"
        assert str(err) == "conversation not found"


class TestUploadDurability:
    """store_upload write-through: a durability failure must not leave a
    half-persisted file in the sandbox workspace."""

    def _session(self):
        from app.infra.config import get_settings
        from app.infra.db import get_session_factory, reset_engine_for_tests

        reset_engine_for_tests(get_settings().db_url)
        return get_session_factory()()

    def _user_conv(self, db):
        from app.models import Conversation, User, new_id

        user = User(id=new_id("usr"), email="up@example.com", password_hash="x")
        db.add(user)
        db.flush()
        conv = Conversation(id=new_id("cnv"), user_id=user.id, title="t")
        db.add(conv)
        db.flush()
        return user, conv

    def test_durability_failure_unlinks_local_file(self) -> None:
        import asyncio

        from app.controllers import files
        from app.infra.config import conversation_workspace
        from app.infra.storage import StorageError

        class _BrokenStorage:
            def put_stream(self, key, stream):
                raise StorageError("durable tier down")

        async def _run(db, user, conv):
            chunks = [b"partial-bytes", b""]

            async def read_chunk(_n: int) -> bytes:
                return chunks.pop(0)

            await files.store_upload(
                db,
                user,
                conv,
                filename="x.csv",
                read_chunk=read_chunk,
                storage=_BrokenStorage(),
            )

        with self._session() as db:
            user, conv = self._user_conv(db)
            with pytest.raises(StorageError):
                asyncio.run(_run(db, user, conv))
            # no file left behind in the workspace, and no DB row committed
            workspace = conversation_workspace(conv.id)
            assert list(workspace.iterdir()) == []
            from app.models import ConversationFile

            assert db.query(ConversationFile).count() == 0

    def test_success_persists_to_both_tiers(self) -> None:
        import asyncio
        from pathlib import Path

        from app.controllers import files

        class _MemStorage:
            def __init__(self):
                self.puts = {}

            def put_stream(self, key, stream):
                data = stream.read()
                self.puts[key] = data
                return len(data)

        store = _MemStorage()

        async def _run(db, user, conv):
            chunks = [b"hello,world\n", b""]

            async def read_chunk(_n: int) -> bytes:
                return chunks.pop(0)

            return await files.store_upload(
                db, user, conv, filename="x.csv", read_chunk=read_chunk, storage=store
            )

        with self._session() as db:
            user, conv = self._user_conv(db)
            row = asyncio.run(_run(db, user, conv))
            key = files.durable_key(conv.id, row.stored_name)
            assert store.puts[key] == b"hello,world\n"  # durable tier
            assert Path(row.path).read_bytes() == b"hello,world\n"  # sandbox tier
