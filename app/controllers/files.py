"""Upload and artifact business logic for a conversation workspace.

The workspace is the agent's cwd, so it holds two kinds of file: what
the user uploaded (tracked in ``conversation_files``) and what the
agent produced (everything else).  That distinction, the name
sanitizing, and the size limit are the rules this module owns.
"""

from __future__ import annotations

import contextlib
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.orm import Session

from ..infra.config import conversation_workspace
from ..infra.db import commit_now
from ..models import Conversation, ConversationFile, User, new_id
from .access import get_owned
from .errors import InvalidRequest, NotFound, PayloadTooLarge

MAX_UPLOAD_BYTES = 100 * 1024 * 1024
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class ArtifactInfo:
    """One agent-produced file on disk (no DB row backs these)."""

    name: str
    size: int
    modified_at: datetime


def safe_filename(name: str) -> str:
    """Strip path components and unsafe characters; keep the suffix."""
    base = Path(name).name or "upload"
    cleaned = _SAFE_NAME.sub("_", base).strip("._") or "upload"
    return cleaned[:200]


async def store_upload(
    db: Session,
    user: User,
    conversation: Conversation,
    *,
    filename: str | None,
    read_chunk: Callable[[int], Awaitable[bytes]],
) -> ConversationFile:
    """Stream an upload into the conversation's agent workspace.

    The file lands in the sandbox cwd so the agent can open it by name.
    ``read_chunk`` is the transport's reader (an ``UploadFile.read``),
    which keeps this free of any HTTP type while still streaming rather
    than buffering the whole body.  A file over the limit is removed
    again -- a partial upload must not linger in the agent's cwd.
    """
    clean = safe_filename(filename or "upload")
    stored_name = f"{uuid.uuid4().hex[:8]}_{clean}"
    dest = conversation_workspace(conversation.id) / stored_name
    size = 0
    with dest.open("wb") as out:
        while chunk := await read_chunk(1024 * 1024):
            size += len(chunk)
            if size > MAX_UPLOAD_BYTES:
                dest.unlink(missing_ok=True)
                raise PayloadTooLarge("file too large")
            out.write(chunk)
    row = ConversationFile(
        id=new_id("file"),
        conversation_id=conversation.id,
        user_id=user.id,
        filename=clean,
        stored_name=stored_name,
        size=size,
        path=str(dest),
    )
    db.add(row)
    db.flush()
    commit_now(db)
    return row


def delete_upload(db: Session, conversation: Conversation, file_id: str) -> None:
    """Forget an upload and unlink it from the workspace."""
    row = get_owned(
        db,
        ConversationFile,
        file_id,
        owner_field="conversation_id",
        owner_value=conversation.id,
        detail="file not found",
    )
    with contextlib.suppress(OSError):
        Path(row.path).unlink()
    db.delete(row)
    commit_now(db)


def list_artifacts(db: Session, conversation: Conversation) -> list[ArtifactInfo]:
    """Files the agent produced in its workspace.

    The workspace doubles as the upload dir, so user uploads are
    excluded -- everything else is an agent output.
    """
    stored = {
        row[0]
        for row in db.query(ConversationFile.stored_name).filter(
            ConversationFile.conversation_id == conversation.id
        )
    }
    artifacts = []
    for entry in sorted(conversation_workspace(conversation.id).iterdir()):
        if not entry.is_file() or entry.name in stored:
            continue
        stat = entry.stat()
        artifacts.append(
            ArtifactInfo(
                name=entry.name,
                size=stat.st_size,
                modified_at=datetime.fromtimestamp(stat.st_mtime, UTC),
            )
        )
    return artifacts


def artifact_path(conversation: Conversation, name: str) -> Path:
    """On-disk path of one agent-produced file.

    Only a basename is accepted, so a crafted name cannot walk out of
    the workspace.
    """
    if not name or Path(name).name != name:
        raise InvalidRequest("invalid file name")
    path = conversation_workspace(conversation.id) / name
    if not path.is_file():
        raise NotFound("artifact not found")
    return path
