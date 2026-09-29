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

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..infra.config import conversation_workspace, get_settings
from ..infra.db import commit_now
from ..infra.filetype import sniff_matches
from ..infra.storage import Storage, get_storage
from ..models import Conversation, ConversationFile, User, new_id
from .access import get_owned
from .errors import InvalidRequest, NotFound, PayloadTooLarge

MAX_UPLOAD_BYTES = 100 * 1024 * 1024
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def user_storage_used(db: Session, user_id: str) -> int:
    """Total bytes the user currently stores across all conversations."""
    total = db.scalar(
        select(func.coalesce(func.sum(ConversationFile.size), 0)).where(
            ConversationFile.user_id == user_id
        )
    )
    return int(total or 0)


def durable_key(conversation_id: str, stored_name: str) -> str:
    """Key for a conversation's file in the durable store.

    Mirrors the on-disk layout (``<conversation>/<stored_name>``) so the
    durable tier and the sandbox-facing workspace stay legible against
    each other.
    """
    return f"{conversation_id}/{stored_name}"


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
    storage: Storage | None = None,
) -> ConversationFile:
    """Stream an upload into the conversation's agent workspace, then
    write it through to the durable store.

    Two tiers: the file lands in the sandbox cwd so the agent can open
    it by name (the *sandbox-facing* tier), and is copied to durable
    ``Storage`` (the *system of record*) so it survives pod loss -- the
    workspace can be rehydrated from there.  ``read_chunk`` is the
    transport's reader (an ``UploadFile.read``), keeping this free of
    any HTTP type while still streaming rather than buffering the whole
    body.  A file over the limit is removed again -- a partial upload
    must not linger in the agent's cwd.
    """
    clean = safe_filename(filename or "upload")
    stored_name = f"{uuid.uuid4().hex[:8]}_{clean}"
    dest = conversation_workspace(conversation.id) / stored_name
    settings = get_settings()
    max_file = settings.max_upload_bytes
    quota = settings.max_user_storage_bytes
    # remaining room under the user's quota (0 quota = unlimited)
    used = user_storage_used(db, user.id) if quota else 0
    size = 0
    first = True
    with dest.open("wb") as out:
        while chunk := await read_chunk(1024 * 1024):
            if first:
                first = False
                # magic-byte gate: the leading bytes must match one of the
                # allow-listed content types (no-op when the list is empty)
                if not sniff_matches(chunk, settings.upload_allowed_types):
                    dest.unlink(missing_ok=True)
                    raise InvalidRequest("file content does not match an allowed type")
            size += len(chunk)
            if size > max_file:
                dest.unlink(missing_ok=True)
                raise PayloadTooLarge("file too large")
            if quota and used + size > quota:
                dest.unlink(missing_ok=True)
                raise PayloadTooLarge("storage quota exceeded")
            out.write(chunk)
    # write-through to the durable tier: stream the completed local file
    # so the copy is bounded in memory.  A durability failure removes the
    # local file too -- a half-persisted upload (in cwd but not durable)
    # would silently vanish on the next pod loss.
    store = storage or get_storage()
    try:
        with dest.open("rb") as fh:
            store.put_stream(durable_key(conversation.id, stored_name), fh)
    except Exception:
        dest.unlink(missing_ok=True)
        raise
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
    # Post-commit quota re-check: the pre-write check used a snapshot, so
    # concurrent uploads by the same user could each pass it and both
    # commit (TOCTOU).  Re-read the committed total and, if this upload
    # pushed the user over, roll THIS one back -- turning an unbounded
    # overrun into a bounded, self-correcting one (the loser of the race
    # is rejected).  The winner keeps its file.
    if quota:
        total = user_storage_used(db, user.id)
        if total > quota:
            with contextlib.suppress(Exception):
                store.delete(durable_key(conversation.id, stored_name))
            with contextlib.suppress(OSError):
                dest.unlink()
            db.delete(row)
            commit_now(db)
            raise PayloadTooLarge("storage quota exceeded")
    return row


def delete_upload(
    db: Session, conversation: Conversation, file_id: str, storage: Storage | None = None
) -> None:
    """Forget an upload: remove it from the durable store and the
    workspace, then drop the row."""
    row = get_owned(
        db,
        ConversationFile,
        file_id,
        owner_field="conversation_id",
        owner_value=conversation.id,
        detail="file not found",
    )
    store = storage or get_storage()
    with contextlib.suppress(Exception):
        store.delete(durable_key(conversation.id, row.stored_name))
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


def sync_artifacts(
    db: Session, conversation: Conversation, storage: Storage | None = None
) -> list[str]:
    """Write agent-produced files through to the durable store.

    Called after a run finishes: the harness writes outputs to its local
    workspace (its cwd), which is ephemeral -- this copies each new
    artifact to durable ``Storage`` so outputs survive pod loss and are
    visible from any instance.  Uploads (tracked rows) are skipped; they
    were already persisted at upload time.  Returns the artifact names
    synced.  Best-effort per file: one failure does not abort the rest.
    """
    store = storage or get_storage()
    synced: list[str] = []
    for art in list_artifacts(db, conversation):
        key = durable_key(conversation.id, art.name)
        # skip files already durable with the same size: re-uploading the
        # entire artifact set on every run is O(all files) work and real
        # object-store cost on a long-lived conversation.  Size is a cheap
        # proxy for "unchanged"; an agent that rewrites a file to the same
        # length is a rare miss and self-heals on the next differing run.
        if store.size(key) == art.size:
            continue
        local = conversation_workspace(conversation.id) / art.name
        try:
            with local.open("rb") as fh:
                store.put_stream(key, fh)
            synced.append(art.name)
        except Exception:  # never let one file break the sweep
            continue
    return synced


def artifact_path(conversation: Conversation, name: str, storage: Storage | None = None) -> Path:
    """On-disk path of one agent-produced file, rehydrating from durable
    storage if the local workspace copy is absent.

    Only a basename is accepted, so a crafted name cannot walk out of
    the workspace.  On a fresh instance (or after pod loss) the local
    copy may be gone while the durable copy remains -- in that case the
    file is pulled back into the workspace so it can be served.
    """
    if not name or Path(name).name != name:
        raise InvalidRequest("invalid file name")
    path = conversation_workspace(conversation.id) / name
    if path.is_file():
        return path
    # not local: try to rehydrate from the durable tier
    store = storage or get_storage()
    key = durable_key(conversation.id, name)
    if store.exists(key):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as out:
            for chunk in store.open_stream(key):
                out.write(chunk)
        return path
    raise NotFound("artifact not found")
