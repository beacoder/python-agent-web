"""Conversation + run routes: CRUD, file upload, start run, SSE stream."""

from __future__ import annotations

import asyncio
import contextlib
import queue
import re
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session

from ..controller.manager import Controller, get_controller
from ..core.config import conversation_workspace, get_settings
from ..db import commit_now, get_db
from ..models import Conversation, ConversationFile, Run, User, new_id
from ..routes.auth import authenticate_user
from ..schemas import (
    ArtifactOut,
    ConversationCreate,
    ConversationDetail,
    ConversationOut,
    FileOut,
    RunAnswer,
    RunCreate,
    RunOut,
)

MAX_UPLOAD_BYTES = 100 * 1024 * 1024
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")

router = APIRouter(prefix="/conversations", tags=["conversations"])


def _own_conversation(db: Session, user: User, conversation_id: str) -> Conversation:
    conversation = db.get(Conversation, conversation_id)
    if conversation is None or conversation.user_id != user.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "conversation not found")
    return conversation


def _file_out(f: ConversationFile) -> FileOut:
    return FileOut(id=f.id, filename=f.filename, size=f.size, created_at=f.created_at)


def _safe_filename(name: str) -> str:
    """Strip path components and unsafe characters; keep the suffix."""
    base = Path(name).name or "upload"
    cleaned = _SAFE_NAME.sub("_", base).strip("._") or "upload"
    return cleaned[:200]


def _run_out(run: Run) -> RunOut:
    return RunOut(
        id=run.id,
        status=run.status,
        prompt=run.prompt,
        answer=run.answer,
        error=run.error,
        cancelled=run.cancelled,
        exit_code=run.exit_code,
        duration_ms=run.duration_ms,
        created_at=run.created_at,
        finished_at=run.finished_at,
    )


@router.post("", response_model=ConversationOut, status_code=status.HTTP_201_CREATED)
def create_conversation(
    body: ConversationCreate,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> ConversationOut:
    conversation = Conversation(id=new_id("cnv"), user_id=user.id, title=body.title)
    db.add(conversation)
    db.flush()
    commit_now(db)
    return ConversationOut(
        id=conversation.id,
        title=conversation.title,
        created_at=conversation.created_at,
        updated_at=conversation.updated_at,
    )


@router.get("", response_model=list[ConversationOut])
def list_conversations(
    user: User = Depends(authenticate_user), db: Session = Depends(get_db)
) -> list[ConversationOut]:
    rows = (
        db.query(Conversation)
        .filter(Conversation.user_id == user.id)
        .order_by(Conversation.updated_at.desc())
        .all()
    )
    return [
        ConversationOut(id=c.id, title=c.title, created_at=c.created_at, updated_at=c.updated_at)
        for c in rows
    ]


@router.get("/{conversation_id}", response_model=ConversationDetail)
def get_conversation(
    conversation_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> ConversationDetail:
    conversation = _own_conversation(db, user, conversation_id)
    files = (
        db.query(ConversationFile)
        .filter(ConversationFile.conversation_id == conversation.id)
        .order_by(ConversationFile.created_at)
        .all()
    )
    return ConversationDetail(
        id=conversation.id,
        title=conversation.title,
        created_at=conversation.created_at,
        updated_at=conversation.updated_at,
        runs=[_run_out(r) for r in conversation.runs],
        files=[_file_out(f) for f in files],
    )


@router.post(
    "/{conversation_id}/files", response_model=FileOut, status_code=status.HTTP_201_CREATED
)
async def upload_file(
    conversation_id: str,
    file: UploadFile = File(...),
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> FileOut:
    """Store an upload in the conversation's agent workspace.

    The file lands in the sandbox cwd, so the agent can open it by
    name; the run prompt is told about it (see ``start_run``).
    """
    conversation = _own_conversation(db, user, conversation_id)
    filename = _safe_filename(file.filename or "upload")
    workspace = conversation_workspace(conversation.id)
    stored_name = f"{uuid.uuid4().hex[:8]}_{filename}"
    dest = workspace / stored_name
    size = 0
    with dest.open("wb") as out:
        while chunk := await file.read(1024 * 1024):
            size += len(chunk)
            if size > MAX_UPLOAD_BYTES:
                dest.unlink(missing_ok=True)
                raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "file too large")
            out.write(chunk)
    row = ConversationFile(
        id=new_id("file"),
        conversation_id=conversation.id,
        user_id=user.id,
        filename=filename,
        stored_name=stored_name,
        size=size,
        path=str(dest),
    )
    db.add(row)
    db.flush()
    commit_now(db)
    return _file_out(row)


@router.delete("/{conversation_id}/files/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_file(
    conversation_id: str,
    file_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> None:
    conversation = _own_conversation(db, user, conversation_id)
    row = db.get(ConversationFile, file_id)
    if row is None or row.conversation_id != conversation.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "file not found")
    with contextlib.suppress(OSError):
        Path(row.path).unlink()
    db.delete(row)
    commit_now(db)


@router.get("/{conversation_id}/artifacts", response_model=list[ArtifactOut])
def list_artifacts(
    conversation_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> list[ArtifactOut]:
    """List files the agent produced in its workspace.

    The workspace doubles as the upload dir, so user uploads (tracked
    in ``conversation_files``) are excluded — everything else is an
    agent output.
    """
    conversation = _own_conversation(db, user, conversation_id)
    stored = {
        row[0]
        for row in db.query(ConversationFile.stored_name).filter(
            ConversationFile.conversation_id == conversation.id
        )
    }
    workspace = conversation_workspace(conversation.id)
    artifacts = []
    for entry in sorted(workspace.iterdir()):
        if not entry.is_file() or entry.name in stored:
            continue
        stat = entry.stat()
        artifacts.append(
            ArtifactOut(
                name=entry.name,
                size=stat.st_size,
                modified_at=datetime.fromtimestamp(stat.st_mtime, UTC),
            )
        )
    return artifacts


@router.get("/{conversation_id}/artifacts/{name}")
def download_artifact(
    conversation_id: str,
    name: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> FileResponse:
    """Download one agent-produced file (basename only, no traversal)."""
    conversation = _own_conversation(db, user, conversation_id)
    if Path(name).name != name or not name:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid file name")
    path = conversation_workspace(conversation.id) / name
    if not path.is_file():
        raise HTTPException(status.HTTP_404_NOT_FOUND, "artifact not found")
    return FileResponse(path, filename=name)


@router.delete("/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_conversation(
    conversation_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> None:
    conversation = _own_conversation(db, user, conversation_id)
    # Resolve the workspace BEFORE deleting: conversation_workspace()
    # mkdirs, and calling it after the delete would recreate the dir.
    workspace = None if get_settings().harness.cwd else conversation_workspace(conversation.id)
    for row in (
        db.query(ConversationFile).filter(ConversationFile.conversation_id == conversation.id).all()
    ):
        with contextlib.suppress(OSError):
            Path(row.path).unlink()
        db.delete(row)
    db.delete(conversation)
    commit_now(db)
    if workspace is not None:
        with contextlib.suppress(OSError):
            shutil.rmtree(workspace, ignore_errors=True)


@router.post("/{conversation_id}/runs", response_model=RunOut, status_code=status.HTTP_202_ACCEPTED)
def start_run(
    conversation_id: str,
    body: RunCreate,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
    controller: Controller = Depends(get_controller),
) -> RunOut:
    conversation = _own_conversation(db, user, conversation_id)
    prompt = body.prompt
    files = (
        db.query(ConversationFile)
        .filter(ConversationFile.conversation_id == conversation.id)
        .order_by(ConversationFile.created_at)
        .all()
    )
    if files:
        # stored_name (not filename): the on-disk name carries a unique
        # prefix, and the agent must open the file that actually exists
        names = ", ".join(f.stored_name for f in files)
        prompt += f"\n\n[Uploaded files in your working directory: {names}]"
    prompt += (
        "\n\n[Save any result files (e.g. output Excel) into your working "
        "directory with a clear name; the user downloads them from there.]"
    )
    try:
        run = controller.start_run(
            db, user_id=user.id, conversation_id=conversation.id, prompt=prompt
        )
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    return _run_out(run)


@router.get("/{conversation_id}/runs/{run_id}", response_model=RunOut)
def get_run(
    conversation_id: str,
    run_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> RunOut:
    conversation = _own_conversation(db, user, conversation_id)
    run = db.get(Run, run_id)
    if run is None or run.conversation_id != conversation.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return _run_out(run)


@router.post("/{conversation_id}/runs/{run_id}/cancel")
def cancel_run(
    conversation_id: str,
    run_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
    controller: Controller = Depends(get_controller),
) -> dict:
    conversation = _own_conversation(db, user, conversation_id)
    run = db.get(Run, run_id)
    if run is None or run.conversation_id != conversation.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return {"cancelled": controller.cancel_run(run_id)}


@router.post("/{conversation_id}/runs/{run_id}/answer")
def answer_run(
    conversation_id: str,
    run_id: str,
    body: RunAnswer,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
    controller: Controller = Depends(get_controller),
) -> dict:
    """Answer a pending mid-run question.

    The harness's ask event (notify kind ``ask`` on the SSE stream)
    carries the questions; the user's reply is forwarded verbatim to
    the blocked agent.  409 when the run is not live.
    """
    conversation = _own_conversation(db, user, conversation_id)
    run = db.get(Run, run_id)
    if run is None or run.conversation_id != conversation.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    if not controller.deliver_answer(run_id, body.answers):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "run is not accepting answers (finished, or runner lacks mid-run Q&A)",
        )
    return {"delivered": True}


@router.get("/{conversation_id}/runs/{run_id}/stream")
def stream_run(
    conversation_id: str,
    run_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
    controller: Controller = Depends(get_controller),
) -> StreamingResponse:
    """SSE stream of harness events for one run.

    Events are relayed as JSON; a terminal ``{"type": "run", ...}``
    event (state done/error/cancelled) closes the stream.
    """
    conversation = _own_conversation(db, user, conversation_id)
    run = db.get(Run, run_id)
    if run is None or run.conversation_id != conversation.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")

    q = controller.subscribe(run_id)
    if q is None:
        # run already finished: replay the stored transcript once
        events = list(run.events or [])

        def _replay() -> str:
            import json

            out = ""
            for event in events:
                out += f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            out += f'data: {{"type": "run", "state": "{run.status}"}}\n\n'
            return out

        return StreamingResponse(_replay(), media_type="text/event-stream")

    async def _stream():
        import json

        loop = asyncio.get_running_loop()
        while True:
            try:
                event = await loop.run_in_executor(None, q.get, True, 1.0)
            except queue.Empty:
                yield ": keepalive\n\n"
                continue
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            if event.get("type") == "run":
                controller.unsubscribe(run_id, q)
                break

    return StreamingResponse(_stream(), media_type="text/event-stream")
