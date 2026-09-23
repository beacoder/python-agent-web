"""Conversation + run routes: CRUD, file upload, start run, SSE stream.

HTTP wiring and serialization only.  Ownership, upload limits, artifact
rules, prompt augmentation and run state all live in the matching
controllers (``conversations``, ``files``, ``runs``).
"""

from __future__ import annotations

import asyncio
import json
import queue

from fastapi import APIRouter, Depends, File, UploadFile, status
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session

from ..controllers import conversations as conversations_controller
from ..controllers import files as files_controller
from ..controllers import runs as runs_controller
from ..controllers.manager import Controller, get_controller
from ..models import Conversation, ConversationFile, Run, User
from ..models.db import get_db
from ..routes.auth import authenticate_user
from ..validation.schemas import (
    ArtifactOut,
    ConversationCreate,
    ConversationDetail,
    ConversationOut,
    FileOut,
    RunAnswer,
    RunCreate,
    RunEvent,
    RunOut,
)

router = APIRouter(prefix="/conversations", tags=["conversations"])


def _conversation_out(c: Conversation) -> ConversationOut:
    return ConversationOut(id=c.id, title=c.title, created_at=c.created_at, updated_at=c.updated_at)


def _file_out(f: ConversationFile) -> FileOut:
    return FileOut(id=f.id, filename=f.filename, size=f.size, created_at=f.created_at)


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
        events=[RunEvent(type=e["type"], data=e.get("data") or {}) for e in run.events or []],
    )


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


@router.post("", response_model=ConversationOut, status_code=status.HTTP_201_CREATED)
def create_conversation(
    body: ConversationCreate,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> ConversationOut:
    return _conversation_out(conversations_controller.create(db, user, body.title))


@router.get("", response_model=list[ConversationOut])
def list_conversations(
    user: User = Depends(authenticate_user), db: Session = Depends(get_db)
) -> list[ConversationOut]:
    return [_conversation_out(c) for c in conversations_controller.list_for_user(db, user)]


@router.get("/{conversation_id}", response_model=ConversationDetail)
def get_conversation(
    conversation_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> ConversationDetail:
    conversation = conversations_controller.owned(db, user, conversation_id)
    files = conversations_controller.files_of(db, conversation)
    return ConversationDetail(
        id=conversation.id,
        title=conversation.title,
        created_at=conversation.created_at,
        updated_at=conversation.updated_at,
        runs=[_run_out(r) for r in conversation.runs],
        files=[_file_out(f) for f in files],
    )


@router.delete("/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_conversation(
    conversation_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> None:
    conversations_controller.delete(db, user, conversation_id)


@router.post(
    "/{conversation_id}/files", response_model=FileOut, status_code=status.HTTP_201_CREATED
)
async def upload_file(
    conversation_id: str,
    file: UploadFile = File(...),
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> FileOut:
    conversation = conversations_controller.owned(db, user, conversation_id)
    row = await files_controller.store_upload(
        db,
        user,
        conversation,
        filename=file.filename,
        read_chunk=file.read,
    )
    return _file_out(row)


@router.delete("/{conversation_id}/files/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_file(
    conversation_id: str,
    file_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> None:
    conversation = conversations_controller.owned(db, user, conversation_id)
    files_controller.delete_upload(db, conversation, file_id)


@router.get("/{conversation_id}/artifacts", response_model=list[ArtifactOut])
def list_artifacts(
    conversation_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> list[ArtifactOut]:
    conversation = conversations_controller.owned(db, user, conversation_id)
    return [
        ArtifactOut(name=a.name, size=a.size, modified_at=a.modified_at)
        for a in files_controller.list_artifacts(db, conversation)
    ]


@router.get("/{conversation_id}/artifacts/{name}")
def download_artifact(
    conversation_id: str,
    name: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> FileResponse:
    conversation = conversations_controller.owned(db, user, conversation_id)
    return FileResponse(files_controller.artifact_path(conversation, name), filename=name)


@router.post("/{conversation_id}/runs", response_model=RunOut, status_code=status.HTTP_202_ACCEPTED)
def start_run(
    conversation_id: str,
    body: RunCreate,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
    controller: Controller = Depends(get_controller),
) -> RunOut:
    conversation = conversations_controller.owned(db, user, conversation_id)
    run = runs_controller.start(
        controller, db, user_id=user.id, conversation=conversation, prompt=body.prompt
    )
    return _run_out(run)


@router.get("/{conversation_id}/runs/{run_id}", response_model=RunOut)
def get_run(
    conversation_id: str,
    run_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> RunOut:
    conversation = conversations_controller.owned(db, user, conversation_id)
    return _run_out(runs_controller.of_conversation(db, conversation, run_id))


@router.post("/{conversation_id}/runs/{run_id}/cancel")
def cancel_run(
    conversation_id: str,
    run_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
    controller: Controller = Depends(get_controller),
) -> dict:
    conversation = conversations_controller.owned(db, user, conversation_id)
    runs_controller.of_conversation(db, conversation, run_id)
    return {"cancelled": runs_controller.cancel(controller, run_id)}


@router.post("/{conversation_id}/runs/{run_id}/answer")
def answer_run(
    conversation_id: str,
    run_id: str,
    body: RunAnswer,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
    controller: Controller = Depends(get_controller),
) -> dict:
    conversation = conversations_controller.owned(db, user, conversation_id)
    runs_controller.of_conversation(db, conversation, run_id)
    runs_controller.answer(controller, run_id, body.answers)
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
    conversation = conversations_controller.owned(db, user, conversation_id)
    run = runs_controller.of_conversation(db, conversation, run_id)

    q = controller.subscribe(run_id)
    if q is None:
        # run already finished: replay the stored transcript once
        payloads = runs_controller.replay_payloads(run)
        return StreamingResponse("".join(_sse(p) for p in payloads), media_type="text/event-stream")

    async def _stream():
        loop = asyncio.get_running_loop()
        while True:
            try:
                event = await loop.run_in_executor(None, q.get, True, 1.0)
            except queue.Empty:
                yield ": keepalive\n\n"
                continue
            yield _sse(event)
            if event.get("type") == "run":
                controller.unsubscribe(run_id, q)
                break

    return StreamingResponse(_stream(), media_type="text/event-stream")
