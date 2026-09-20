"""Conversation + run routes: CRUD, start run, SSE event stream."""

from __future__ import annotations

import asyncio
import queue

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from ..controller.manager import Controller, get_controller
from ..db import get_db
from ..models import Conversation, Run, User, new_id
from ..routes.auth import authenticate_user
from ..schemas import (
    ConversationCreate,
    ConversationDetail,
    ConversationOut,
    RunCreate,
    RunOut,
)

router = APIRouter(prefix="/conversations", tags=["conversations"])


def _own_conversation(db: Session, user: User, conversation_id: str) -> Conversation:
    conversation = db.get(Conversation, conversation_id)
    if conversation is None or conversation.user_id != user.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "conversation not found")
    return conversation


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
    return ConversationDetail(
        id=conversation.id,
        title=conversation.title,
        created_at=conversation.created_at,
        updated_at=conversation.updated_at,
        runs=[_run_out(r) for r in conversation.runs],
    )


@router.delete("/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_conversation(
    conversation_id: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> None:
    conversation = _own_conversation(db, user, conversation_id)
    db.delete(conversation)


@router.post("/{conversation_id}/runs", response_model=RunOut, status_code=status.HTTP_202_ACCEPTED)
def start_run(
    conversation_id: str,
    body: RunCreate,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
    controller: Controller = Depends(get_controller),
) -> RunOut:
    conversation = _own_conversation(db, user, conversation_id)
    try:
        run = controller.start_run(
            db, user_id=user.id, conversation_id=conversation.id, prompt=body.prompt
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
