"""Conversation business logic: ownership, CRUD, workspace teardown.

The routes above this module only wire HTTP to these calls; every rule
about what a user may see or delete lives here.
"""

from __future__ import annotations

import contextlib
import shutil
from pathlib import Path

from sqlalchemy.orm import Session

from ..infra.config import conversation_workspace, get_settings
from ..infra.db import commit_now
from ..models import Conversation, ConversationFile, User, new_id
from .access import get_owned


def owned(db: Session, user: User, conversation_id: str) -> Conversation:
    """The user's conversation, or NotFound.

    Someone else's conversation is reported missing rather than
    forbidden, so an id cannot be probed for existence.
    """
    return get_owned(
        db,
        Conversation,
        conversation_id,
        owner_field="user_id",
        owner_value=user.id,
        detail="conversation not found",
    )


def create(db: Session, user: User, title: str | None) -> Conversation:
    conversation = Conversation(id=new_id("cnv"), user_id=user.id, title=title)
    db.add(conversation)
    db.flush()
    commit_now(db)
    return conversation


def list_for_user(db: Session, user: User) -> list[Conversation]:
    """The user's conversations, most recently touched first."""
    return (
        db.query(Conversation)
        .filter(Conversation.user_id == user.id)
        .order_by(Conversation.updated_at.desc())
        .all()
    )


def files_of(db: Session, conversation: Conversation) -> list[ConversationFile]:
    """Uploads attached to a conversation, oldest first."""
    return (
        db.query(ConversationFile)
        .filter(ConversationFile.conversation_id == conversation.id)
        .order_by(ConversationFile.created_at)
        .all()
    )


def delete(db: Session, user: User, conversation_id: str) -> None:
    """Delete a conversation with its uploads and agent workspace."""
    conversation = owned(db, user, conversation_id)
    # Resolve the workspace BEFORE deleting: conversation_workspace()
    # mkdirs, and calling it after the delete would recreate the dir.
    workspace = None if get_settings().harness.cwd else conversation_workspace(conversation.id)
    for row in files_of(db, conversation):
        with contextlib.suppress(OSError):
            Path(row.path).unlink()
        db.delete(row)
    db.delete(conversation)
    commit_now(db)
    if workspace is not None:
        with contextlib.suppress(OSError):
            shutil.rmtree(workspace, ignore_errors=True)
