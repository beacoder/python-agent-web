"""Run business logic: what the agent is told, and what a caller may do.

``manager.Controller`` owns execution (threads, sandboxes, event
fan-out).  This module sits in front of it with the per-request rules:
augmenting the prompt with workspace context, checking that a run
belongs to the conversation, and turning a refused cancel/answer into a
domain error.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from ..models import Conversation, ConversationFile, Run
from .conversations import files_of
from .errors import Conflict, NotFound
from .manager import Controller

# Told to the agent, not to the user: the sandbox cwd is the only place
# a produced file can be picked up from, and the model has no other way
# to learn that.
_SAVE_HINT = (
    "\n\n[Save any result files (e.g. output Excel) into your working "
    "directory with a clear name; the user downloads them from there.]"
)


def build_harness_prompt(prompt: str, files: list[ConversationFile]) -> str:
    """The prompt the agent receives, given the conversation's uploads.

    The Run row keeps the user's verbatim prompt (the UI echoes it);
    this is the augmented copy.  Uploads are named by ``stored_name``,
    not ``filename``: the on-disk name carries a unique prefix, and the
    agent must open the file that actually exists.
    """
    augmented = prompt
    if files:
        names = ", ".join(f.stored_name for f in files)
        augmented += f"\n\n[Uploaded files in your working directory: {names}]"
    return augmented + _SAVE_HINT


def start(
    controller: Controller,
    db: Session,
    *,
    user_id: str,
    conversation: Conversation,
    prompt: str,
) -> Run:
    """Begin a run for the conversation's next turn."""
    harness_prompt = build_harness_prompt(prompt, files_of(db, conversation))
    try:
        return controller.start_run(
            db,
            user_id=user_id,
            conversation_id=conversation.id,
            prompt=prompt,
            harness_prompt=harness_prompt,
        )
    except RuntimeError as exc:
        # one run at a time per conversation (enforced by the manager)
        raise Conflict(str(exc)) from exc


def of_conversation(db: Session, conversation: Conversation, run_id: str) -> Run:
    """A run belonging to this conversation, or NotFound."""
    run = db.get(Run, run_id)
    if run is None or run.conversation_id != conversation.id:
        raise NotFound("run not found")
    return run


def cancel(controller: Controller, run_id: str) -> bool:
    """Request cancellation; False when the run is no longer live."""
    return controller.cancel_run(run_id)


def answer(controller: Controller, run_id: str, answers: list[str]) -> None:
    """Forward a reply to a pending mid-run question.

    The harness's ask event (notify kind ``ask``) carries the questions;
    the reply goes to the blocked agent verbatim.
    """
    if not controller.deliver_answer(run_id, answers):
        raise Conflict("run is not accepting answers (finished, or runner lacks mid-run Q&A)")


def replay_payloads(run: Run) -> list[dict[str, Any]]:
    """Stored transcript of a finished run, terminal event included.

    A late subscriber gets the same event sequence a live one saw, so
    the browser renders a finished run exactly like it watched it.
    """
    return [*(run.events or []), {"type": "run", "state": run.status}]
