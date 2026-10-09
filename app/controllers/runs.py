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

from ..models import Conversation, ConversationFile, Run, User
from .access import get_owned
from .conversations import files_of
from .errors import Conflict
from .manager import Controller
from .usage import enforce_budget

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
    # Spend kill-switch: refuse before doing any work if the user is out
    # of token budget (no-op unless budget enforcement is enabled).
    user = db.get(User, user_id)
    if user is not None:
        enforce_budget(db, user)
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
    return get_owned(
        db,
        Run,
        run_id,
        owner_field="conversation_id",
        owner_value=conversation.id,
        detail="run not found",
    )


def cancel(controller: Controller, run_id: str) -> bool:
    """Request cancellation; False when the run is no longer live."""
    return controller.cancel_run(run_id)


def answer(
    controller: Controller, run_id: str, answers: list[str], ask_id: str | None = None
) -> None:
    """Forward a reply to a pending mid-run question.

    The harness's ask event (notify kind ``ask``) carries the questions
    and an ``ask_id``; the reply goes to the blocked agent verbatim,
    tagged with the id so it cannot resolve a different question.
    """
    if not controller.deliver_answer(run_id, answers, ask_id):
        raise Conflict("run is not accepting answers (finished, or runner lacks mid-run Q&A)")


def replay_payloads(run: Run, since_seq: int = 0) -> list[dict[str, Any]]:
    """Stored transcript of a finished run, terminal event included.

    A late subscriber gets the same event sequence a live one saw, so
    the browser renders a finished run exactly like it watched it.
    ``since_seq`` skips what a reconnecting client already has; the
    terminal event has no ``seq`` and is always included, so a resume
    can never leave the client hanging on a finished run.
    """
    stored = [
        e
        for e in (run.events or [])
        if not (since_seq > 0 and isinstance(e.get("seq"), int) and e["seq"] <= since_seq)
    ]
    return [*stored, {"type": "run", "state": run.status}]
