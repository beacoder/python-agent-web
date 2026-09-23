"""Usage business logic: aggregate the token ledger for a user."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import Conversation, Run, UsageEvent, User


@dataclass
class UsageTotals:
    """A user's billed totals, plus a per-conversation token breakdown."""

    input_tokens: int = 0
    output_tokens: int = 0
    rounds: int = 0
    runs: int = 0
    by_conversation: dict[str, int] = field(default_factory=dict)


def summary(db: Session, user: User) -> UsageTotals:
    """Totals over the user's usage events, grouped per conversation.

    ``by_conversation`` carries combined input+output tokens, which is
    what a bill is drawn on; the run count comes from the Run table so
    runs that recorded no usage (failures) are still counted.
    """
    totals = UsageTotals()
    rows = db.execute(
        select(
            UsageEvent.conversation_id,
            func.sum(UsageEvent.input_tokens),
            func.sum(UsageEvent.output_tokens),
            func.sum(UsageEvent.rounds),
        )
        .where(UsageEvent.user_id == user.id)
        .group_by(UsageEvent.conversation_id)
    ).all()
    for conversation_id, inp, out, rounds in rows:
        inp_i, out_i, rounds_i = int(inp or 0), int(out or 0), int(rounds or 0)
        totals.input_tokens += inp_i
        totals.output_tokens += out_i
        totals.rounds += rounds_i
        totals.by_conversation[conversation_id] = inp_i + out_i
    totals.runs = int(
        db.query(func.count(Run.id))
        .join(Conversation, Run.conversation_id == Conversation.id)
        .filter(Conversation.user_id == user.id)
        .scalar()
        or 0
    )
    return totals
