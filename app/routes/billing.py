"""Billing routes: usage ledger summaries (tokens per user)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import Conversation, Run, UsageEvent, User
from ..models.db import get_db
from ..routes.auth import authenticate_user
from ..validation.schemas import UsageSummary

router = APIRouter(prefix="/usage", tags=["billing"])


@router.get("/summary", response_model=UsageSummary)
def usage_summary(
    user: User = Depends(authenticate_user), db: Session = Depends(get_db)
) -> UsageSummary:
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
    by_conversation: dict[str, int] = {}
    input_total = output_total = rounds_total = 0
    for conversation_id, inp, out, rounds in rows:
        inp_i, out_i, rounds_i = int(inp or 0), int(out or 0), int(rounds or 0)
        input_total += inp_i
        output_total += out_i
        rounds_total += rounds_i
        by_conversation[conversation_id] = inp_i + out_i
    run_count = (
        db.query(func.count(Run.id))
        .join(Conversation, Run.conversation_id == Conversation.id)
        .filter(Conversation.user_id == user.id)
        .scalar()
        or 0
    )
    return UsageSummary(
        input_tokens=input_total,
        output_tokens=output_total,
        rounds=rounds_total,
        runs=int(run_count),
        by_conversation=by_conversation,
    )
