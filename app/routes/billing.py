"""Billing routes: usage ledger summaries (tokens per user).

HTTP wiring only; the aggregation lives in ``controllers.usage``.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..controllers import usage
from ..models import User
from ..models.db import get_db
from ..routes.auth import authenticate_user
from ..validation.schemas import UsageSummary

router = APIRouter(prefix="/usage", tags=["billing"])


@router.get("/summary", response_model=UsageSummary)
def usage_summary(
    user: User = Depends(authenticate_user), db: Session = Depends(get_db)
) -> UsageSummary:
    totals = usage.summary(db, user)
    return UsageSummary(
        input_tokens=totals.input_tokens,
        output_tokens=totals.output_tokens,
        rounds=totals.rounds,
        runs=totals.runs,
        by_conversation=totals.by_conversation,
    )
