"""Usage business logic: aggregate the token ledger for a user."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..infra.config import get_settings
from ..models import Conversation, Run, UsageEvent, User
from .errors import PaymentRequired


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


def consumed_tokens(db: Session, user: User) -> int:
    """Total tokens (input + output) the user has spent, from the ledger."""
    row = db.execute(
        select(
            func.coalesce(func.sum(UsageEvent.input_tokens), 0)
            + func.coalesce(func.sum(UsageEvent.output_tokens), 0)
        ).where(UsageEvent.user_id == user.id)
    ).scalar()
    return int(row or 0)


def token_budget(user: User) -> int:
    """The user's total token allowance.

    Free-tier allowance plus the token value of their purchased points
    (both buckets).  A points top-up therefore raises the ceiling.
    """
    s = get_settings()
    points = (user.plan_points or 0) + (user.pack_points or 0)
    return s.budget_free_tokens + points * s.budget_tokens_per_point


@dataclass(frozen=True)
class BudgetStatus:
    consumed: int
    budget: int

    @property
    def remaining(self) -> int:
        return max(0, self.budget - self.consumed)

    @property
    def exhausted(self) -> bool:
        return self.consumed >= self.budget


def budget_status(db: Session, user: User) -> BudgetStatus:
    return BudgetStatus(consumed=consumed_tokens(db, user), budget=token_budget(user))


def enforce_budget(db: Session, user: User) -> None:
    """Kill-switch: refuse a new run when the user is out of budget.

    A no-op unless ``budget_enforce`` is on, so dev/internal deployments
    are unaffected.  Enforced *before* a run starts against a snapshot of
    the ledger.

    Bound, stated honestly: the ledger only records after a run finishes,
    and the per-conversation running-run guard admits one concurrent run
    *per conversation*.  So a user with many conversations can start one
    run per conversation against the same pre-spend snapshot -- overspend
    is bounded by the number of the user's conversations, not by one run
    globally.  Tightening that to a hard global cap would require
    reserving tokens up front (a debit-on-start ledger), which is a
    deliberate future step, not implemented here.
    """
    if not get_settings().budget_enforce:
        return
    status = budget_status(db, user)
    if status.exhausted:
        raise PaymentRequired(
            f"token budget exhausted ({status.consumed}/{status.budget}); "
            "add credits to continue"
        )
