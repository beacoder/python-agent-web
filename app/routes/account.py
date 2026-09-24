"""Account route: the profile read model for the UI.

HTTP wiring only; the payload is assembled in ``controllers.accounts``.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..controllers import accounts
from ..infra.db import get_db
from ..models import User
from ..routes.auth import authenticate_user
from ..validation.schemas import AccountProfile

router = APIRouter(prefix="/account", tags=["account"])


@router.get("/profile", response_model=AccountProfile)
def get_profile(
    user: User = Depends(authenticate_user), db: Session = Depends(get_db)
) -> AccountProfile:
    return AccountProfile(**accounts.profile(db, user))
