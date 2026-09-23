"""Secrets routes: write-only storage of user secrets.

HTTP wiring only.  Values are encrypted at rest and never returned by
the API -- only names and timestamps are listable; see
``controllers.secrets``.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from ..controllers import secrets as secrets_controller
from ..models import Secret, User
from ..models.db import get_db
from ..routes.auth import authenticate_user
from ..validation.schemas import SecretIn, SecretOut

router = APIRouter(prefix="/secrets", tags=["secrets"])


def _secret_out(secret: Secret) -> SecretOut:
    return SecretOut(name=secret.name, created_at=secret.created_at)


@router.put("/{name}", response_model=SecretOut, status_code=status.HTTP_201_CREATED)
def put_secret(
    name: str,
    body: SecretIn,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> SecretOut:
    secret = secrets_controller.put(db, user, name, body_name=body.name, value=body.value)
    return _secret_out(secret)


@router.get("", response_model=list[SecretOut])
def list_secrets(
    user: User = Depends(authenticate_user), db: Session = Depends(get_db)
) -> list[SecretOut]:
    return [_secret_out(s) for s in secrets_controller.list_for_user(db, user)]


@router.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
def delete_secret(
    name: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> None:
    secrets_controller.delete(db, user, name)
