"""Secrets routes: write-only storage of user secrets.

Values are Fernet-encrypted at rest and never returned by the API —
only names and timestamps are listable.  The sandbox manager (not the
agent, not the browser) is the intended consumer of decrypted values.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..core import secrets_store
from ..db import commit_now, get_db
from ..models import Secret, User, new_id
from ..routes.auth import authenticate_user
from ..schemas import SecretIn, SecretOut

router = APIRouter(prefix="/secrets", tags=["secrets"])


@router.put("/{name}", response_model=SecretOut, status_code=status.HTTP_201_CREATED)
def put_secret(
    name: str,
    body: SecretIn,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> SecretOut:
    if body.name != name:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "name mismatch with path")
    existing = db.scalar(select(Secret).where(Secret.user_id == user.id, Secret.name == name))
    if existing is not None:
        existing.value_encrypted = secrets_store.encrypt(body.value)
        db.flush()
        commit_now(db)
        return SecretOut(name=name, created_at=existing.created_at)
    secret = Secret(
        id=new_id("sec"),
        user_id=user.id,
        name=name,
        value_encrypted=secrets_store.encrypt(body.value),
    )
    db.add(secret)
    db.flush()
    commit_now(db)
    return SecretOut(name=name, created_at=secret.created_at)


@router.get("", response_model=list[SecretOut])
def list_secrets(
    user: User = Depends(authenticate_user), db: Session = Depends(get_db)
) -> list[SecretOut]:
    rows = db.scalars(select(Secret).where(Secret.user_id == user.id)).all()
    return [SecretOut(name=s.name, created_at=s.created_at) for s in rows]


@router.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
def delete_secret(
    name: str,
    user: User = Depends(authenticate_user),
    db: Session = Depends(get_db),
) -> None:
    secret = db.scalar(select(Secret).where(Secret.user_id == user.id, Secret.name == name))
    if secret is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "secret not found")
    db.delete(secret)
    commit_now(db)
