"""Secret business logic: write-only, encrypted at rest.

Values are Fernet-encrypted here and never handed back -- callers can
learn a secret's name and age, nothing more.  The sandbox manager (not
the agent, not the browser) is the intended consumer of decrypted
values.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..infra import secrets_store
from ..models import Secret, User, new_id
from ..models.db import commit_now
from .errors import InvalidRequest, NotFound


def _find(db: Session, user: User, name: str) -> Secret | None:
    return db.scalar(select(Secret).where(Secret.user_id == user.id, Secret.name == name))


def put(db: Session, user: User, name: str, *, body_name: str, value: str) -> Secret:
    """Create or replace a secret, encrypting the value.

    The body's name must agree with the path: a mismatch would silently
    write a different secret than the URL names.
    """
    if body_name != name:
        raise InvalidRequest("name mismatch with path")
    encrypted = secrets_store.encrypt(value)
    existing = _find(db, user, name)
    if existing is not None:
        existing.value_encrypted = encrypted
        db.flush()
        commit_now(db)
        return existing
    secret = Secret(
        id=new_id("sec"),
        user_id=user.id,
        name=name,
        value_encrypted=encrypted,
    )
    db.add(secret)
    db.flush()
    commit_now(db)
    return secret


def list_for_user(db: Session, user: User) -> list[Secret]:
    return list(db.scalars(select(Secret).where(Secret.user_id == user.id)).all())


def delete(db: Session, user: User, name: str) -> None:
    secret = _find(db, user, name)
    if secret is None:
        raise NotFound("secret not found")
    db.delete(secret)
    commit_now(db)
