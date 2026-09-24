"""Shared ownership lookup for controllers: fetch-or-404 with an owner
check.

Several controllers fetch a row by id and 404 unless it belongs to the
caller's parent (a conversation to its user, a run/file to its
conversation).  This centralizes that "missing OR not yours -> NotFound"
branch; ownership failures surface as 404 rather than 403 so an id can't
be probed for existence.
"""

from __future__ import annotations

from typing import TypeVar

from sqlalchemy.orm import Session

from .errors import NotFound

M = TypeVar("M")


def get_owned(
    db: Session,
    model: type[M],
    id_: str,
    *,
    owner_field: str,
    owner_value: object,
    detail: str,
) -> M:
    """Return the row iff it exists and its ``owner_field`` matches
    ``owner_value``; otherwise raise ``NotFound(detail)``."""
    obj = db.get(model, id_)
    if obj is None or getattr(obj, owner_field) != owner_value:
        raise NotFound(detail)
    return obj
