"""Domain errors raised by controllers, mapped to HTTP by the app.

Controllers hold the business rules, so they must be able to reject a
request -- but they should not know that the caller speaks HTTP.  They
raise these instead of ``HTTPException``; ``main.py`` registers one
handler that renders them in FastAPI's own error shape
(``{"detail": ...}``), so the wire contract is unchanged and a
controller stays callable from a test, a CLI, or a worker thread.
"""

from __future__ import annotations


class DomainError(Exception):
    """A request the domain refuses, with the status it maps to."""

    status_code = 500

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class InvalidRequest(DomainError):
    """Malformed input that schema validation cannot express (400)."""

    status_code = 400


class Unauthorized(DomainError):
    """Missing or unusable credentials (401)."""

    status_code = 401


class Forbidden(DomainError):
    """Authenticated but not allowed (403)."""

    status_code = 403


class NotFound(DomainError):
    """No such resource, or not the caller's to see (404).

    Ownership failures deliberately surface as 404 rather than 403: a
    403 would confirm that someone else's id exists.
    """

    status_code = 404


class Conflict(DomainError):
    """The resource's current state forbids the request (409)."""

    status_code = 409


class PayloadTooLarge(DomainError):
    """Upload exceeds the configured limit (413)."""

    status_code = 413
