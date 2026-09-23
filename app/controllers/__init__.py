"""Controllers: all of the app's business logic.

Two kinds of module live here:

*Machinery* — ``runner`` (sandboxes and harness processes), ``protocol``
(the harness event contract), ``manager`` (run lifecycle, event
fan-out).

*Request logic* — ``accounts``, ``conversations``, ``files``, ``runs``,
``secrets``, ``usage``.  One module per area of the domain, holding the
rules that decide what a caller may do and what the agent is told.

``app.routes`` is deliberately thin on top of these: it wires HTTP
(dependencies, status codes, response schemas) and nothing else.  A
controller refuses work by raising from ``errors``, so it never needs to
know the caller speaks HTTP.
"""

from .errors import (
    Conflict,
    DomainError,
    Forbidden,
    InvalidRequest,
    NotFound,
    PayloadTooLarge,
    Unauthorized,
)
from .manager import Controller
from .protocol import ProtocolEvent, RunOutcome, apply_event, parse_line, parse_stream
from .runner import ExecResult, Runner, ServerRunner, get_runner

__all__ = [
    "Conflict",
    "Controller",
    "DomainError",
    "ExecResult",
    "Forbidden",
    "InvalidRequest",
    "NotFound",
    "PayloadTooLarge",
    "ProtocolEvent",
    "RunOutcome",
    "Runner",
    "ServerRunner",
    "Unauthorized",
    "apply_event",
    "get_runner",
    "parse_line",
    "parse_stream",
]
