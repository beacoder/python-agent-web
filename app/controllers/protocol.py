"""Protocol types and parsing for the harness event stream.

The harness emits one JSON object per line on stdout (``start``/
``delta``/``notify``/``log``/``result``, each carrying ``seq`` and
``run_id``; ``result`` carries ``answer``, ``errors``, ``usage``,
``model``, ``cancelled``) — the same line shapes on both the one-shot
``headless --json`` pipe and the resident ``serve`` protocol.  This
module turns raw lines into typed events — the *only* coupling point
with the harness repo, and purely a data contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

LINE_TYPES = {"start", "delta", "notify", "log", "result", "error"}


@dataclass
class ProtocolEvent:
    """One decoded harness line, verbatim payload plus metadata."""

    type: str
    seq: int | None
    run_id: str | None
    data: dict[str, Any]
    malformed: bool = False
    raw: str = ""


@dataclass
class RunOutcome:
    """Aggregated result of one harness exec."""

    answer: str = ""
    errors: list[str] = field(default_factory=list)
    usage: dict[str, Any] | None = None
    model: str | None = None
    cancelled: bool = False
    saw_result: bool = False
    exit_code: int | None = None
    duration_ms: int = 0


def parse_line(line: str) -> ProtocolEvent | None:
    """Parse one stdout line; None for blank/non-JSON noise.

    Non-JSON output is preserved as a ``log``-shaped malformed event so
    a driver can surface harness crashes instead of silently dropping
    them.
    """
    text = line.strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return ProtocolEvent(
            type="log", seq=None, run_id=None, data={"message": text}, malformed=True
        )
    if not isinstance(payload, dict):
        return ProtocolEvent(
            type="log", seq=None, run_id=None, data={"message": text}, malformed=True
        )
    etype = payload.get("type")
    if etype not in LINE_TYPES:
        return ProtocolEvent(
            type="log",
            seq=None,
            run_id=None,
            data={"message": f"unknown event type: {etype!r}"},
            malformed=True,
        )
    return ProtocolEvent(
        type=str(etype),
        seq=payload.get("seq"),
        run_id=payload.get("run_id"),
        data=payload,
    )


def _error_text(value: Any) -> str:
    """Human-readable text of one error entry.

    The harness carries errors as ``{"code", "message"}`` objects (on
    the ``result`` line and the protocol ``error`` line); a driver must
    render the message, never ``str(dict)`` which leaks a Python repr
    into the run's error trail.  A nested ``{"error": {...}}`` is
    unwrapped; a message-less object falls back to its code; a plain
    string passes through.  Mirrors the harness's own
    ``error_display_text`` so both sides agree on the text.
    """
    if isinstance(value, dict):
        inner = value.get("error")
        if isinstance(inner, dict):
            value = inner
        return str(value.get("message") or value.get("code") or "")
    return str(value)


def apply_event(outcome: RunOutcome, event: ProtocolEvent) -> None:
    """Fold an event into the run outcome (result is canonical)."""
    if event.type == "result":
        outcome.saw_result = True
        outcome.answer = str(event.data.get("answer", ""))
        # Prefer the flat "error_messages" mirror the harness emits
        # alongside the structured "errors" precisely for drivers that
        # do not parse the objects; fall back to extracting each
        # entry's message.  Never str() a {"code","message"} dict —
        # that leaks a Python repr into the run's error trail (and the
        # DB).  Merge, preserving order and not wiping errors folded
        # from earlier lines (e.g. a rejected mid-run answer).
        messages = event.data.get("error_messages")
        if not isinstance(messages, list):
            errors = event.data.get("errors")
            messages = [_error_text(e) for e in errors] if isinstance(errors, list) else []
        # Do NOT drop empty entries: run_status keys off the list being
        # non-empty, so filtering could turn a (degenerate) empty-message
        # error into a "done" run — hiding a failure.  Map to text (never
        # str(dict)) but preserve one trail entry per source error.
        for m in (str(x) for x in messages):
            if m not in outcome.errors:
                outcome.errors.append(m)
        usage = event.data.get("usage")
        outcome.usage = dict(usage) if isinstance(usage, dict) else None
        outcome.model = event.data.get("model")
        outcome.cancelled = bool(event.data.get("cancelled", False))
    elif event.type == "notify" and event.data.get("kind") == "error":
        data = event.data.get("data")
        if data is not None:
            text = _error_text(data)
            if text and text not in outcome.errors:
                outcome.errors.append(text)
    elif event.type == "error":
        # harness protocol-level failure (unknown op, stale answer, a
        # cancel racing the run's finish): keep it in the run's error
        # trail so it surfaces instead of vanishing.  Prefer the flat
        # "message" sibling over the structured "error" object so the
        # trail never carries a dict repr.
        raw = event.data.get("message")
        if raw is None:
            raw = event.data.get("error")
        if raw is not None:
            text = _error_text(raw)
            if text and text not in outcome.errors:
                outcome.errors.append(text)


def parse_stream(text: str) -> list[ProtocolEvent]:
    """Parse a whole captured stdout blob (used by tests and retries)."""
    events = []
    for line in text.splitlines():
        event = parse_line(line)
        if event is not None:
            events.append(event)
    return events
