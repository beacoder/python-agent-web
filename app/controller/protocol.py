"""Protocol types and parsing for the harness ``headless --json`` stream.

The harness emits one JSON object per line on stdout
(``start``/``delta``/``notify``/``log``/``result``, each carrying
``seq`` and ``run_id``; ``result`` carries ``answer``, ``errors``,
``usage``, ``model``, ``cancelled``).  This module turns raw output
into typed events — the *only* coupling point with the harness repo,
and purely a data contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

LINE_TYPES = {"start", "delta", "notify", "log", "result"}


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


def apply_event(outcome: RunOutcome, event: ProtocolEvent) -> None:
    """Fold an event into the run outcome (result is canonical)."""
    if event.type == "result":
        outcome.saw_result = True
        outcome.answer = str(event.data.get("answer", ""))
        errors = event.data.get("errors")
        if isinstance(errors, list):
            outcome.errors = [str(e) for e in errors]
        usage = event.data.get("usage")
        outcome.usage = dict(usage) if isinstance(usage, dict) else None
        outcome.model = event.data.get("model")
        outcome.cancelled = bool(event.data.get("cancelled", False))
    elif event.type == "notify" and event.data.get("kind") == "error":
        message = event.data.get("data")
        if message is not None and str(message) not in outcome.errors:
            outcome.errors.append(str(message))


def parse_stream(text: str) -> list[ProtocolEvent]:
    """Parse a whole captured stdout blob (used by tests and retries)."""
    events = []
    for line in text.splitlines():
        event = parse_line(line)
        if event is not None:
            events.append(event)
    return events
