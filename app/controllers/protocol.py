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

# Control lines the resident ``serve`` protocol emits outside a run.
# They are NOT run events, but they must be recognised: classifying a
# ``pong`` (or a stray ``ready`` from a respawn) as malformed would fan
# a bogus "unknown event type" line out to the browser.
CONTROL_LINE_TYPES = {"ready", "pong", "hello"}

# Wire schema versions this driver can parse.  The harness stamps
# ``protocol`` on every line and ``protocol_version`` on ``ready``; it
# is a single integer a host can only accept or reject wholesale, so
# an unknown one must fail loudly at the handshake rather than be
# silently misparsed event by event.  Widen this set deliberately,
# after checking the line shapes a new version changes.
SUPPORTED_PROTOCOL_VERSIONS = frozenset({1})

# Protocol version assumed when ``ready`` omits the field (a harness
# predating the stamped handshake).  Version 1 is the shape this
# module was written against.
ASSUMED_PROTOCOL_VERSION = 1


# Capability names this driver knows how to use.  Features are not
# gated on them in general -- everything the protocol adds degrades
# gracefully, so behaviour follows what arrives on the wire -- but
# these two are things the HOST initiates, so they must only be sent
# to a build that understands them.
CAP_HELLO = "hello"
CAP_OP_ID = "op_id"


@dataclass(frozen=True)
class Handshake:
    """The resident process's ``ready`` line, parsed.

    ``capabilities`` are the named features the build supports.  The
    version alone can only be accepted or rejected; the capability list
    is what lets a host discover what is available instead of inferring
    a feature's absence from events that never arrive.
    """

    protocol_version: int
    capabilities: frozenset[str] = frozenset()
    pid: int | None = None

    @property
    def supported(self) -> bool:
        return self.protocol_version in SUPPORTED_PROTOCOL_VERSIONS

    def has(self, capability: str) -> bool:
        return capability in self.capabilities

    def describe(self) -> str:
        caps = ",".join(sorted(self.capabilities)) or "(none advertised)"
        return f"protocol={self.protocol_version} capabilities={caps}"


def parse_ready(payload: dict[str, Any]) -> Handshake:
    """Build a ``Handshake`` from a decoded ``ready`` payload.

    Tolerant of a missing/garbled version or capability list: a harness
    that predates the stamped handshake still has to be usable, so the
    version falls back to ``ASSUMED_PROTOCOL_VERSION`` rather than
    being treated as unsupported.
    """
    raw_version = payload.get("protocol_version", payload.get("protocol"))
    try:
        version = int(raw_version)
    except (TypeError, ValueError):
        version = ASSUMED_PROTOCOL_VERSION
    raw_caps = payload.get("capabilities")
    caps = frozenset(str(c) for c in raw_caps) if isinstance(raw_caps, list) else frozenset()
    raw_pid = payload.get("pid")
    pid = raw_pid if isinstance(raw_pid, int) else None
    return Handshake(protocol_version=version, capabilities=caps, pid=pid)


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
    codes: list[str] = field(default_factory=list)
    """Structured error codes from the harness, in arrival order.

    Parallel to ``errors`` but kept separate so the human text and the
    machine-branchable code never have to be re-derived from each
    other.  The harness classifies what it can (``budget``,
    ``timeout``, ``protocol``, ...); matching words in a sentence to
    decide whether a failure is retryable or billable is exactly what
    these exist to replace."""
    usage: dict[str, Any] | None = None
    live_usage: dict[str, Any] | None = None
    """Latest per-round usage snapshot seen mid-run (notify ``usage``).

    The ``result`` line stays canonical for billing; this is what lets
    a host meter a run while it is still running, instead of only
    learning the cost once it has finished."""
    model: str | None = None
    cancelled: bool = False
    saw_result: bool = False
    exit_code: int | None = None
    duration_ms: int = 0
    protocol_errors: list[str] = field(default_factory=list)
    """Protocol-level refusals from the harness (``error`` lines).

    Kept apart from ``errors`` because they are not agent failures: a
    rejected ``answer`` (a stale ``ask_id``, nothing pending) is a
    caller/timing problem and the run carries on.  Folding them in with
    agent errors made ANY such refusal mark an otherwise successful run
    as failed, since the terminal state keys off ``errors`` being
    non-empty.  They stay visible in the trail; they just do not decide
    the verdict on their own."""
    seq_gaps: int = 0
    """Count of missing/out-of-order ``seq`` values observed.

    ``seq`` exists so a driver can detect drops; counting them is what
    makes a silently truncated stream visible instead of looking like a
    short run."""

    @property
    def primary_code(self) -> str:
        """The code a caller should branch on, or "" when there is none.

        Prefers a classified code over ``unknown``: the harness folds
        unrecognised failures into ``unknown``, and one of those
        arriving first must not mask a ``budget``/``timeout`` verdict
        recorded alongside it.
        """
        for code in self.codes:
            if code and code != "unknown":
                return code
        return self.codes[0] if self.codes else ""

    @property
    def failed(self) -> bool:
        """Whether the run itself failed.

        Agent/result errors decide this; protocol refusals do not (see
        ``protocol_errors``).
        """
        return bool(self.errors)

    @property
    def error_trail(self) -> list[str]:
        """Everything worth showing a human, agent errors first."""
        return [*self.errors, *self.protocol_errors]


class SeqTracker:
    """Per-run monotonic ``seq`` checker.

    The harness numbers each run's lines from 1 and documents ``seq`` as
    the means to "detect drops/reorder across reconnects" -- but a
    number nobody compares proves nothing.  This reports the first
    anomaly for each kind so a lossy stream is logged once rather than
    once per line.

    Lines with no ``seq`` (control lines like ``ready``/``pong``) are
    ignored: they are not part of a run's numbered sequence.
    """

    __slots__ = ("last", "gaps", "reordered")

    def __init__(self) -> None:
        self.last = 0
        self.gaps = 0
        self.reordered = 0

    def check(self, seq: int | None) -> str | None:
        """Record *seq*; return a description when it is anomalous."""
        if seq is None:
            return None
        expected = self.last + 1
        if seq == expected:
            self.last = seq
            return None
        if seq <= self.last:
            self.reordered += 1
            return f"seq {seq} repeats or precedes {self.last}"
        missing = seq - expected
        self.gaps += missing
        self.last = seq
        return f"seq jumped {expected} -> {seq} ({missing} line(s) lost)"


def _error_code(value: Any) -> str:
    """The structured code of one error entry, or "" when untyped."""
    if isinstance(value, dict):
        inner = value.get("error")
        if isinstance(inner, dict):
            value = inner
        code = value.get("code")
        return str(code) if code else ""
    return ""


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
    if etype in CONTROL_LINE_TYPES:
        # ready/pong are control lines, not run events.  They carry no
        # seq/run_id and must not be surfaced as malformed: a `pong`
        # from a liveness probe, or a `ready` line from a respawn, used
        # to reach the browser as "unknown event type".
        return ProtocolEvent(type=str(etype), seq=None, run_id=None, data=payload)
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
        # Codes come from the structured list only (error_messages is
        # the flat mirror and carries none).  Collected separately from
        # the text so a caller can branch without re-deriving meaning
        # from prose.
        raw_errors = event.data.get("errors")
        if isinstance(raw_errors, list):
            for entry in raw_errors:
                code = _error_code(entry)
                if code and code not in outcome.codes:
                    outcome.codes.append(code)
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
            code = _error_code(data)
            if code and code not in outcome.codes:
                outcome.codes.append(code)
    elif event.type == "notify" and event.data.get("kind") == "usage":
        # Per-round running totals (sub-agent tokens included).  The
        # result line stays canonical for billing; this is what makes
        # mid-run metering possible, so a run that outruns its budget
        # can be cancelled instead of discovered after the fact.
        data = event.data.get("data")
        if isinstance(data, dict):
            outcome.live_usage = {
                key: int(data.get(key) or 0)
                for key in ("input", "output", "rounds")
                if isinstance(data.get(key), (int, float)) and not isinstance(data.get(key), bool)
            }
    elif event.type == "error":
        # Harness protocol-level refusal (unknown op, stale answer, a
        # cancel racing the run's finish).  Recorded in its OWN list:
        # these are not agent failures, and treating them as such made
        # one mis-timed answer mark a run that finished perfectly well
        # as "error", since the terminal state keys off `errors` being
        # non-empty.  Prefer the flat "message" sibling over the
        # structured object so the trail never carries a dict repr.
        raw = event.data.get("message")
        if raw is None:
            raw = event.data.get("error")
        if raw is not None:
            text = _error_text(raw)
            op_id = event.data.get("op_id")
            if text and op_id:
                # The harness echoes the id we put on the op, which is
                # the only way to tell WHICH op was refused: there is no
                # ack, so a pipelined answer and cancel otherwise come
                # back as two indistinguishable error lines.
                text = f"{text} (op {op_id})"
            if text and text not in outcome.protocol_errors:
                outcome.protocol_errors.append(text)
        code = _error_code(event.data.get("error")) or _error_code(event.data)
        if code and code not in outcome.codes:
            outcome.codes.append(code)


def live_usage_total(usage: dict[str, Any] | None) -> int:
    """Billable tokens in a usage snapshot (input + output)."""
    if not usage:
        return 0
    return int(usage.get("input", 0) or 0) + int(usage.get("output", 0) or 0)


def parse_stream(text: str) -> list[ProtocolEvent]:
    """Parse a whole captured stdout blob (used by tests and retries)."""
    events = []
    for line in text.splitlines():
        event = parse_line(line)
        if event is not None:
            events.append(event)
    return events


# notify kinds this host knows how to render.  An unknown kind is still
# relayed (the harness is free to add them, and the UI degrades to
# showing it raw) but it is marked so a client can decide not to.
KNOWN_NOTIFY_KINDS = frozenset(
    {
        "tool_start",
        "tool_calls",
        "tool_running",
        "tool",
        "todos",
        "compact",
        "retry",
        "error",
        "ask",
        "usage",
        "run_done",
    }
)

_TRUNCATION_NOTE = "…[truncated by the web layer]"


def sanitize_event(payload: dict[str, Any], max_bytes: int) -> dict[str, Any]:
    """Bound one relayed event's size before it crosses to a client.

    Everything in a ``notify``/``log`` payload is produced by untrusted
    agent code on the far side of the sandbox boundary and is forwarded
    verbatim into an SSE frame, the stored transcript, and the DOM.  An
    event large enough to matter is therefore a memory and bandwidth
    problem for the trusted side and the browser, so oversize bodies
    are replaced rather than relayed.

    Types are preserved, not just lengths: ``data`` stays a mapping
    (the API schema and the UI both require that, so collapsing it to a
    string would turn a huge event into a validation error) and string
    fields stay strings.  The envelope -- ``type``, ``seq``,
    ``run_id``, ``kind`` -- is never touched, since a client needs it
    to stay parseable.  ``max_bytes`` <= 0 disables the cap.
    """
    if max_bytes <= 0:
        return payload
    try:
        if len(json.dumps(payload, ensure_ascii=False, default=str)) <= max_bytes:
            return payload
    except (TypeError, ValueError):
        pass  # unmeasurable (circular, say): replace the body regardless
    keep = max(0, max_bytes // 2)
    trimmed = dict(payload)
    for key in ("data", "message", "text", "answer", "prompt"):
        if key not in trimmed:
            continue
        value = trimmed[key]
        if isinstance(value, str):
            trimmed[key] = value[:keep] + _TRUNCATION_NOTE
        elif isinstance(value, dict):
            # keep it a mapping; a client validating `data` as an object
            # must not be handed a string because the agent was chatty
            trimmed[key] = {"truncated": True, "preview": _render(value)[:keep]}
        elif value is not None:
            trimmed[key] = _render(value)[:keep] + _TRUNCATION_NOTE
    trimmed["truncated"] = True
    return trimmed


def _render(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(value)


def notify_kind(event: ProtocolEvent) -> str | None:
    """The ``kind`` of a notify event, or None for other line types."""
    if event.type != "notify":
        return None
    kind = event.data.get("kind")
    return str(kind) if kind is not None else None


def affects_outcome(payload: dict[str, Any]) -> bool:
    """Whether a line can still change the run's verdict or billing.

    ``apply_event`` only reads the terminal ``result``, protocol
    ``error`` lines, and error/usage ``notify`` kinds.  Deltas, logs
    and ``start`` contribute nothing to the outcome, which is what
    makes it safe to stop retaining them once a transcript has grown
    past its cap: the answer, the error trail and the token counts
    survive regardless of how chatty the run was.
    """
    etype = payload.get("type")
    if etype in ("result", "error"):
        return True
    if etype == "notify":
        return payload.get("kind") in ("error", "usage")
    return False


def read_capped_line(stream: Any, max_chars: int) -> tuple[str, bool]:
    """Read one line, refusing to buffer more than *max_chars*.

    Returns ``(line, truncated)``.  A plain ``readline()`` is unbounded,
    so a single pathological line from untrusted code can exhaust the
    host's memory before anything gets to look at it.

    When a line exceeds the cap the remainder is drained up to its
    newline and thrown away: leaving it in the buffer would make the
    tail reparse as a sequence of fresh (invalid) lines, turning one
    oversize line into a flood of malformed events.  ``max_chars`` <= 0
    disables the cap.
    """
    if max_chars <= 0:
        return stream.readline(), False
    chunk = stream.readline(max_chars)
    if not chunk:
        return "", False
    if chunk.endswith("\n"):
        return chunk, False
    if len(chunk) < max_chars:
        return chunk, False  # EOF mid-line, not an oversize line
    while True:  # oversize: discard through the newline
        more = stream.readline(max_chars)
        if not more or more.endswith("\n"):
            break
    return chunk, True


_ELISION = "events elided by the web layer"


def _shallow_copy_event(event: dict[str, Any]) -> dict[str, Any]:
    """Copy an event far enough that merging cannot touch the original.

    ``data`` is copied too, not just the envelope: the events handed in
    here are the same dicts held in the live fan-out buffer and already
    delivered to subscribers, so concatenating text into one in place
    would retroactively rewrite what a client was shown.
    """
    copy = dict(event)
    data = copy.get("data")
    if isinstance(data, dict):
        copy["data"] = dict(data)
    return copy


def coalesce_events(events: list[dict[str, Any]], max_events: int = 0) -> list[dict[str, Any]]:
    """Shrink a run's transcript for storage without changing how it renders.

    Consecutive ``delta`` events are merged into one, keeping the last
    ``seq``.  Deltas are token-sized and a transcript held one row per
    token, which is what made the stored JSON grow without bound;
    concatenating them is lossless, since any client renders deltas by
    concatenation anyway.

    If the result still exceeds *max_events*, the middle is dropped --
    head and tail are what a reader needs (the prompt and the verdict)
    -- and replaced by a single marker so the replay does not pretend
    to be complete.
    """
    merged: list[dict[str, Any]] = []
    for event in events:
        if (
            event.get("type") == "delta"
            and merged
            and merged[-1].get("type") == "delta"
            and not event.get("truncated")
            and not merged[-1].get("truncated")
        ):
            prev = merged[-1]
            prev_data = prev.get("data")
            data = event.get("data")
            if isinstance(prev_data, dict) and isinstance(data, dict):
                prev_data["text"] = str(prev_data.get("text", "")) + str(data.get("text", ""))
                if event.get("seq") is not None:
                    prev["seq"] = event["seq"]
                continue
        merged.append(_shallow_copy_event(event))
    if max_events <= 0 or len(merged) <= max_events:
        return merged
    keep = max(1, (max_events - 1) // 2)
    dropped = len(merged) - 2 * keep
    marker = {
        "type": "log",
        "data": {"message": f"[{dropped} {_ELISION}]"},
        "elided": dropped,
    }
    return [*merged[:keep], marker, *merged[-keep:]]
