"""Protocol module: parsing harness --json lines."""

from __future__ import annotations

import json

from app.controllers.protocol import (
    ASSUMED_PROTOCOL_VERSION,
    KNOWN_NOTIFY_KINDS,
    RunOutcome,
    SeqTracker,
    affects_outcome,
    apply_event,
    coalesce_events,
    live_usage_total,
    notify_kind,
    parse_line,
    parse_ready,
    parse_stream,
    read_capped_line,
    sanitize_event,
)


class TestParseLine:
    def test_start(self) -> None:
        event = parse_line(json.dumps({"type": "start", "seq": 1, "run_id": "r1", "prompt": "hi"}))
        assert event is not None
        assert event.type == "start"
        assert event.seq == 1
        assert event.run_id == "r1"

    def test_result_with_usage(self) -> None:
        payload = {
            "type": "result",
            "seq": 9,
            "answer": "42",
            "errors": [],
            "usage": {"input": 10, "output": 5, "rounds": 2},
            "model": "gpt-test",
            "cancelled": False,
        }
        event = parse_line(json.dumps(payload))
        assert event is not None
        assert event.data["usage"]["input"] == 10

    def test_blank_and_noise(self) -> None:
        assert parse_line("") is None
        assert parse_line("   ") is None
        noise = parse_line("Traceback (most recent call last):")
        assert noise is not None
        assert noise.malformed
        assert noise.type == "log"

    def test_non_dict_json(self) -> None:
        event = parse_line("[1, 2, 3]")
        assert event is not None
        assert event.malformed

    def test_unknown_type(self) -> None:
        event = parse_line(json.dumps({"type": "mystery", "seq": 2}))
        assert event is not None
        assert event.malformed
        assert "mystery" in event.data["message"]

    def test_missing_fields_tolerated(self) -> None:
        event = parse_line(json.dumps({"type": "delta", "text": "abc"}))
        assert event is not None
        assert event.seq is None
        assert event.run_id is None


class TestApplyEvent:
    def test_result_folds(self) -> None:
        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "result",
                    "answer": "done",
                    "errors": ["e1"],
                    "usage": {"input": 1, "output": 2, "rounds": 3},
                    "model": "m",
                    "cancelled": True,
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.answer == "done"
        assert outcome.errors == ["e1"]
        assert outcome.usage == {"input": 1, "output": 2, "rounds": 3}
        assert outcome.model == "m"
        assert outcome.cancelled is True
        assert outcome.saw_result

    def test_error_notify_dedupes(self) -> None:
        outcome = RunOutcome()
        for _ in range(2):
            event = parse_line(json.dumps({"type": "notify", "kind": "error", "data": "boom"}))
            assert event is not None
            apply_event(outcome, event)
        assert outcome.errors == ["boom"]

    def test_non_error_notify_ignored(self) -> None:
        outcome = RunOutcome()
        event = parse_line(json.dumps({"type": "notify", "kind": "tool_start", "data": []}))
        assert event is not None
        apply_event(outcome, event)
        assert outcome.errors == []
        assert not outcome.saw_result

    def test_result_structured_errors_fold_to_messages(self) -> None:
        """The harness ships errors as [{code,message}] with a flat
        error_messages mirror.  The trail must carry the messages, not
        Python dict reprs."""
        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "result",
                    "answer": "",
                    "errors": [{"code": "budget", "message": "Error: out of rounds"}],
                    "error_messages": ["Error: out of rounds"],
                    "cancelled": False,
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.errors == ["Error: out of rounds"]
        assert all("{" not in e for e in outcome.errors)

    def test_result_errors_without_mirror_still_unwrap(self) -> None:
        """If error_messages is absent, each {code,message} entry is
        still unwrapped to its message rather than str(dict)."""
        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "result",
                    "answer": "",
                    "errors": [{"code": "timeout", "message": "Error: too slow"}],
                    "cancelled": False,
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.errors == ["Error: too slow"]

    def test_structured_notify_error_unwraps(self) -> None:
        """An older harness may still send a dict on the notify line;
        it must not surface as a repr."""
        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "notify",
                    "kind": "error",
                    "data": {"code": "budget", "message": "Error: out of rounds"},
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.errors == ["Error: out of rounds"]

    def test_protocol_error_line_prefers_flat_message(self) -> None:
        """The type:error line carries both a structured 'error' object
        and a flat 'message'; the trail uses the message, not the dict."""
        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "error",
                    "error": {"code": "protocol", "message": "stale answer"},
                    "message": "stale answer",
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.protocol_errors == ["stale answer"]
        assert outcome.errors == []  # not an agent failure
        assert outcome.error_trail == ["stale answer"]  # still visible
        assert outcome.failed is False  # and does not decide the verdict

    def test_plain_string_errors_unchanged(self) -> None:
        """Back-compat: the simplified string form still folds as-is."""
        outcome = RunOutcome()
        event = parse_line(
            json.dumps({"type": "result", "answer": "done", "errors": ["e1"], "cancelled": False})
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.errors == ["e1"]

    def test_empty_message_error_still_signals_error(self) -> None:
        """A (degenerate) error whose message resolves empty must not
        be dropped: run_status keys off the list being non-empty, so
        dropping it would silently turn an errored run into 'done'."""
        from app.controllers.manager import run_status

        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "result",
                    "answer": "",
                    "errors": [{}],  # no code, no message -> text is ""
                    "cancelled": False,
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.errors == [""]  # entry preserved, not dropped
        assert run_status(outcome) == "error"  # NOT downgraded to "done"


class TestParseStream:
    def test_multi_line(self) -> None:
        blob = "\n".join(
            [
                json.dumps({"type": "start", "seq": 1}),
                "",
                "noise line",
                json.dumps({"type": "result", "seq": 2, "answer": "ok", "errors": []}),
            ]
        )
        events = parse_stream(blob)
        assert [e.type for e in events] == ["start", "log", "result"]
        assert events[-1].data["answer"] == "ok"


class TestHandshake:
    """``ready`` is the one place a version can be accepted or refused.

    The harness stamps every line with ``protocol``, but a version is a
    single integer a host can only take or leave wholesale -- so an
    unknown one has to fail at the handshake rather than be misparsed
    line by line for the rest of the run.
    """

    def test_parses_version_capabilities_and_pid(self) -> None:
        hs = parse_ready(
            {
                "type": "ready",
                "pid": 4242,
                "protocol_version": 1,
                "capabilities": ["submit", "answer", "ask_id", "usage_notify"],
            }
        )
        assert hs.protocol_version == 1
        assert hs.pid == 4242
        assert hs.supported is True
        assert hs.has("ask_id") and hs.has("usage_notify")
        assert not hs.has("teleportation")
        assert "ask_id" in hs.describe()

    def test_unknown_version_is_unsupported(self) -> None:
        hs = parse_ready({"type": "ready", "protocol_version": 99})
        assert hs.protocol_version == 99
        assert hs.supported is False

    def test_missing_version_falls_back_rather_than_refusing(self) -> None:
        """A harness predating the stamped handshake must still work."""
        hs = parse_ready({"type": "ready", "pid": 1})
        assert hs.protocol_version == ASSUMED_PROTOCOL_VERSION
        assert hs.supported is True
        assert hs.capabilities == frozenset()

    def test_garbled_fields_degrade(self) -> None:
        hs = parse_ready({"type": "ready", "protocol_version": "x", "capabilities": "nope"})
        assert hs.protocol_version == ASSUMED_PROTOCOL_VERSION
        assert hs.capabilities == frozenset()
        assert hs.pid is None

    def test_protocol_field_is_accepted_as_the_version(self) -> None:
        hs = parse_ready({"type": "ready", "protocol": 1})
        assert hs.protocol_version == 1


class TestControlLines:
    """``ready``/``pong`` are control lines, not malformed events."""

    def test_pong_is_not_malformed(self) -> None:
        event = parse_line(json.dumps({"type": "pong", "protocol": 1}))
        assert event is not None
        assert event.type == "pong"
        assert event.malformed is False
        assert event.seq is None

    def test_ready_is_not_malformed(self) -> None:
        event = parse_line(json.dumps({"type": "ready", "pid": 7, "protocol_version": 1}))
        assert event is not None
        assert event.type == "ready"
        assert event.malformed is False

    def test_a_genuinely_unknown_type_is_still_malformed(self) -> None:
        event = parse_line(json.dumps({"type": "sideways"}))
        assert event is not None
        assert event.malformed is True
        assert "unknown event type" in event.data["message"]

    def test_control_lines_do_not_disturb_the_outcome(self) -> None:
        outcome = RunOutcome()
        for payload in ({"type": "pong"}, {"type": "ready", "protocol_version": 1}):
            event = parse_line(json.dumps(payload))
            assert event is not None
            apply_event(outcome, event)
        assert outcome.errors == []
        assert outcome.codes == []
        assert outcome.saw_result is False


class TestErrorCodes:
    """Codes are kept beside the text, not re-derived from it."""

    def test_result_codes_are_collected(self) -> None:
        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "result",
                    "answer": "",
                    "errors": [
                        {"code": "budget", "message": "round budget exhausted"},
                        {"code": "timeout", "message": "wall-clock limit hit"},
                    ],
                    "error_messages": ["round budget exhausted", "wall-clock limit hit"],
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.codes == ["budget", "timeout"]
        assert outcome.errors == ["round budget exhausted", "wall-clock limit hit"]
        assert outcome.primary_code == "budget"

    def test_primary_code_prefers_a_classified_one_over_unknown(self) -> None:
        """An unclassified failure arriving first must not mask the verdict."""
        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "result",
                    "answer": "",
                    "errors": [
                        {"code": "unknown", "message": "something went wrong"},
                        {"code": "budget", "message": "round budget exhausted"},
                    ],
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.codes == ["unknown", "budget"]
        assert outcome.primary_code == "budget"

    def test_no_errors_means_no_code(self) -> None:
        outcome = RunOutcome()
        event = parse_line(json.dumps({"type": "result", "answer": "ok", "errors": []}))
        assert event is not None
        apply_event(outcome, event)
        assert outcome.primary_code == ""

    def test_notify_error_code_is_captured(self) -> None:
        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "notify",
                    "kind": "error",
                    "data": {"code": "timeout", "message": "took too long"},
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.codes == ["timeout"]
        assert outcome.errors == ["took too long"]

    def test_protocol_error_line_code_is_captured(self) -> None:
        outcome = RunOutcome()
        event = parse_line(
            json.dumps(
                {
                    "type": "error",
                    "error": {"code": "protocol", "message": "stale answer"},
                    "message": "stale answer",
                }
            )
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.codes == ["protocol"]
        assert outcome.primary_code == "protocol"

    def test_a_plain_string_error_yields_no_code(self) -> None:
        """Codes come from the structured list only; never guessed."""
        outcome = RunOutcome()
        event = parse_line(
            json.dumps({"type": "result", "answer": "", "errors": ["just a sentence"]})
        )
        assert event is not None
        apply_event(outcome, event)
        assert outcome.errors == ["just a sentence"]
        assert outcome.codes == []
        assert outcome.primary_code == ""


class TestLiveUsage:
    """Per-round usage makes mid-run metering possible."""

    def test_usage_notify_updates_the_live_snapshot(self) -> None:
        outcome = RunOutcome()
        for rounds, out in ((1, 10), (2, 25)):
            event = parse_line(
                json.dumps(
                    {
                        "type": "notify",
                        "kind": "usage",
                        "data": {"input": 100, "output": out, "rounds": rounds},
                    }
                )
            )
            assert event is not None
            apply_event(outcome, event)
        assert outcome.live_usage == {"input": 100, "output": 25, "rounds": 2}
        assert live_usage_total(outcome.live_usage) == 125
        assert outcome.usage is None  # the result line stays canonical

    def test_result_usage_is_unaffected_by_live_snapshots(self) -> None:
        outcome = RunOutcome()
        for payload in (
            {"type": "notify", "kind": "usage", "data": {"input": 1, "output": 1, "rounds": 1}},
            {
                "type": "result",
                "answer": "done",
                "errors": [],
                "usage": {"input": 7, "output": 3, "rounds": 2},
            },
        ):
            event = parse_line(json.dumps(payload))
            assert event is not None
            apply_event(outcome, event)
        assert outcome.usage == {"input": 7, "output": 3, "rounds": 2}

    def test_garbled_usage_is_ignored(self) -> None:
        outcome = RunOutcome()
        event = parse_line(json.dumps({"type": "notify", "kind": "usage", "data": "nope"}))
        assert event is not None
        apply_event(outcome, event)
        assert outcome.live_usage is None

    def test_total_of_nothing_is_zero(self) -> None:
        assert live_usage_total(None) == 0
        assert live_usage_total({}) == 0


class TestSeqTracker:
    """``seq`` exists for drop detection, which needs an actual check."""

    def test_a_clean_sequence_reports_nothing(self) -> None:
        t = SeqTracker()
        assert [t.check(n) for n in (1, 2, 3, 4)] == [None] * 4
        assert t.gaps == 0 and t.reordered == 0

    def test_a_gap_is_reported_and_counted(self) -> None:
        t = SeqTracker()
        t.check(1)
        anomaly = t.check(4)
        assert anomaly is not None
        assert "2 line(s) lost" in anomaly
        assert t.gaps == 2
        # and the stream resyncs rather than reporting every later line
        assert t.check(5) is None

    def test_a_repeat_or_reorder_is_reported(self) -> None:
        t = SeqTracker()
        t.check(1)
        t.check(2)
        assert t.check(2) is not None
        assert t.reordered == 1
        assert t.check(1) is not None
        assert t.reordered == 2

    def test_unnumbered_lines_are_ignored(self) -> None:
        """Control lines are not part of a run's numbered sequence."""
        t = SeqTracker()
        t.check(1)
        assert t.check(None) is None
        assert t.check(2) is None
        assert t.gaps == 0


class TestSanitizeEvent:
    """Untrusted payloads are bounded before crossing to a client."""

    def test_a_small_event_is_untouched(self) -> None:
        payload = {"type": "notify", "kind": "tool", "seq": 3, "data": {"name": "Bash"}}
        assert sanitize_event(payload, 64 * 1024) is payload

    def test_an_oversize_body_is_truncated(self) -> None:
        payload = {"type": "log", "seq": 4, "message": "x" * 10_000}
        out = sanitize_event(payload, 1_000)
        assert out["truncated"] is True
        assert len(out["message"]) < 10_000
        assert out["message"].endswith("[truncated by the web layer]")
        # the envelope a client parses on is preserved
        assert out["type"] == "log" and out["seq"] == 4

    def test_the_envelope_survives_truncation(self) -> None:
        payload = {
            "type": "notify",
            "kind": "tool_calls",
            "seq": 9,
            "run_id": "r1",
            "data": {"blob": "y" * 20_000},
        }
        out = sanitize_event(payload, 500)
        assert out["type"] == "notify"
        assert out["kind"] == "tool_calls"
        assert out["seq"] == 9
        assert out["run_id"] == "r1"
        assert out["truncated"] is True
        # type-preserving: `data` must remain a mapping
        assert isinstance(out["data"], dict)
        assert out["data"]["truncated"] is True

    def test_a_zero_cap_disables_the_check(self) -> None:
        payload = {"type": "log", "message": "z" * 50_000}
        assert sanitize_event(payload, 0) is payload

    def test_unserialisable_payloads_are_still_bounded(self) -> None:
        """A circular payload cannot be measured, so it is replaced
        rather than relayed unbounded."""
        circular: dict = {"loop": None}
        circular["loop"] = circular
        payload = {"type": "log", "message": "m" * 500, "data": circular}
        out = sanitize_event(payload, 100)
        assert out["truncated"] is True
        assert out["type"] == "log"


class TestNotifyKind:
    def test_known_kinds(self) -> None:
        event = parse_line(json.dumps({"type": "notify", "kind": "todos", "data": []}))
        assert event is not None
        assert notify_kind(event) == "todos"
        assert "todos" in KNOWN_NOTIFY_KINDS

    def test_non_notify_has_no_kind(self) -> None:
        event = parse_line(json.dumps({"type": "delta", "text": "hi"}))
        assert event is not None
        assert notify_kind(event) is None


class TestReadCappedLine:
    """``readline`` is unbounded; one huge line must not exhaust the host."""

    def _stream(self, text: str):
        import io

        return io.StringIO(text)

    def test_a_normal_line_passes_through(self) -> None:
        s = self._stream('{"type": "delta"}\n{"type": "result"}\n')
        line, truncated = read_capped_line(s, 1000)
        assert line == '{"type": "delta"}\n'
        assert truncated is False

    def test_an_oversize_line_is_discarded_whole(self) -> None:
        """The tail must be skipped, not left to reparse as new lines --
        otherwise one oversize line becomes a flood of malformed ones."""
        s = self._stream("X" * 5000 + "\n" + '{"type": "result"}\n')
        line, truncated = read_capped_line(s, 100)
        assert truncated is True
        # the NEXT read is the following real line, not the tail
        nxt, nxt_trunc = read_capped_line(s, 100)
        assert nxt == '{"type": "result"}\n'
        assert nxt_trunc is False

    def test_eof_midline_is_not_reported_as_oversize(self) -> None:
        s = self._stream('{"partial"')
        line, truncated = read_capped_line(s, 1000)
        assert line == '{"partial"'
        assert truncated is False

    def test_eof_returns_empty(self) -> None:
        line, truncated = read_capped_line(self._stream(""), 1000)
        assert line == "" and truncated is False

    def test_a_zero_cap_disables_the_limit(self) -> None:
        s = self._stream("Y" * 5000 + "\n")
        line, truncated = read_capped_line(s, 0)
        assert len(line) == 5001 and truncated is False


class TestAffectsOutcome:
    """Which lines may still be dropped once a transcript is capped."""

    def test_outcome_bearing_lines(self) -> None:
        assert affects_outcome({"type": "result"})
        assert affects_outcome({"type": "error"})
        assert affects_outcome({"type": "notify", "kind": "error"})
        assert affects_outcome({"type": "notify", "kind": "usage"})

    def test_cosmetic_lines(self) -> None:
        assert not affects_outcome({"type": "delta"})
        assert not affects_outcome({"type": "start"})
        assert not affects_outcome({"type": "log"})
        assert not affects_outcome({"type": "notify", "kind": "todos"})

    def test_it_matches_what_apply_event_actually_reads(self) -> None:
        """If apply_event grows a branch, this must grow with it."""
        samples = [
            ({"type": "result", "answer": "a", "errors": [{"code": "budget", "message": "m"}]}),
            ({"type": "error", "error": {"code": "protocol", "message": "m"}}),
            ({"type": "notify", "kind": "error", "data": {"code": "timeout", "message": "m"}}),
            ({"type": "notify", "kind": "usage", "data": {"input": 1, "output": 2}}),
            ({"type": "delta", "text": "x"}),
            ({"type": "start", "prompt": "p"}),
            ({"type": "log", "message": "m"}),
        ]
        for payload in samples:
            before = RunOutcome()
            event = parse_line(json.dumps(payload))
            assert event is not None
            apply_event(before, event)
            changed = before != RunOutcome()
            assert changed == affects_outcome(payload), payload


class TestCoalesceEvents:
    """Storage shrinks without changing how a replay renders."""

    def _delta(self, seq: int, text: str) -> dict:
        return {"type": "delta", "seq": seq, "data": {"type": "delta", "text": text}}

    def test_consecutive_deltas_merge_losslessly(self) -> None:
        out = coalesce_events(
            [self._delta(1, "He"), self._delta(2, "llo"), self._delta(3, " there")]
        )
        assert len(out) == 1
        assert out[0]["data"]["text"] == "Hello there"
        assert out[0]["seq"] == 3  # the newest seq survives for resume

    def test_non_deltas_break_a_run_of_deltas(self) -> None:
        out = coalesce_events(
            [
                self._delta(1, "a"),
                {"type": "notify", "seq": 2, "kind": "tool", "data": {}},
                self._delta(3, "b"),
            ]
        )
        assert [e["type"] for e in out] == ["delta", "notify", "delta"]

    def test_the_input_is_not_mutated(self) -> None:
        events = [self._delta(1, "a"), self._delta(2, "b")]
        coalesce_events(events)
        assert events[0]["data"]["text"] == "a"

    def test_a_cap_elides_the_middle_and_says_so(self) -> None:
        events = [
            {"type": "notify", "seq": n, "kind": "tool", "data": {"n": n}} for n in range(1, 51)
        ]
        out = coalesce_events(events, max_events=11)
        assert len(out) <= 11
        assert out[0]["data"]["n"] == 1  # head kept
        assert out[-1]["data"]["n"] == 50  # tail kept
        markers = [e for e in out if e.get("elided")]
        assert len(markers) == 1
        assert "elided" in markers[0]["data"]["message"]

    def test_under_the_cap_nothing_is_elided(self) -> None:
        events = [{"type": "notify", "seq": n, "kind": "tool", "data": {}} for n in range(5)]
        out = coalesce_events(events, max_events=100)
        assert len(out) == 5
        assert not any(e.get("elided") for e in out)

    def test_truncated_deltas_are_not_merged(self) -> None:
        """A size-capped delta is already incomplete; concatenating it
        onto a neighbour would silently paper over the gap."""
        a = self._delta(1, "a")
        b = self._delta(2, "b")
        b["truncated"] = True
        out = coalesce_events([a, b])
        assert len(out) == 2
