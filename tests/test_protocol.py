"""Protocol module: parsing harness --json lines."""

from __future__ import annotations

import json

from app.controllers.protocol import RunOutcome, apply_event, parse_line, parse_stream


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
        assert outcome.errors == ["stale answer"]

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
