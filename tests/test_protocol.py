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
