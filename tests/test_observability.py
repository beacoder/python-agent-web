"""Observability: request-id propagation, error capture, log config."""

from __future__ import annotations

import logging

from fastapi.testclient import TestClient


class TestRequestId:
    def test_generated_when_absent(self, client: TestClient) -> None:
        res = client.get("/healthz")
        assert res.status_code == 200
        rid = res.headers.get("X-Request-ID")
        assert rid and len(rid) >= 8

    def test_echoed_when_provided(self, client: TestClient) -> None:
        res = client.get("/healthz", headers={"X-Request-ID": "trace-abc-123"})
        assert res.headers.get("X-Request-ID") == "trace-abc-123"

    def test_present_on_error_responses(self, client: TestClient) -> None:
        # a 401 still flows through the middleware and gets the header
        res = client.get("/auth/me")
        assert res.status_code == 401
        assert res.headers.get("X-Request-ID")


class TestErrorCapture:
    def test_unhandled_error_becomes_logged_500_with_request_id(
        self, client: TestClient, auth_headers: dict, monkeypatch, caplog
    ) -> None:
        """An unexpected exception in a controller is caught by the
        middleware: logged with a traceback, returned as 500 carrying the
        request id the client can quote."""
        from app.controllers import accounts

        def boom(*_a, **_k):
            raise RuntimeError("kaboom")

        monkeypatch.setattr(accounts, "profile", boom)
        # the TestClient must not re-raise the server exception itself
        client.raise_server_exceptions = False
        with caplog.at_level(logging.ERROR, logger="paw.request"):
            res = client.get(
                "/account/profile",
                headers={**auth_headers, "X-Request-ID": "err-1"},
            )
        assert res.status_code == 500
        body = res.json()
        assert body["detail"] == "internal server error"
        assert body["request_id"] == "err-1"
        assert res.headers.get("X-Request-ID") == "err-1"
        # the failure was logged with a traceback, not swallowed
        assert any("unhandled error" in r.message for r in caplog.records)
        assert any(r.exc_info for r in caplog.records)


class TestLogConfig:
    def test_request_id_filter_binds_contextvar(self) -> None:
        from app.infra.logging import _RequestIdFilter, set_request_id

        rec = logging.LogRecord("paw.x", logging.INFO, __file__, 1, "m", None, None)
        set_request_id("bound-42")
        assert _RequestIdFilter().filter(rec) is True
        assert rec.request_id == "bound-42"

    def test_json_formatter_emits_one_object(self) -> None:
        import json

        from app.infra.logging import _JsonFormatter

        rec = logging.LogRecord("paw.x", logging.INFO, __file__, 1, "hello", None, None)
        rec.request_id = "r1"
        out = json.loads(_JsonFormatter().format(rec))
        assert out["level"] == "INFO"
        assert out["request_id"] == "r1"
        assert out["message"] == "hello"
