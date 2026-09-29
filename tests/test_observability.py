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


class TestMetrics:
    def test_metrics_endpoint_exposes_prometheus_text(self, client: TestClient) -> None:
        # generate some traffic first
        client.get("/healthz")
        res = client.get("/metrics")
        assert res.status_code == 200
        assert "text/plain" in res.headers["content-type"]
        body = res.text
        assert "paw_http_requests_total" in body
        assert "# TYPE paw_http_requests_total counter" in body

    def test_http_requests_counted_by_status(self, client: TestClient) -> None:
        from app.infra.metrics import get_registry

        get_registry().reset()
        client.get("/healthz")  # 200
        client.get("/auth/me")  # 401
        body = client.get("/metrics").text
        assert 'paw_http_requests_total{method="GET",status="200"}' in body
        # the 401 from /auth/me is counted too
        assert 'status="401"' in body

    def test_counter_and_histogram_render(self) -> None:
        from app.infra.metrics import MetricsRegistry

        r = MetricsRegistry()
        r.counter("things_total", 3, labels={"kind": "a"})
        r.observe("lat_seconds", 0.2)
        out = r.render()
        assert 'things_total{kind="a"} 3.0' in out
        assert "lat_seconds_bucket" in out
        assert "lat_seconds_count" in out
        assert "lat_seconds_sum" in out

    def test_method_label_is_bounded(self) -> None:
        from app.infra.metrics import normalize_method

        assert normalize_method("get") == "GET"
        assert normalize_method("POST") == "POST"
        # an arbitrary/garbage verb collapses to a single bucket so it
        # cannot create unbounded metric series
        assert normalize_method("BREW") == "other"
        assert normalize_method("../../etc") == "other"

    def test_garbage_method_does_not_grow_series(self, client: TestClient) -> None:
        from app.infra.metrics import get_registry

        get_registry().reset()
        # a request with an unusual method must not mint a new label value
        client.request("BREW", "/healthz")
        body = client.get("/metrics").text
        assert 'method="other"' in body
        assert 'method="BREW"' not in body


class TestTracing:
    def test_span_is_noop_without_tracer(self) -> None:
        from app.infra.metrics import span

        # no tracer registered: must not raise, just runs the block
        ran = []
        with span("work", {"k": "v"}):
            ran.append(1)
        assert ran == [1]

    def test_span_delegates_to_registered_tracer(self) -> None:
        from contextlib import contextmanager

        from app.infra.metrics import set_tracer, span

        calls = []

        class FakeTracer:
            @contextmanager
            def span(self, name, attributes=None):
                calls.append((name, attributes))
                yield

        set_tracer(FakeTracer())
        try:
            with span("run.exec", {"run_id": "r1"}):
                pass
            assert calls == [("run.exec", {"run_id": "r1"})]
        finally:
            set_tracer(None)  # restore no-op for other tests

    def test_span_records_duration_metric(self) -> None:
        from app.infra.metrics import get_registry, span

        get_registry().reset()
        with span("timed.block"):
            pass
        out = get_registry().render()
        assert "paw_span_duration_seconds" in out
        assert 'span="timed.block"' in out
