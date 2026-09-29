"""In-process metrics registry + tracing hooks.

Dependency-free by design: a small counter/histogram registry that
renders Prometheus text exposition at ``/metrics``, plus a ``span``
tracing hook that is a no-op until a tracer is registered (so wiring
OpenTelemetry later is a one-liner, and nothing is required today).

Not a full metrics client -- it is the minimum that makes runs, tokens,
and HTTP observable by a scraper, without pulling in a backend.  Labels
are supported as sorted key/value tuples so a metric can be sliced
(e.g. http_requests_total by method+status).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Protocol

_Labels = tuple[tuple[str, str], ...]


def _labelset(labels: dict[str, str] | None) -> _Labels:
    if not labels:
        return ()
    return tuple(sorted(labels.items()))


# Prometheus default histogram buckets (seconds), fine for request/run latency.
_DEFAULT_BUCKETS = (0.005, 0.025, 0.1, 0.5, 1.0, 5.0, 30.0, 120.0)


class MetricsRegistry:
    """Thread-safe counters and histograms with optional labels."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, _Labels], float] = {}
        self._hist_sum: dict[tuple[str, _Labels], float] = {}
        self._hist_count: dict[tuple[str, _Labels], int] = {}
        self._hist_buckets: dict[tuple[str, _Labels], list[int]] = {}
        self._help: dict[str, str] = {}

    def counter(
        self, name: str, value: float = 1.0, labels: dict[str, str] | None = None, help: str = ""
    ) -> None:
        key = (name, _labelset(labels))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + value
            if help and name not in self._help:
                self._help[name] = help

    def observe(
        self, name: str, value: float, labels: dict[str, str] | None = None, help: str = ""
    ) -> None:
        key = (name, _labelset(labels))
        with self._lock:
            self._hist_sum[key] = self._hist_sum.get(key, 0.0) + value
            self._hist_count[key] = self._hist_count.get(key, 0) + 1
            buckets = self._hist_buckets.get(key)
            if buckets is None:
                buckets = [0] * len(_DEFAULT_BUCKETS)
                self._hist_buckets[key] = buckets
            for i, edge in enumerate(_DEFAULT_BUCKETS):
                if value <= edge:
                    buckets[i] += 1
            if help and name not in self._help:
                self._help[name] = help

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._hist_sum.clear()
            self._hist_count.clear()
            self._hist_buckets.clear()

    def render(self) -> str:
        """Prometheus text exposition format."""
        lines: list[str] = []
        with self._lock:
            for name in sorted({n for (n, _) in self._counters}):
                if name in self._help:
                    lines.append(f"# HELP {name} {self._help[name]}")
                lines.append(f"# TYPE {name} counter")
                for (n, labels), val in self._counters.items():
                    if n != name:
                        continue
                    lines.append(f"{name}{_fmt_labels(labels)} {val}")
            for name in sorted({n for (n, _) in self._hist_count}):
                if name in self._help:
                    lines.append(f"# HELP {name} {self._help[name]}")
                lines.append(f"# TYPE {name} histogram")
                for (n, labels), buckets in self._hist_buckets.items():
                    if n != name:
                        continue
                    cumulative = 0
                    for i, edge in enumerate(_DEFAULT_BUCKETS):
                        cumulative += buckets[i]
                        le = _fmt_labels(labels, extra=("le", _fmt_float(edge)))
                        lines.append(f"{name}_bucket{le} {cumulative}")
                    inf = _fmt_labels(labels, extra=("le", "+Inf"))
                    lines.append(f"{name}_bucket{inf} {self._hist_count[(n, labels)]}")
                    lines.append(f"{name}_sum{_fmt_labels(labels)} {self._hist_sum[(n, labels)]}")
                    lines.append(
                        f"{name}_count{_fmt_labels(labels)} {self._hist_count[(n, labels)]}"
                    )
        return "\n".join(lines) + "\n"


def _fmt_float(v: float) -> str:
    return repr(v)


def _fmt_labels(labels: _Labels, extra: tuple[str, str] | None = None) -> str:
    items = list(labels)
    if extra is not None:
        items = items + [extra]
    if not items:
        return ""
    inner = ",".join(f'{k}="{v}"' for k, v in items)
    return "{" + inner + "}"


_registry = MetricsRegistry()


def get_registry() -> MetricsRegistry:
    return _registry


# Bounded set of HTTP methods for the method label -- an arbitrary or
# garbage verb must not create a new permanent metric series (unbounded
# label cardinality is a memory-growth DoS).  Anything else is "other".
_KNOWN_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})


def normalize_method(method: str) -> str:
    """Map an HTTP method to a bounded label value."""
    return method.upper() if method.upper() in _KNOWN_METHODS else "other"


# -- tracing hooks --------------------------------------------------------


class Tracer(Protocol):
    @contextmanager
    def span(self, name: str, attributes: dict[str, Any] | None = None) -> Iterator[None]: ...


_tracer: Tracer | None = None


def set_tracer(tracer: Tracer | None) -> None:
    """Register a tracer (e.g. an OpenTelemetry adapter).  ``None``
    restores the no-op behavior."""
    global _tracer
    _tracer = tracer


@contextmanager
def span(name: str, attributes: dict[str, Any] | None = None) -> Iterator[None]:
    """Trace a block of work.

    A no-op (zero overhead beyond the context manager) unless a tracer
    is registered via ``set_tracer`` -- so run/request spans are already
    marked in the code and become real spans the moment a backend is
    wired, with no call-site changes.  Also records a duration histogram
    so timing is visible even without a tracer.
    """
    start = time.perf_counter()
    if _tracer is not None:
        with _tracer.span(name, attributes):
            yield
    else:
        yield
    _registry.observe(
        "paw_span_duration_seconds", time.perf_counter() - start, labels={"span": name}
    )
