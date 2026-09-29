"""Durable blob storage: the system of record for uploads and artifacts.

Local disk dies with the pod, so a conversation's files must live
somewhere durable (object storage in production).  This module is that
durability layer, behind a small key/bytes interface with two backends:

* ``LocalStorage`` -- a directory tree, the default (dev / single node).
* ``S3Storage``    -- an S3-compatible bucket (prod / multi-node).

Two-tier model: this store is the *durable* tier (the source of
truth).  The sandbox harness still needs files as real paths in its
working directory, so the per-conversation workspace remains a
*sandbox-facing* tier -- a materialized working copy that is written
through to this store on upload and can be rehydrated from it after a
pod loss.  So Storage never replaces the workspace; it backs it.

Keys are opaque, ``/``-joined strings (e.g. ``"<conversation>/<name>"``);
a backend maps them to a path or an object key.  This module is a pure
infra primitive: no ``Session``, no ORM entity, no request scope -- it
imports only stdlib + config (and, lazily, boto3 for S3).
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO, Protocol, runtime_checkable

from .config import get_settings


class StorageError(RuntimeError):
    """A backend operation failed (missing key, transport error)."""


@runtime_checkable
class Storage(Protocol):
    """Durable key/bytes store.  Keys are opaque ``/``-joined strings."""

    def put_bytes(self, key: str, data: bytes) -> None:
        """Store ``data`` under ``key`` (overwrites)."""
        ...

    def put_stream(self, key: str, stream: BinaryIO) -> int:
        """Store a stream under ``key``; return the number of bytes written."""
        ...

    def get_bytes(self, key: str) -> bytes:
        """Return the bytes stored under ``key`` (raises if absent)."""
        ...

    def open_stream(self, key: str) -> Iterator[bytes]:
        """Yield the object's bytes in chunks (raises if absent)."""
        ...

    def exists(self, key: str) -> bool: ...

    def size(self, key: str) -> int | None:
        """Byte length of the stored object, or None if absent."""
        ...

    def delete(self, key: str) -> None:
        """Remove ``key``; a missing key is not an error (idempotent)."""
        ...


_CHUNK = 1024 * 1024


def _safe_key(key: str) -> str:
    """Reject keys that could escape the storage root.

    Keys are ``/``-joined; ``..`` or an absolute component would let a
    crafted key walk out of a LocalStorage root or forge an S3 prefix.
    """
    if not key or key.startswith("/") or ".." in Path(key).parts:
        raise StorageError(f"unsafe storage key: {key!r}")
    return key


class LocalStorage:
    """Filesystem-backed store rooted at a directory.

    The default backend: durable enough for a single node, and the same
    on-disk layout the workspace already uses, so dev behavior is
    unchanged.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def _path(self, key: str) -> Path:
        return self._root / _safe_key(key)

    def put_bytes(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def put_stream(self, key: str, stream: BinaryIO) -> int:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        with path.open("wb") as out:
            while chunk := stream.read(_CHUNK):
                written += len(chunk)
                out.write(chunk)
        return written

    def get_bytes(self, key: str) -> bytes:
        try:
            return self._path(key).read_bytes()
        except OSError as exc:
            raise StorageError(f"cannot read {key!r}: {exc}") from exc

    def open_stream(self, key: str) -> Iterator[bytes]:
        path = self._path(key)
        if not path.is_file():
            raise StorageError(f"no such key: {key!r}")

        def _gen() -> Iterator[bytes]:
            with path.open("rb") as fh:
                while chunk := fh.read(_CHUNK):
                    yield chunk

        return _gen()

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def size(self, key: str) -> int | None:
        path = self._path(key)
        return path.stat().st_size if path.is_file() else None

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)


def get_storage() -> Storage:
    """The configured durable store (process-wide, chosen by settings)."""
    settings = get_settings()
    backend = settings.storage.backend
    if backend == "local":
        return LocalStorage(settings.storage.local_root)
    if backend == "s3":
        from .storage_s3 import S3Storage

        return S3Storage(
            bucket=settings.storage.s3_bucket,
            prefix=settings.storage.s3_prefix,
            region=settings.storage.s3_region or None,
            endpoint_url=settings.storage.s3_endpoint_url or None,
        )
    raise ValueError(f"unknown storage backend: {backend!r}")
