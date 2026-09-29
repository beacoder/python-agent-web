"""Durable storage backends: LocalStorage and S3Storage.

Both are exercised behind the shared ``Storage`` protocol so the tests
double as a conformance suite -- the contract (round-trip, streaming,
idempotent delete, missing-key error, traversal rejection) must hold
for either backend.  S3 uses an in-memory fake client so no network or
moto is required; the real client path is a thin lazy ``boto3.client``.
"""

from __future__ import annotations

import io

import pytest

from app.infra.storage import LocalStorage, Storage, StorageError, get_storage
from app.infra.storage_s3 import S3Storage

# -- an in-memory fake S3 client ------------------------------------------


class _FakeError(Exception):
    pass


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._d = data
        self._o = 0

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            n = len(self._d) - self._o
        chunk = self._d[self._o : self._o + n]
        self._o += len(chunk)
        return chunk


class FakeS3Client:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket, Key, Body) -> None:  # noqa: N803 - boto3 kwargs
        self.store[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.read()

    def get_object(self, Bucket, Key):  # noqa: N803
        if (Bucket, Key) not in self.store:
            raise _FakeError("NoSuchKey")
        return {"Body": _FakeBody(self.store[(Bucket, Key)])}

    def head_object(self, Bucket, Key) -> dict:  # noqa: N803
        if (Bucket, Key) not in self.store:
            raise _FakeError("404")
        return {"ContentLength": len(self.store[(Bucket, Key)])}

    def delete_object(self, Bucket, Key) -> None:  # noqa: N803
        self.store.pop((Bucket, Key), None)


def _s3(prefix: str = "paw") -> S3Storage:
    fake = FakeS3Client()
    store = S3Storage(bucket="b", prefix=prefix, client=fake)
    store._client_error = lambda: _FakeError  # translate the fake's error
    store._fake = fake  # expose for object-key assertions
    return store


@pytest.fixture(params=["local", "s3"])
def store(request, tmp_path) -> Storage:
    if request.param == "local":
        return LocalStorage(tmp_path)
    return _s3()


# -- conformance: must hold for either backend ----------------------------


class TestStorageContract:
    def test_is_storage_protocol(self, store: Storage) -> None:
        assert isinstance(store, Storage)

    def test_put_get_bytes_round_trip(self, store: Storage) -> None:
        store.put_bytes("c1/a.txt", b"hello")
        assert store.get_bytes("c1/a.txt") == b"hello"

    def test_exists(self, store: Storage) -> None:
        assert not store.exists("c1/x")
        store.put_bytes("c1/x", b"1")
        assert store.exists("c1/x")

    def test_put_stream_returns_size(self, store: Storage) -> None:
        n = store.put_stream("c1/b.bin", io.BytesIO(b"y" * 4096))
        assert n == 4096
        assert store.get_bytes("c1/b.bin") == b"y" * 4096

    def test_open_stream_yields_all_bytes(self, store: Storage) -> None:
        store.put_bytes("c1/s.bin", b"z" * 5000)
        assert b"".join(store.open_stream("c1/s.bin")) == b"z" * 5000

    def test_delete_is_idempotent(self, store: Storage) -> None:
        store.put_bytes("c1/d", b"1")
        store.delete("c1/d")
        store.delete("c1/d")  # no error the second time
        assert not store.exists("c1/d")

    def test_size_reports_length_or_none(self, store: Storage) -> None:
        assert store.size("c1/missing") is None
        store.put_bytes("c1/sz", b"12345")
        assert store.size("c1/sz") == 5

    def test_get_missing_raises(self, store: Storage) -> None:
        with pytest.raises(StorageError):
            store.get_bytes("c1/nope")

    def test_open_missing_raises(self, store: Storage) -> None:
        with pytest.raises(StorageError):
            list(store.open_stream("c1/nope"))

    @pytest.mark.parametrize("bad", ["/abs/key", "../escape", "a/../../b", ""])
    def test_unsafe_keys_rejected(self, store: Storage, bad: str) -> None:
        with pytest.raises(StorageError):
            store.put_bytes(bad, b"x")


# -- backend-specific -----------------------------------------------------


class TestLocalStorage:
    def test_writes_under_root(self, tmp_path) -> None:
        store = LocalStorage(tmp_path)
        store.put_bytes("conv/file.txt", b"data")
        assert (tmp_path / "conv" / "file.txt").read_bytes() == b"data"


class TestS3Storage:
    def test_prefix_is_applied_to_object_key(self) -> None:
        store = _s3(prefix="paw")
        store.put_bytes("conv/file.txt", b"data")
        assert ("b", "paw/conv/file.txt") in store._fake.store

    def test_empty_prefix_maps_key_directly(self) -> None:
        store = _s3(prefix="")
        store.put_bytes("conv/file.txt", b"data")
        assert ("b", "conv/file.txt") in store._fake.store

    def test_empty_bucket_rejected(self) -> None:
        with pytest.raises(StorageError):
            S3Storage(bucket="")


# -- config selection -----------------------------------------------------


class TestBackendSelection:
    def test_local_is_default(self) -> None:
        assert type(get_storage()).__name__ == "LocalStorage"

    def test_s3_selected_by_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.infra.config import get_settings

        monkeypatch.setenv("PAW_STORAGE__BACKEND", "s3")
        monkeypatch.setenv("PAW_STORAGE__S3_BUCKET", "my-bucket")
        get_settings.cache_clear()
        try:
            store = get_storage()
            assert type(store).__name__ == "S3Storage"
        finally:
            get_settings.cache_clear()

    def test_unknown_backend_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.infra.config import get_settings

        monkeypatch.setenv("PAW_STORAGE__BACKEND", "nope")
        get_settings.cache_clear()
        try:
            with pytest.raises(ValueError, match="unknown storage backend"):
                get_storage()
        finally:
            get_settings.cache_clear()
