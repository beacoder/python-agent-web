"""S3-compatible backend for the durable blob store (see storage.py).

Kept in its own module so ``boto3`` is imported lazily -- it is an
optional dependency (extra ``s3``), needed only when
``PAW_STORAGE__BACKEND=s3``.  Works against real AWS S3 or any
S3-compatible endpoint (MinIO, localstack) via ``s3_endpoint_url``.

The client is injectable so tests exercise the key-mapping and error
translation without a network or moto.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, BinaryIO

from .storage import _CHUNK, StorageError, _safe_key


class S3Storage:
    """Object-storage backend behind the ``Storage`` protocol.

    Keys are mapped to ``<prefix><key>`` object names in ``bucket``.
    Botocore's ``ClientError`` (missing key, transport, auth) is
    translated to ``StorageError`` so callers depend only on the
    storage contract, never on boto3.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        region: str | None = None,
        endpoint_url: str | None = None,
        client: Any = None,
    ) -> None:
        if not bucket:
            raise StorageError("s3 storage requires a bucket name")
        self._bucket = bucket
        # normalize prefix to '' or 'something/' so join is a plain concat
        self._prefix = prefix.strip("/")
        if self._prefix:
            self._prefix += "/"
        self._client = client
        self._region = region
        self._endpoint_url = endpoint_url

    def _s3(self) -> Any:
        if self._client is None:
            import boto3  # lazy: only when the s3 backend is selected

            self._client = boto3.client(
                "s3", region_name=self._region, endpoint_url=self._endpoint_url
            )
        return self._client

    def _obj(self, key: str) -> str:
        return self._prefix + _safe_key(key)

    def _client_error(self) -> type[Exception]:
        try:
            from botocore.exceptions import ClientError

            return ClientError
        except Exception:  # botocore always ships with boto3; be defensive
            return Exception

    def put_bytes(self, key: str, data: bytes) -> None:
        try:
            self._s3().put_object(Bucket=self._bucket, Key=self._obj(key), Body=data)
        except self._client_error() as exc:
            raise StorageError(f"s3 put failed for {key!r}: {exc}") from exc

    def put_stream(self, key: str, stream: BinaryIO) -> int:
        # Read the stream to count bytes and hand a body to put_object.
        # upload_fileobj would stream without a count; callers here want
        # the size (upload cap is enforced before this), so read once.
        data = stream.read()
        self.put_bytes(key, data)
        return len(data)

    def get_bytes(self, key: str) -> bytes:
        try:
            resp = self._s3().get_object(Bucket=self._bucket, Key=self._obj(key))
            return resp["Body"].read()
        except self._client_error() as exc:
            raise StorageError(f"s3 get failed for {key!r}: {exc}") from exc

    def open_stream(self, key: str) -> Iterator[bytes]:
        try:
            resp = self._s3().get_object(Bucket=self._bucket, Key=self._obj(key))
        except self._client_error() as exc:
            raise StorageError(f"no such key: {key!r} ({exc})") from exc
        body = resp["Body"]

        def _gen() -> Iterator[bytes]:
            while chunk := body.read(_CHUNK):
                yield chunk

        return _gen()

    def exists(self, key: str) -> bool:
        try:
            self._s3().head_object(Bucket=self._bucket, Key=self._obj(key))
            return True
        except self._client_error():
            return False

    def size(self, key: str) -> int | None:
        try:
            resp = self._s3().head_object(Bucket=self._bucket, Key=self._obj(key))
            return int(resp.get("ContentLength", 0))
        except self._client_error():
            return None

    def delete(self, key: str) -> None:
        # S3 delete_object is idempotent (deleting a missing key is a
        # no-op, matching the LocalStorage contract).
        try:
            self._s3().delete_object(Bucket=self._bucket, Key=self._obj(key))
        except self._client_error() as exc:
            raise StorageError(f"s3 delete failed for {key!r}: {exc}") from exc
