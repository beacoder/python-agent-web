"""File upload validation: magic-byte type gate and per-user quota."""

from __future__ import annotations

from app.infra.filetype import is_probably_text, sniff_matches


class TestSniff:
    def test_empty_allowlist_accepts_anything(self) -> None:
        assert sniff_matches(b"\x00\x01\x02anything", []) is True

    def test_zip_xlsx_signature(self) -> None:
        assert sniff_matches(b"PK\x03\x04rest", ["xlsx"]) is True
        assert sniff_matches(b"PK\x03\x04rest", ["zip"]) is True

    def test_pdf_signature(self) -> None:
        assert sniff_matches(b"%PDF-1.7 ...", ["pdf"]) is True
        assert sniff_matches(b"not a pdf", ["pdf"]) is False

    def test_png_signature(self) -> None:
        assert sniff_matches(b"\x89PNG\r\n\x1a\n....", ["png"]) is True

    def test_mismatch_is_rejected(self) -> None:
        # a text body claiming to be allowed only as xlsx must fail
        assert sniff_matches(b"hello,world\n", ["xlsx"]) is False

    def test_text_types_accept_utf8(self) -> None:
        assert sniff_matches(b"a,b,c\n1,2,3\n", ["csv"]) is True
        assert sniff_matches(b'{"k": 1}', ["json"]) is True

    def test_text_type_rejects_binary(self) -> None:
        assert sniff_matches(b"\x00\x01\x02\x03", ["csv"]) is False

    def test_unknown_type_name_never_matches(self) -> None:
        # a typo in the allow-list fails closed, not open
        assert sniff_matches(b"PK\x03\x04", ["xlxs"]) is False

    def test_multiple_allowed_types(self) -> None:
        allowed = ["pdf", "png", "xlsx"]
        assert sniff_matches(b"%PDF-", allowed) is True
        assert sniff_matches(b"PK\x03\x04", allowed) is True
        assert sniff_matches(b"garbage", allowed) is False

    def test_is_probably_text(self) -> None:
        assert is_probably_text(b"plain text") is True
        assert is_probably_text(b"\x00binary") is False


class TestUploadValidationEndToEnd:
    def _setup(self, client):
        tokens = client.post(
            "/auth/register", json={"email": "fv@example.com", "password": "password-1"}
        ).json()
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        cid = client.post("/conversations", json={"title": "c"}, headers=headers).json()["id"]
        return headers, cid

    def test_rejects_content_not_matching_allowed_type(self, client, monkeypatch) -> None:
        from app.infra.config import get_settings

        monkeypatch.setattr(get_settings(), "upload_allowed_types", ["xlsx"])
        headers, cid = self._setup(client)
        # claims .xlsx but the bytes are plain text -> rejected 400
        res = client.post(
            f"/conversations/{cid}/files",
            headers=headers,
            files={"file": ("sales.xlsx", b"not really xlsx", "application/octet-stream")},
        )
        assert res.status_code == 400, res.text
        assert "allowed type" in res.json()["detail"]

    def test_accepts_content_matching_allowed_type(self, client, monkeypatch) -> None:
        from app.infra.config import get_settings

        monkeypatch.setattr(get_settings(), "upload_allowed_types", ["xlsx"])
        headers, cid = self._setup(client)
        res = client.post(
            f"/conversations/{cid}/files",
            headers=headers,
            files={"file": ("real.xlsx", b"PK\x03\x04zipbody", "application/octet-stream")},
        )
        assert res.status_code == 201, res.text

    def test_quota_exceeded_returns_413(self, client, monkeypatch) -> None:
        from app.infra.config import get_settings

        monkeypatch.setattr(get_settings(), "max_user_storage_bytes", 20)
        headers, cid = self._setup(client)
        # first small upload fits
        r1 = client.post(
            f"/conversations/{cid}/files",
            headers=headers,
            files={"file": ("a.txt", b"0123456789", "text/plain")},
        )
        assert r1.status_code == 201, r1.text
        # second upload would push total over the 20-byte quota
        r2 = client.post(
            f"/conversations/{cid}/files",
            headers=headers,
            files={"file": ("b.txt", b"0123456789ABCDEF", "text/plain")},
        )
        assert r2.status_code == 413, r2.text
        assert "quota" in r2.json()["detail"]

    def test_quota_zero_means_unlimited(self, client, monkeypatch) -> None:
        from app.infra.config import get_settings

        monkeypatch.setattr(get_settings(), "max_user_storage_bytes", 0)
        headers, cid = self._setup(client)
        res = client.post(
            f"/conversations/{cid}/files",
            headers=headers,
            files={"file": ("big.txt", b"x" * 5000, "text/plain")},
        )
        assert res.status_code == 201, res.text
