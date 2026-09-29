"""Magic-byte content sniffing for uploads.

A filename extension is a claim, not a fact -- ``sales.xlsx`` can hold
anything.  This checks the leading bytes of an upload against known file
signatures so the *content* must match one of an allow-listed set of
types before it is accepted.  A pure ``infra`` primitive: no session, no
entities, stdlib only.

Signatures are deliberately small and well-known.  Note that modern
Office formats (xlsx/docx/pptx) are ZIP containers, so they share the
ZIP signature -- ``xlsx`` is accepted as "a zip" here; distinguishing
the Office subtype would require reading the archive, which is out of
scope for a byte-signature gate.
"""

from __future__ import annotations

# short name -> list of acceptable leading-byte signatures
_SIGNATURES: dict[str, list[bytes]] = {
    "zip": [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"],
    "xlsx": [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"],  # zip container
    "docx": [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"],
    "pptx": [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"],
    "pdf": [b"%PDF-"],
    "png": [b"\x89PNG\r\n\x1a\n"],
    "jpg": [b"\xff\xd8\xff"],
    "gif": [b"GIF87a", b"GIF89a"],
    "json": [],  # text: no signature, validated as utf-8 text below
    "csv": [],
    "txt": [],
    "text": [],
}

_TEXT_TYPES = {"json", "csv", "txt", "text"}


def is_probably_text(head: bytes) -> bool:
    """Heuristic: decodable as UTF-8 and free of NUL bytes."""
    if b"\x00" in head:
        return False
    try:
        head.decode("utf-8")
        return True
    except UnicodeDecodeError:
        # a multibyte char may be split at the chunk boundary; tolerate a
        # short tail by retrying without the last few bytes
        try:
            head[:-3].decode("utf-8")
            return True
        except UnicodeDecodeError:
            return False


def sniff_matches(head: bytes, allowed: list[str]) -> bool:
    """Does ``head`` match at least one of the ``allowed`` type names?

    Empty ``allowed`` means validation is disabled -> always True.
    Unknown type names in ``allowed`` are ignored (they can never match),
    so a typo fails closed rather than silently allowing everything.
    """
    if not allowed:
        return True
    for name in allowed:
        key = name.lower().lstrip(".")
        sigs = _SIGNATURES.get(key)
        if sigs is None:
            continue  # unknown type name: cannot match
        if key in _TEXT_TYPES:
            if is_probably_text(head):
                return True
            continue
        if any(head.startswith(sig) for sig in sigs):
            return True
    return False
