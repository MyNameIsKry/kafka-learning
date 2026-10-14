"""Lossless fallback payload for Kafka records that cannot be decoded."""

from base64 import b64encode
from typing import Any


def undecoded_payload(value: bytes | None) -> dict[str, Any]:
    if value is None:
        return {"rawBase64": None, "encoding": "base64"}
    return {"rawBase64": b64encode(value).decode("ascii"), "encoding": "base64"}
