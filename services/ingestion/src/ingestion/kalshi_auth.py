"""Kalshi API-key authentication primitives."""

from __future__ import annotations

import base64
import time

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


class KalshiAuthError(ValueError):
    """Raised when Kalshi credentials are missing or malformed."""


def load_private_key(pem_text: str):
    if not pem_text:
        raise KalshiAuthError("Kalshi private key is required")
    try:
        return serialization.load_pem_private_key(
            pem_text.replace("\\n", "\n").encode(), password=None
        )
    except (ValueError, TypeError) as exc:
        raise KalshiAuthError("Kalshi private key is not valid PEM") from exc


def build_auth_headers(
    api_key: str,
    private_key,
    *,
    method: str = "GET",
    path: str = "/trade-api/ws/v2",
    now_ms: int | None = None,
) -> dict[str, str]:
    if not api_key:
        raise KalshiAuthError("Kalshi API key is required")
    timestamp = int(time.time() * 1000) if now_ms is None else int(now_ms)
    message = f"{timestamp}{method.upper()}{path.split('?', 1)[0]}".encode()
    try:
        signature = private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256().digest_size),
            hashes.SHA256(),
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise KalshiAuthError("Kalshi private key cannot create RSA-PSS signature") from exc
    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("ascii"),
        "KALSHI-ACCESS-TIMESTAMP": str(timestamp),
    }
