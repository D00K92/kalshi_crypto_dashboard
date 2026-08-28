"""Coinbase Advanced Trade WebSocket JWT generation."""

from __future__ import annotations

import secrets
import time

import jwt
from cryptography.hazmat.primitives import serialization


class CoinbaseAuthError(ValueError):
    """Raised when Coinbase JWT credentials are missing or malformed."""


def build_coinbase_ws_jwt(key_name: str, key_secret: str, *, now: int | None = None) -> str:
    if not key_name:
        raise CoinbaseAuthError("Coinbase API key name is required")
    if not key_secret:
        raise CoinbaseAuthError("Coinbase API private key is required")
    pem = key_secret.replace("\\n", "\n")
    try:
        private_key = serialization.load_pem_private_key(pem.encode(), password=None)
    except (ValueError, TypeError) as exc:
        raise CoinbaseAuthError("Coinbase API secret is not a valid PEM private key") from exc
    issued = int(time.time()) if now is None else now
    return jwt.encode(
        {"sub": key_name, "iss": "cdp", "nbf": issued, "exp": issued + 120},
        private_key,
        algorithm="ES256",
        headers={"kid": key_name, "nonce": secrets.token_hex(16)},
    )
