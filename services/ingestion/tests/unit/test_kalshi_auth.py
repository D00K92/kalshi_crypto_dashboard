from __future__ import annotations

import base64

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from ingestion.kalshi_auth import KalshiAuthError, build_auth_headers, load_private_key


def test_build_auth_headers_creates_verifiable_rsa_pss_signature() -> None:
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    headers = build_auth_headers("key-id", private, now_ms=1700000000123)
    message = b"1700000000123GET/trade-api/ws/v2"
    private.public_key().verify(
        base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256().digest_size),
        hashes.SHA256(),
    )
    assert headers["KALSHI-ACCESS-TIMESTAMP"] == "1700000000123"


def test_load_private_key_accepts_escaped_newlines() -> None:
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode().replace("\n", "\\n")
    assert load_private_key(pem).private_numbers().public_numbers.n == private.private_numbers().public_numbers.n


def test_missing_credentials_raise_without_logging_secret() -> None:
    with pytest.raises(KalshiAuthError, match="API key"):
        build_auth_headers("", rsa.generate_private_key(public_exponent=65537, key_size=2048), now_ms=1)
