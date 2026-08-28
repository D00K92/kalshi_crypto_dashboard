import pytest

from ingestion.adapters.coinbase_auth import CoinbaseAuthError, build_coinbase_ws_jwt


def test_rejects_missing_credentials() -> None:
    with pytest.raises(CoinbaseAuthError):
        build_coinbase_ws_jwt("", "", now=1)


def test_rejects_invalid_pem() -> None:
    with pytest.raises(CoinbaseAuthError, match="PEM"):
        build_coinbase_ws_jwt("organizations/o/apiKeys/k", "not-a-key", now=1)
