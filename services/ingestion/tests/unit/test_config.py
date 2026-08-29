from __future__ import annotations

from ingestion.config import Settings


def test_gke_redis_host_and_port_build_connection_url(monkeypatch) -> None:
    monkeypatch.delenv("INGESTION_REDIS_URL", raising=False)
    monkeypatch.setenv("REDIS_HOST", "10.20.30.40")
    monkeypatch.setenv("REDIS_PORT", "6380")

    settings = Settings.from_env()

    assert settings.redis_url == "redis://10.20.30.40:6380/0"


def test_explicit_redis_url_takes_precedence(monkeypatch) -> None:
    monkeypatch.setenv("INGESTION_REDIS_URL", "rediss://redis.example:6380/2")
    monkeypatch.setenv("REDIS_HOST", "ignored")
    monkeypatch.setenv("REDIS_PORT", "1234")

    settings = Settings.from_env()

    assert settings.redis_url == "rediss://redis.example:6380/2"


def test_kalshi_rsa_path_loads_private_key_without_exposing_contents(monkeypatch, tmp_path) -> None:
    key_file = tmp_path / "kalshi.pem"
    key_file.write_text("test-private-key", encoding="utf-8")
    monkeypatch.setenv("KALSHI_API_KEY", "key-id")
    monkeypatch.setenv("KALSHI_RSA_PATH", str(key_file))
    monkeypatch.delenv("KALSHI_PRIVATE_KEY", raising=False)

    settings = Settings.from_env()

    assert settings.kalshi_private_key == "test-private-key"
