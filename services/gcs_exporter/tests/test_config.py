from __future__ import annotations

from gcs_exporter.config import Settings


def test_gke_environment_builds_settings(monkeypatch) -> None:
    monkeypatch.delenv("GCS_EXPORTER_REDIS_URL", raising=False)
    monkeypatch.setenv("REDIS_HOST", "10.20.30.40")
    monkeypatch.setenv("REDIS_PORT", "6380")
    monkeypatch.setenv("HOSTNAME", "exporter-abc")

    settings = Settings.from_env()

    assert settings.redis_url == "redis://10.20.30.40:6380/0"
    assert settings.consumer_group == "gcs_archiver_group"
    assert settings.consumer_name == "pod-exporter-abc"
    assert settings.bucket_name == "kalshi-crypto-tick-data"
    assert settings.flush_size == 10_000
    assert settings.flush_interval_seconds == 60.0


def test_explicit_redis_url_takes_precedence(monkeypatch) -> None:
    monkeypatch.setenv("GCS_EXPORTER_REDIS_URL", "rediss://redis.example:6380/2")
    monkeypatch.setenv("REDIS_HOST", "ignored")

    assert Settings.from_env().redis_url == "rediss://redis.example:6380/2"
