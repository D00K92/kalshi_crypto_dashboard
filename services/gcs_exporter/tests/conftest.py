from __future__ import annotations

import pytest

from gcs_exporter.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        redis_url="redis://unused:6379/0",
        stream_name="stream:ticks",
        consumer_group="gcs_archiver_group",
        consumer_name="test-consumer",
        bucket_name="test-bucket",
        flush_size=10_000,
        flush_interval_seconds=60.0,
        read_count=500,
        read_block_ms=1_000,
        reclaim_interval_seconds=30.0,
        reclaim_min_idle_ms=120_000,
        shutdown_grace_seconds=5,
        health_port=18080,
        log_level="INFO",
    )
