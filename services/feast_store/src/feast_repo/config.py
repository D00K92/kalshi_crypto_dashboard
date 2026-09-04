"""Configuration boundary for Feast jobs and serving."""
from dataclasses import dataclass


@dataclass(frozen=True)
class FeastSettings:
    """Runtime settings shared by apply, materialization, and validation."""

    repo_path: str = "."
    project: str = "kalshi_crypto"
    feature_version: str = "v1"
    gcs_bucket: str = "kalshi-crypto-tick-data"
    redis_url: str | None = None


def load_settings() -> FeastSettings:
    """Load validated settings from environment or deployment configuration."""
    raise NotImplementedError
