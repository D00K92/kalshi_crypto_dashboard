"""Configuration boundary for Feast jobs and serving."""
from dataclasses import dataclass
import os
from pathlib import Path


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
    repo_path = os.getenv("FEAST_REPO_PATH", ".")
    if not (Path(repo_path) / "feature_store.yaml").is_file():
        raise ValueError(f"FEAST_REPO_PATH must contain feature_store.yaml: {repo_path}")
    feature_version = os.getenv("FEATURE_VERSION", "v1")
    if not feature_version:
        raise ValueError("FEATURE_VERSION must not be empty")
    return FeastSettings(
        repo_path=repo_path,
        project=os.getenv("GCP_PROJECT_ID", "kalshi-crypto-506614"),
        feature_version=feature_version,
        gcs_bucket=os.getenv("GCS_BUCKET", "kalshi-crypto-tick-data"),
        redis_url=os.getenv("REDIS_URL"),
    )
