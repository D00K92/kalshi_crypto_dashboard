"""Apply Feast entities, sources, views, and services."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess


def apply_repository(*, repo_path: str) -> None:
    """Apply the declarative Feast repository at ``repo_path``."""
    path = Path(repo_path).resolve()
    if not (path / "feature_store.yaml").is_file():
        raise FileNotFoundError(f"Feast repository config not found: {path / 'feature_store.yaml'}")
    subprocess.run(["feast", "apply"], cwd=path, check=True)


def main() -> None:
    """CLI entrypoint for applying the repository."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-path", default=os.getenv("FEAST_REPO_PATH", "."))
    args = parser.parse_args()
    apply_repository(repo_path=args.repo_path)
