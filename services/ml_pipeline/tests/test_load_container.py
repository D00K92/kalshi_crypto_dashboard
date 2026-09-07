"""Regression coverage for the Vertex training-data loader entrypoint."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


def _load_module():
    path = Path(__file__).parents[1] / "containers" / "load" / "main.py"
    spec = importlib.util.spec_from_file_location("ml_load_container", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_feast_loader_creates_nested_artifact_parent(tmp_path, monkeypatch):
    module = _load_module()
    monkeypatch.setattr(
        module,
        "load_training_table_from_feast",
        lambda **_: pd.DataFrame({"log_return": [0.0], "venue_count": [1]}),
    )
    output = tmp_path / "nested" / "output_dataset"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py", "--feast-repo", "/tmp/feast", "--start-date", "2026-08-31",
            "--end-date", "2026-09-06", "--output", str(output), "--project", "test",
        ],
    )

    module.main()

    assert output.exists()
    assert len(pd.read_parquet(output)) == 1
