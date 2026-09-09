from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

import joblib
import pandas as pd


def _load_module():
    path = Path(__file__).parents[1] / "containers" / "evaluate" / "main.py"
    spec = importlib.util.spec_from_file_location("ml_evaluate_container", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_evaluate_container_creates_nested_artifact_parents(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "gcsfs", types.SimpleNamespace(GCSFileSystem=object))
    module = _load_module()
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    joblib.dump(object(), model_dir / "model.joblib")
    (model_dir / "metadata.json").write_text(
        json.dumps({"rows": {"test": 13}}), encoding="utf-8"
    )
    champion = tmp_path / "champion.json"
    champion.write_text(json.dumps({"qlike": -0.8}), encoding="utf-8")
    dataset = tmp_path / "dataset.parquet"
    pd.DataFrame({"timestamp": []}).to_parquet(dataset)
    calls = []

    def evaluate_frame(*args, **kwargs):
        calls.append(kwargs)
        return {"metrics": {"qlike": -0.9}}

    monkeypatch.setattr(module, "evaluate_frame", evaluate_frame)
    monkeypatch.setattr(module, "retrain_decision", lambda *_: {"beats_benchmark": False, "degraded": False})
    report = tmp_path / "nested" / "report"
    promote = tmp_path / "nested" / "promote"
    monkeypatch.setattr(sys, "argv", [
        "main.py", "--dataset", str(dataset),
        "--model", str(model_dir), "--horizon", "15m",
        "--champion-metrics", str(champion), "--report", str(report),
        "--promote", str(promote),
    ])

    module.main()

    assert json.loads(report.read_text(encoding="utf-8"))["promoted"] is False
    assert promote.read_text(encoding="utf-8") == "false"
    assert calls == [{"window_rows": 13}]
