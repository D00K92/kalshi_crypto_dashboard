"""Container entrypoint for QLIKE/EWMA candidate evaluation."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import pandas as pd
import gcsfs

from src.common.evaluation import evaluate_frame, retrain_decision


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--horizon", required=True)
    p.add_argument("--champion-metrics", required=True)
    p.add_argument("--report", required=True)
    p.add_argument("--promote", required=True)
    a = p.parse_args()
    result = evaluate_frame(pd.read_parquet(a.dataset), joblib.load(Path(a.model) / "model.joblib"), a.horizon)
    if a.champion_metrics.startswith("gs://"):
        with gcsfs.GCSFileSystem().open(a.champion_metrics, "r") as handle:
            champion = json.load(handle)
    else:
        champion = json.loads(Path(a.champion_metrics).read_text(encoding="utf-8"))
    decision = retrain_decision(result, champion.get(a.horizon, champion))
    promoted = bool(decision["beats_benchmark"] and not decision["degraded"])
    result["decision"], result["promoted"] = decision, promoted
    Path(a.report).write_text(json.dumps(result, indent=2), encoding="utf-8")
    Path(a.promote).write_text(str(promoted).lower(), encoding="utf-8")


if __name__ == "__main__":
    main()
