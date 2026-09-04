"""Compute causal EWMA volatility benchmark metrics for model comparison."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import gcsfs

from src.common.benchmarks import ewma_annualized_volatility
from src.common.data_io import load_training_table
from src.common.evaluation import score_predictions

HORIZONS = ("1m", "5m", "15m", "30m", "1h")


def compute_benchmark(table, decay: float = 0.96) -> dict:
    """Score EWMA on the same chronological holdout used by training."""
    report = {"name": "ewma", "decay": decay, "split": "test", "horizons": {}}
    for horizon in HORIZONS:
        target = f"target_rv_{horizon}"
        usable = table.dropna(subset=[target]).sort_values("timestamp").reset_index(drop=True)
        test_start = int(len(usable) * 0.85)
        if len(usable) <= test_start:
            raise ValueError(f"not enough rows for benchmark holdout: {horizon}")
        # Build the EWMA over all historical rows, then score only the test tail.
        prediction = ewma_annualized_volatility(usable, horizon, decay=decay)
        metrics = score_predictions(usable[target].iloc[test_start:], prediction[test_start:])
        report["horizons"][horizon] = {"rows": len(usable) - test_start, "metrics": metrics}
    report["training_cutoff"] = table.attrs.get("training_cutoff")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--project", required=True)
    parser.add_argument("--output", type=Path, default=Path("benchmark_metrics.json"))
    parser.add_argument("--decay", type=float, default=0.96)
    parser.add_argument("--feature-root", default="gs://kalshi-crypto-tick-data/features/v1")
    parser.add_argument("--target-root", default="gs://kalshi-crypto-tick-data/processed/future_realized_volatility")
    args = parser.parse_args()
    if not 0 < args.decay < 1:
        raise ValueError("decay must be between 0 and 1")
    fs = gcsfs.GCSFileSystem(project=args.project)
    table = load_training_table(fs, args.feature_root, args.target_root, args.start_date, args.end_date)
    report = compute_benchmark(table, decay=args.decay)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for horizon, result in report["horizons"].items():
        print(json.dumps(result["metrics"] | {"horizon": horizon}), flush=True)


if __name__ == "__main__":
    main()
