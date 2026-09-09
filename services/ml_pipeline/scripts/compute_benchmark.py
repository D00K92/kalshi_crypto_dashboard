"""Compute causal EWMA volatility benchmark metrics for model comparison."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import gcsfs

from src.common.benchmarks import ewma_annualized_volatility
from src.common.contracts import CURRENT_CONTRACT_VERSION
from src.common.data_io import (
    DEFAULT_TARGET_TABLE,
    load_training_table,
    load_training_table_from_feast,
)
from src.common.evaluation import score_predictions

HORIZONS = ("1m", "5m", "15m", "30m", "1h")


def compute_benchmark(table, decay: float = 0.96) -> dict:
    """Score EWMA and emit the horizon-keyed champion artifact contract."""
    report = {
        "_metadata": {
            "name": "ewma",
            "decay": decay,
            "split": "test",
            "training_cutoff": table.attrs.get("training_cutoff"),
            "feature_contract": table.attrs.get("feature_contract"),
        }
    }
    for horizon in HORIZONS:
        target = f"target_rv_{horizon}"
        usable = table.dropna(subset=[target]).sort_values("timestamp").reset_index(drop=True)
        test_start = int(len(usable) * 0.85)
        if len(usable) <= test_start:
            raise ValueError(f"not enough rows for benchmark holdout: {horizon}")
        # Build the EWMA over all historical rows, then score only the test tail.
        prediction = ewma_annualized_volatility(usable, horizon, decay=decay)
        metrics = score_predictions(usable[target].iloc[test_start:], prediction[test_start:])
        report[horizon] = {"rows": len(usable) - test_start, "metrics": metrics}
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--project", required=True)
    parser.add_argument("--output", type=Path, default=Path("benchmark_metrics.json"))
    parser.add_argument("--decay", type=float, default=0.96)
    parser.add_argument("--feast-repo")
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE)
    parser.add_argument("--feature-version", default=CURRENT_CONTRACT_VERSION)
    parser.add_argument("--feature-root", default="gs://kalshi-crypto-tick-data/features/v1")
    parser.add_argument("--target-root", default="gs://kalshi-crypto-tick-data/processed/future_realized_volatility")
    args = parser.parse_args()
    if not 0 < args.decay < 1:
        raise ValueError("decay must be between 0 and 1")
    if args.feast_repo:
        table = load_training_table_from_feast(
            project=args.project,
            feast_repo=args.feast_repo,
            start=args.start_date,
            end=args.end_date,
            target_table=args.target_table,
            feature_version=args.feature_version,
        )
    else:
        fs = gcsfs.GCSFileSystem(project=args.project)
        table = load_training_table(fs, args.feature_root, args.target_root, args.start_date, args.end_date)
    report = compute_benchmark(table, decay=args.decay)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for horizon in HORIZONS:
        result = report[horizon]
        print(json.dumps(result["metrics"] | {"horizon": horizon}), flush=True)


if __name__ == "__main__":
    main()
