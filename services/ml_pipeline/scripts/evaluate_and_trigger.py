"""Evaluate the champion on a newly labeled window and trigger KFP if needed."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd

from src.common.evaluation import evaluate_frame, retrain_decision

HORIZONS = ("1m", "5m", "15m", "30m", "1h")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Joined, newly labeled feature/target parquet")
    parser.add_argument("--model-root", required=True, help="Directory containing <horizon>/model.joblib")
    parser.add_argument("--champion-metrics", required=True, help="JSON mapping horizons to champion metrics")
    parser.add_argument("--state", required=True, help="JSON state file used for idempotency/cooldown")
    parser.add_argument("--output", required=True, help="Evaluation report JSON")
    parser.add_argument("--trigger-training", action="store_true")
    parser.add_argument("--project")
    parser.add_argument("--location", default="asia-northeast3")
    parser.add_argument("--template", default="volatility_training_pipeline.json")
    parser.add_argument("--pipeline-root")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--model-version", default="v1")
    args = parser.parse_args()

    table = pd.read_parquet(args.dataset)
    champions = json.loads(Path(args.champion_metrics).read_text(encoding="utf-8"))
    state_path = Path(args.state)
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    report = {"evaluated_at": datetime.now(timezone.utc).isoformat(), "horizons": {}, "trigger_retraining": False}
    for horizon in HORIZONS:
        model = joblib.load(Path(args.model_root) / horizon / "model.joblib")
        champion = champions[horizon] if horizon in champions else champions
        result = evaluate_frame(table, model, horizon)
        result["decision"] = retrain_decision(
            result, champion, prior_failures=state.get("consecutive_failures", 0),
            last_triggered_at=state.get("last_triggered_at"),
        )
        report["horizons"][horizon] = result
        report["trigger_retraining"] |= result["decision"]["trigger_retraining"]

    if report["trigger_retraining"] and args.trigger_training:
        required = (args.project, args.pipeline_root, args.start_date, args.end_date)
        if not all(required):
            raise ValueError("--project, --pipeline-root, --start-date, and --end-date are required to trigger training")
        subprocess.run([
            sys.executable, str(Path(__file__).with_name("run_pipeline.py")),
            "--template", args.template, "--project", args.project, "--location", args.location,
            "--pipeline-root", args.pipeline_root, "--start-date", args.start_date,
            "--end-date", args.end_date, "--model-version", args.model_version,
        ], check=True)
        state["last_triggered_at"] = report["evaluated_at"]
    state["last_evaluated_at"] = report["evaluated_at"]
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"trigger_retraining": report["trigger_retraining"], "horizons": list(report["horizons"])}), flush=True)


if __name__ == "__main__":
    main()
