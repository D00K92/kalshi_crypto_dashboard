"""Evaluate a deployed/challenger model on the newest labeled window."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd

from src.common.evaluation import evaluate_frame, retrain_decision


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Joined feature/target parquet")
    parser.add_argument("--model", required=True, help="Directory containing model.joblib")
    parser.add_argument("--horizon", choices=("1m", "5m", "15m", "30m", "1h"), required=True)
    parser.add_argument("--champion-metrics", required=True, help="JSON with champion metrics")
    parser.add_argument("--state", default=None, help="Optional JSON state file")
    parser.add_argument("--window-rows", type=int, default=0)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = evaluate_frame(pd.read_parquet(args.dataset), joblib.load(Path(args.model) / "model.joblib"),
                            args.horizon, args.window_rows)
    champion = json.loads(Path(args.champion_metrics).read_text(encoding="utf-8"))
    state = json.loads(Path(args.state).read_text(encoding="utf-8")) if args.state and Path(args.state).exists() else {}
    result["decision"] = retrain_decision(result, champion, prior_failures=state.get("consecutive_failures", 0),
                                           last_triggered_at=state.get("last_triggered_at"))
    if args.state:
        decision = result["decision"]
        next_state = {"consecutive_failures": decision["consecutive_failures"],
                      "last_triggered_at": (datetime.now(timezone.utc).isoformat()
                                             if decision["trigger_retraining"] else state.get("last_triggered_at"))}
        Path(args.state).write_text(json.dumps(next_state, indent=2), encoding="utf-8")
    Path(args.output).write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
