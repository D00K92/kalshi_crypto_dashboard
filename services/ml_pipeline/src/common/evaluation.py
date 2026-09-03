"""Rolling evaluation and retraining decision policy."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

from src.common.benchmarks import ewma_annualized_volatility


def score_predictions(actual: pd.Series, prediction: np.ndarray) -> dict[str, float]:
    prediction = np.maximum(np.asarray(prediction, dtype=float), 0.0)
    actual = np.asarray(actual, dtype=float)
    actual_variance = np.maximum(actual * actual, 1e-18)
    forecast_variance = np.maximum(prediction * prediction, 1e-18)
    return {
        "qlike": float(np.mean(np.log(forecast_variance) + actual_variance / forecast_variance)),
        "rmse": float(np.sqrt(mean_squared_error(actual, prediction))),
        "mae": float(mean_absolute_error(actual, prediction)),
    }


def evaluate_frame(table: pd.DataFrame, model, horizon: str, window_rows: int = 0) -> dict:
    target = f"target_rv_{horizon}"
    if target not in table:
        raise ValueError(f"missing target column: {target}")
    usable = table.dropna(subset=[target]).sort_values("timestamp").reset_index(drop=True)
    if window_rows:
        usable = usable.tail(window_rows)
    columns = list(getattr(model, "feature_names_in_", []))
    if not columns:
        raise ValueError("model does not expose feature columns")
    missing = sorted(set(columns) - set(usable.columns))
    if missing:
        raise ValueError(f"evaluation data missing feature columns: {missing}")
    prediction = np.maximum(model.predict(usable[columns]), 0.0)
    benchmark = ewma_annualized_volatility(usable, horizon)
    return {"horizon": horizon, "rows": len(usable),
            "metrics": score_predictions(usable[target], prediction),
            "benchmark": {"name": "ewma", "decay": 0.96,
                           "metrics": score_predictions(usable[target], benchmark)}}


def retrain_decision(current: dict, champion: dict, *, degradation_threshold: float = .05,
                     benchmark_margin: float = .02, consecutive_failures: int = 2,
                     prior_failures: int = 0,
                     last_triggered_at: str | None = None, cooldown_hours: float = 6) -> dict:
    current_metrics, champion_metrics = current["metrics"], champion["metrics"]
    metric = "qlike" if "qlike" in current_metrics and "qlike" in champion_metrics else "rmse"
    qlike_change = (current_metrics[metric] - champion_metrics[metric]) / max(abs(champion_metrics[metric]), 1e-12)
    benchmark = current.get("benchmark", {}).get("metrics", {}).get("qlike")
    benchmark_gap = ((current_metrics["qlike"] - benchmark) / max(abs(benchmark), 1e-12)
                     if benchmark is not None else None)
    degraded = qlike_change >= degradation_threshold
    failures = prior_failures + 1 if degraded else 0
    cooldown = False
    if last_triggered_at:
        then = datetime.fromisoformat(last_triggered_at)
        cooldown = (datetime.now(timezone.utc) - then).total_seconds() < cooldown_hours * 3600
    beats_benchmark = benchmark_gap is not None and benchmark_gap <= -benchmark_margin
    return {"degraded": degraded, "consecutive_failures": failures,
            "qlike_change": qlike_change, "benchmark_gap": benchmark_gap,
            "beats_benchmark": beats_benchmark,
            "trigger_retraining": failures >= consecutive_failures and not cooldown,
            "cooldown_active": cooldown}
