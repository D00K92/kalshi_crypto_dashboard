"""Rolling evaluation and retraining decision policy."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error


def score_predictions(actual: pd.Series, prediction: np.ndarray) -> dict[str, float]:
    prediction = np.maximum(np.asarray(prediction, dtype=float), 0.0)
    return {
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
    return {"horizon": horizon, "rows": len(usable),
            "metrics": score_predictions(usable[target], model.predict(usable[columns]))}


def retrain_decision(current: dict, champion: dict, *, threshold: float = .15,
                     consecutive_failures: int = 2, prior_failures: int = 0,
                     last_triggered_at: str | None = None, cooldown_hours: float = 6) -> dict:
    current_metrics, champion_metrics = current["metrics"], champion["metrics"]
    rmse_change = (current_metrics["rmse"] - champion_metrics["rmse"]) / max(champion_metrics["rmse"], 1e-12)
    mae_change = (current_metrics["mae"] - champion_metrics["mae"]) / max(champion_metrics["mae"], 1e-12)
    degraded = rmse_change >= threshold or mae_change >= threshold
    failures = prior_failures + 1 if degraded else 0
    cooldown = False
    if last_triggered_at:
        then = datetime.fromisoformat(last_triggered_at)
        cooldown = (datetime.now(timezone.utc) - then).total_seconds() < cooldown_hours * 3600
    return {"degraded": degraded, "consecutive_failures": failures,
            "rmse_change": rmse_change, "mae_change": mae_change,
            "trigger_retraining": failures >= consecutive_failures and not cooldown,
            "cooldown_active": cooldown}
