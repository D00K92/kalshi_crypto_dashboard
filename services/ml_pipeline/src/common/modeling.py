"""Shared model training and scoring logic for local jobs."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

LIVE_FEATURE_COLUMNS = ("log_return", "venue_count")


def train_horizon(table: pd.DataFrame, horizon: str, seed: int = 42) -> tuple[XGBRegressor, dict]:
    """Train against the exact feature contract available to live analytics."""
    target = f"target_rv_{horizon}"
    if target not in table:
        raise ValueError(f"missing target column: {target}")

    missing_features = [column for column in LIVE_FEATURE_COLUMNS if column not in table]
    if missing_features:
        raise ValueError(f"training data missing live features: {missing_features}")
    columns = list(LIVE_FEATURE_COLUMNS)
    usable = table.dropna(subset=[target, *columns]).sort_values("timestamp").reset_index(drop=True)

    n = len(usable)
    train_end, valid_end = int(n * .70), int(n * .85)
    
    if not columns or train_end < 2 or valid_end <= train_end or n <= valid_end:
        raise ValueError("not enough rows for chronological train/validation/test split")
    
    X, y = usable[columns], usable[target]

    model = XGBRegressor(n_estimators=500, max_depth=6, learning_rate=.05, subsample=.8,
                         colsample_bytree=.8, objective="reg:squarederror", eval_metric="rmse",
                         random_state=seed, n_jobs=-1)
    model.fit(X.iloc[:train_end], y.iloc[:train_end],
              eval_set=[(X.iloc[train_end:valid_end], y.iloc[train_end:valid_end])], verbose=False)
    
    prediction = np.maximum(model.predict(X.iloc[valid_end:]), 0.0)
    actual = y.iloc[valid_end:].to_numpy()

    forecast_variance = np.maximum(prediction * prediction, 1e-18)
    actual_variance = np.maximum(actual * actual, 1e-18)
    
    metadata = {
        "horizon": horizon, "target": target, "feature_columns": columns,
        "rows": {"total": n, "train": train_end, "validation": valid_end - train_end,
                 "test": n - valid_end}, "prediction_floor": 0.0,
        "metrics": {
            "qlike": float(np.mean(np.log(forecast_variance) + actual_variance / forecast_variance)),
            "rmse": float(np.sqrt(mean_squared_error(y.iloc[valid_end:], prediction))),
            "mae": float(mean_absolute_error(y.iloc[valid_end:], prediction)),
            "r2": float(r2_score(y.iloc[valid_end:], prediction)),
        },
    }
    return model, metadata
