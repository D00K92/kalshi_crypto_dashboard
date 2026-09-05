"""Shared model training and scoring logic for local jobs."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

NON_FEATURE_COLUMNS = {"timestamp", "frequency", "asset", "venue", "synthetic_price"}


def train_horizon(table: pd.DataFrame, horizon: str, seed: int = 42) -> tuple[XGBRegressor, dict]:
    '''
    geasfasdf
    
    '''
    target = f"target_rv_{horizon}"
    if target not in table:
        raise ValueError(f"missing target column: {target}")
    
    
    usable = table.dropna(subset=[target]).sort_values("timestamp").reset_index(drop=True)
    columns = [c for c in usable.select_dtypes(include=np.number).columns
               if c not in NON_FEATURE_COLUMNS and not c.startswith("target_")]
    
    n = len(usable)
    train_end, valid_end = int(n * .70), int(n * .85)
    
    if not columns or train_end < 2 or valid_end <= train_end or n <= valid_end:
        raise ValueError("not enough rows for chronological train/validation/test split")
    
    # divide X, y
    X, y = usable[columns], usable[target]
    
    
    # Set up XGBoost and fit data
    model = XGBRegressor(n_estimators=500, max_depth=6, learning_rate=.05, subsample=.8,
                         colsample_bytree=.8, objective="reg:squarederror", eval_metric="rmse",
                         random_state=seed, n_jobs=-1)
    model.fit(X.iloc[:train_end], y.iloc[:train_end],
              eval_set=[(X.iloc[train_end:valid_end], y.iloc[train_end:valid_end])], verbose=False)
    
    # make prediction
    prediction = np.maximum(model.predict(X.iloc[valid_end:]), 0.0)

    # target     
    actual = y.iloc[valid_end:].to_numpy()

    # convert vols to variance
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
