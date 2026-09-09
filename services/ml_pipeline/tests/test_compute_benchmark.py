from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.compute_benchmark import HORIZONS, compute_benchmark


def test_bootstrap_report_covers_pipeline_horizons_in_champion_format() -> None:
    count = 200
    table = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-09-01", periods=count, freq="10s", tz="UTC"),
            "frequency": ["10s"] * count,
            "log_return": np.linspace(-0.002, 0.002, count),
            "venue_count": [6] * count,
            **{
                f"target_rv_{horizon}": np.linspace(0.2, 0.3, count)
                for horizon in ("5m", "15m", "30m", "1h")
            },
        }
    )

    report = compute_benchmark(table)

    assert HORIZONS == ("5m", "15m", "30m", "1h")
    assert set(report) == {"_metadata", *HORIZONS}
    for horizon in HORIZONS:
        assert report[horizon]["rows"] == 30
        assert set(report[horizon]["metrics"]) == {"qlike", "rmse", "mae"}
