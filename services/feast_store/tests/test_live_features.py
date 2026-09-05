from datetime import datetime, timezone

import pytest

from jobs.live_features import payload_to_frame


def test_payload_to_frame_matches_feature_view_schema():
    frame = payload_to_frame({
        "asset": "BTCUSD",
        "event_timestamp": "2026-09-05T00:00:00+00:00",
        "created_timestamp": "2026-09-05T00:00:01+00:00",
        "synthetic_price": "100.5",
        "log_return": "0.001",
        "venue_count": 6,
    })
    assert list(frame.columns) == [
        "asset", "event_timestamp", "created_timestamp", "synthetic_price",
        "log_return", "venue_count", "realized_vol_1h", "realized_vol_3h",
    ]
    assert frame.iloc[0]["asset"] == "BTCUSD"
    assert frame.iloc[0]["venue_count"] == 6
    assert frame.iloc[0]["event_timestamp"].tzinfo is not None


def test_payload_requires_entity_and_timestamp():
    with pytest.raises(ValueError, match="missing fields"):
        payload_to_frame({"synthetic_price": 100})
