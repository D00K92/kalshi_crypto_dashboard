from datetime import datetime, timezone

import pytest

from ingestion.kalshi_discovery import KalshiDiscoveryError, choose_event


NOW = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)


def test_choose_event_prefers_nearest_hourly_event() -> None:
    events = [
        {"event_ticker": "KXBTCD-DAILY", "strike_date": "2026-08-29T18:00:00Z", "status": "open"},
        {"event_ticker": "KXBTCD-HOURLY", "strike_date": "2026-08-29T13:00:00Z", "status": "open"},
    ]
    assert choose_event(events, now=NOW)["event_ticker"] == "KXBTCD-HOURLY"


def test_choose_event_ignores_closed_and_past_events() -> None:
    with pytest.raises(KalshiDiscoveryError):
        choose_event([
            {"event_ticker": "old", "strike_date": "2026-08-29T11:00:00Z", "status": "open"},
            {"event_ticker": "closed", "strike_date": "2026-08-29T13:00:00Z", "status": "closed"},
        ], now=NOW)
