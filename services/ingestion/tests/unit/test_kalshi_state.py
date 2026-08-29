from dataclasses import replace

import pytest

from ingestion.kalshi_state import KalshiBookState, KalshiRolloverState, KalshiStateError
from ingestion.models import BookLevel, KalshiOrderBookSnapshot
from ingestion.kalshi_discovery import KalshiEventSet


def _event(kind: str, sequence: int, **kwargs):
    return KalshiOrderBookSnapshot(
        event_id=f"book:{sequence}", event_type=kind, venue="kalshi", instrument="BTCUSD",
        series_ticker="KXBTCD", event_ticker="KXBTCD-TEST", market_ticker="KXBTCD-TEST-1",
        sequence=sequence, yes_bids=kwargs.get("yes", ()), no_bids=kwargs.get("no", ()),
        exchange_ts_ms=1700000000000, received_ts_ms=1700000000123,
        delta_price_dollars=kwargs.get("price"), delta_fp=kwargs.get("delta"), delta_side=kwargs.get("side"),
    )


def test_book_snapshot_then_delta_updates_and_removes_levels() -> None:
    book = KalshiBookState()
    book.apply(_event("kalshi_orderbook_snapshot", 4, yes=(BookLevel("0.42", "10"),), no=(BookLevel("0.58", "3"),)))
    book.apply(_event("kalshi_orderbook_delta", 5, price="0.42", delta="-10", side="yes"))
    assert book.levels() == ((), (("0.58", "3"),))


def test_book_rejects_sequence_gap() -> None:
    book = KalshiBookState()
    book.apply(_event("kalshi_orderbook_snapshot", 4))
    with pytest.raises(KalshiStateError, match="sequence gap"):
        book.apply(_event("kalshi_orderbook_delta", 6, price="0.42", delta="1", side="yes"))


def test_rollover_keeps_old_markets_until_confirmation() -> None:
    old = KalshiEventSet("KXBTCD", "old", ("old-1",), {}, ({},))
    new = KalshiEventSet("KXBTCD", "new", ("new-1", "new-2"), {}, ({}, {}))
    state = KalshiRolloverState(active=old)
    assert state.stage(new) is True
    assert state.active.event_ticker == "old"
    assert state.confirm("new") == ("old-1",)
    assert state.active.event_ticker == "new"
    assert state.generation == 1


def test_rollover_rejects_wrong_confirmation() -> None:
    state = KalshiRolloverState()
    with pytest.raises(KalshiStateError, match="un-staged"):
        state.confirm("unknown")
