from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
import time
from typing import Any

CANDLE_INTERVAL_MS = 10_000


def _dec(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite() or parsed < 0:
        raise ValueError("numeric value must be finite and non-negative")
    return parsed


def _bucket(ts_ms: int) -> int:
    return (int(ts_ms) // CANDLE_INTERVAL_MS) * CANDLE_INTERVAL_MS


@dataclass(slots=True)
class VenueBook:
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]
    received_ts_ms: int


class MarketAggregator:
    """Pure state machine for venue books and trade-derived market data."""

    def __init__(self, price_tick: str | None = None, depth: int = 10, freshness_ms: int = 5000) -> None:
        self.tick = _dec(price_tick) if price_tick else None
        if self.tick is not None and self.tick <= 0:
            raise ValueError("price_tick must be positive")
        self.depth = depth
        self.freshness_ms = freshness_ms
        self.books: dict[str, VenueBook] = {}
        self.trade_buckets: dict[int, dict[str, Any]] = {}
        self.latest_trades: dict[str, dict[str, Any]] = {}
        self.cvd = Decimal("0")
        self._seen_events: set[str] = set()

    def apply_book(self, event: dict[str, Any], published_ts_ms: int | None = None) -> dict[str, Any]:
        self._remember_event(event)
        venue = str(event["venue"]).lower()
        # Use Redis publication time for freshness. The venue receive time is
        # captured before the bounded ingestion queue and may be old by the
        # time this consumer sees the event.
        received = int(published_ts_ms or event.get("received_ts_ms") or time.time() * 1000)
        self.books[venue] = VenueBook(
            bids=self._levels(event.get("bids", []), reverse=True),
            asks=self._levels(event.get("asks", []), reverse=False),
            received_ts_ms=received,
        )
        return self.book_snapshot(int(time.time() * 1000), str(event.get("instrument", "BTCUSDT")).upper())

    def apply_trade(self, event: dict[str, Any]) -> dict[str, Any]:
        self._remember_event(event)
        venue = str(event["venue"]).lower()
        price, quantity = _dec(event["price"]), _dec(event["quantity"])
        ts = int(event.get("exchange_ts_ms") or event.get("received_ts_ms") or time.time() * 1000)
        received = int(event.get("received_ts_ms") or time.time() * 1000)
        bucket = _bucket(ts)
        state = self.trade_buckets.setdefault(bucket, {"notional": Decimal("0"), "volume": Decimal("0"), "price_sum": Decimal("0"), "trade_count": 0, "open": None, "high": None, "low": None, "close": None, "delta": Decimal("0"), "venues": defaultdict(lambda: {"notional": Decimal("0"), "volume": Decimal("0"), "price_sum": Decimal("0"), "trade_count": 0})})
        state["notional"] += price * quantity
        state["volume"] += quantity
        state["price_sum"] += price
        state["trade_count"] += 1
        state["open"] = price if state["open"] is None else state["open"]
        state["high"] = price if state["high"] is None else max(state["high"], price)
        state["low"] = price if state["low"] is None else min(state["low"], price)
        state["close"] = price
        state["delta"] += quantity if event.get("taker_side") == "buy" else -quantity
        venue_state = state["venues"][venue]
        venue_state["notional"] += price * quantity
        venue_state["volume"] += quantity
        venue_state["price_sum"] += price
        venue_state["trade_count"] += 1
        self.latest_trades[venue] = {"price": price, "received_ts_ms": received}
        self.cvd += quantity if event.get("taker_side") == "buy" else -quantity
        self._trim_buckets(bucket)
        return self.spot_snapshot(bucket, str(event.get("instrument", "BTCUSDT")).upper(), state)

    def book_snapshot(self, now_ms: int, instrument: str) -> dict[str, Any]:
        active = {v: b for v, b in self.books.items() if now_ms - b.received_ts_ms <= self.freshness_ms}
        stale = sorted(set(self.books) - set(active))
        tick = self._resolve_tick(active)
        bid_buckets = self._aggregate_side(active, "bids", tick, reverse=True, rounding=ROUND_DOWN)
        ask_buckets = self._aggregate_side(active, "asks", tick, reverse=False, rounding=ROUND_CEILING)

        # A consolidated cross-venue book can contain crossed quotes when venue
        # mid-prices differ. Use the tightest internally valid venue as the
        # reference and retain only aggregate levels on its valid sides. This
        # keeps both sides populated instead of dropping every ask or bid.
        reference = min(
            (
                book
                for book in active.values()
                if book.bids and book.asks and book.asks[0][0] > book.bids[0][0]
            ),
            key=lambda book: book.asks[0][0] - book.bids[0][0],
            default=None,
        )
        if reference:
            reference_bid = (reference.bids[0][0] / tick).to_integral_value(rounding=ROUND_DOWN) * tick
            reference_ask = (reference.asks[0][0] / tick).to_integral_value(rounding=ROUND_CEILING) * tick
            bid_buckets = [level for level in bid_buckets if Decimal(level["price"]) <= reference_bid]
            ask_buckets = [level for level in ask_buckets if Decimal(level["price"]) >= reference_ask]
        elif bid_buckets:
            highest_bid = Decimal(bid_buckets[0]["price"])
            ask_buckets = [level for level in ask_buckets if Decimal(level["price"]) > highest_bid]
        return {"schema_version": 1, "event_type": "aggregated_book", "instrument": instrument, "generated_ts_ms": now_ms, "depth": self.depth, "price_tick": str(tick), "bucket_method": "bid_floor_ask_ceiling", "venues": sorted(active), "stale_venues": stale, "bids": bid_buckets[: self.depth], "asks": ask_buckets[: self.depth]}

    def spot_snapshot(self, bucket: int, instrument: str, state: dict[str, Any]) -> dict[str, Any]:
        venues = {venue: {"average_price": self._fmt(v["price_sum"] / v["trade_count"]), "volume": self._fmt(v["volume"]), "last_received_ts_ms": self.latest_trades[venue]["received_ts_ms"]} for venue, v in state["venues"].items() if v["trade_count"] > 0}
        total = state["volume"]
        price = self._fmt(state["price_sum"] / state["trade_count"]) if state["trade_count"] else None
        return {"schema_version": 1, "event_type": "aggregated_spot", "instrument": instrument, "price": price, "method": "ten_second_trade_average", "generated_ts_ms": int(time.time() * 1000), "bucket_start_ts_ms": bucket, "bucket_end_ts_ms": bucket + CANDLE_INTERVAL_MS, "total_volume": self._fmt(total), "venues": venues, "used_venues": sorted(venues), "stale_venues": []}

    def candle_snapshot(self, instrument: str) -> list[dict[str, Any]]:
        return [{"instrument": instrument, "bucket_start_ts_ms": start, "open": self._fmt(s["open"]), "high": self._fmt(s["high"]), "low": self._fmt(s["low"]), "close": self._fmt(s["close"]), "volume": self._fmt(s["volume"]), "vwap": self._fmt(s["notional"] / s["volume"])} for start, s in sorted(self.trade_buckets.items()) if s["volume"] > 0]

    def cvd_snapshot(self, instrument: str) -> list[dict[str, Any]]:
        cumulative = Decimal("0")
        rows = []
        for start, state in sorted(self.trade_buckets.items()):
            if state["volume"] <= 0:
                continue
            cumulative += state["delta"]
            rows.append({"instrument": instrument, "bucket_start_ts_ms": start, "delta": self._fmt(state["delta"]), "cvd": self._fmt(cumulative)})
        return rows

    def _levels(self, raw: list[dict[str, Any]], reverse: bool) -> list[tuple[Decimal, Decimal]]:
        levels = [(_dec(level["price"]), _dec(level["quantity"])) for level in raw[:15]]
        return sorted(levels, key=lambda x: x[0], reverse=reverse)

    def _resolve_tick(self, books: dict[str, VenueBook]) -> Decimal:
        if self.tick is not None:
            return self.tick
        precision = max(
            (-price.as_tuple().exponent)
            for book in books.values()
            for price, _ in (*book.bids, *book.asks)
        ) if books else 0
        return Decimal(1).scaleb(-precision)

    def _aggregate_side(self, books: dict[str, VenueBook], side: str, tick: Decimal, reverse: bool, rounding: str) -> list[dict[str, Any]]:
        grouped: dict[Decimal, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
        for venue, book in books.items():
            for price, quantity in getattr(book, side):
                bucket = (price / tick).to_integral_value(rounding=rounding) * tick
                grouped[bucket][venue] += quantity
        result = []
        for price in sorted(grouped, reverse=reverse)[: self.depth]:
            contributions = {venue: self._fmt(qty) for venue, qty in sorted(grouped[price].items())}
            result.append({"price": self._fmt(price), "total_quantity": self._fmt(sum(grouped[price].values(), Decimal("0"))), "venues": contributions})
        return result

    def _trim_buckets(self, current: int) -> None:
        for start in list(self.trade_buckets):
            if start < current - 60 * 60 * 1000:
                del self.trade_buckets[start]

    def _remember_event(self, event: dict[str, Any]) -> None:
        event_id = event.get("event_id")
        if event_id is None:
            return
        if event_id in self._seen_events:
            raise ValueError(f"duplicate event_id {event_id}")
        self._seen_events.add(str(event_id))
        if len(self._seen_events) > 100_000:
            self._seen_events.clear()

    @staticmethod
    def _fmt(value: Decimal | None) -> str | None:
        return format(value, "f") if value is not None else None
