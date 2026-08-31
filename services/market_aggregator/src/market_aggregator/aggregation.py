from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
import time
from typing import Any, Mapping

CANDLE_INTERVAL_MS = 10_000


def _dec(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite() or parsed < 0:
        raise ValueError("numeric value must be finite and non-negative")
    return parsed


def _signed_dec(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError("numeric value must be finite")
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

    def __init__(self, price_tick: str | None = None, depth: int = 10, freshness_ms: int = 500, venues: tuple[str, ...] | None = None, taker_fees: Mapping[str, str | Decimal] | None = None) -> None:
        self.tick = _dec(price_tick) if price_tick else None
        if self.tick is not None and self.tick <= 0:
            raise ValueError("price_tick must be positive")
        self.depth = depth
        self.freshness_ms = freshness_ms
        self.venues = {venue.lower() for venue in venues} if venues is not None else None
        self.taker_fees = {str(venue).lower(): self._fee(value) for venue, value in (taker_fees or {}).items()}
        self.books: dict[str, VenueBook] = {}
        self.trade_buckets: dict[int, dict[str, Any]] = {}
        self.latest_trades: dict[str, dict[str, Any]] = {}
        self.cvd = Decimal("0")
        self._seen_events: set[str] = set()

    def apply_book(self, event: dict[str, Any], published_ts_ms: int | None = None) -> dict[str, Any] | None:
        venue = str(event["venue"]).lower()
        if self.venues is not None and venue not in self.venues:
            return None
        self._remember_event(event)
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

    def apply_trade(self, event: dict[str, Any]) -> dict[str, Any] | None:
        venue = str(event["venue"]).lower()
        if self.venues is not None and venue not in self.venues:
            return None
        self._remember_event(event)
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

        bid_buckets, ask_buckets = self._uncross(bid_buckets, ask_buckets)
        return {"schema_version": 1, "event_type": "aggregated_book", "instrument": instrument, "generated_ts_ms": now_ms, "depth": self.depth, "price_tick": str(tick), "bucket_method": "effective_price_bid_floor_ask_ceiling_uncrossed", "taker_fees": {venue: self._fmt(fee) for venue, fee in sorted(self.taker_fees.items())}, "venues": sorted(active), "stale_venues": stale, "bids": bid_buckets[: self.depth], "asks": ask_buckets[: self.depth]}

    def spot_snapshot(self, bucket: int, instrument: str, state: dict[str, Any]) -> dict[str, Any]:
        venues = {venue: {"average_price": self._fmt(v["price_sum"] / v["trade_count"]), "volume": self._fmt(v["volume"]), "last_received_ts_ms": self.latest_trades[venue]["received_ts_ms"]} for venue, v in state["venues"].items() if v["trade_count"] > 0}
        total = state["volume"]
        price = self._fmt(state["price_sum"] / state["trade_count"]) if state["trade_count"] else None
        return {"schema_version": 1, "event_type": "aggregated_spot", "instrument": instrument, "price": price, "method": "ten_second_trade_average", "generated_ts_ms": int(time.time() * 1000), "bucket_start_ts_ms": bucket, "bucket_end_ts_ms": bucket + CANDLE_INTERVAL_MS, "total_volume": self._fmt(total), "venues": venues, "used_venues": sorted(venues), "stale_venues": []}

    def candle_snapshot(self, instrument: str) -> list[dict[str, Any]]:
        return [{"instrument": instrument, "bucket_start_ts_ms": start, "open": self._fmt(s["open"]), "high": self._fmt(s["high"]), "low": self._fmt(s["low"]), "close": self._fmt(s["close"]), "volume": self._fmt(s["volume"]), "vwap": self._fmt(s["notional"] / s["volume"])} for start, s in sorted(self.trade_buckets.items()) if s["volume"] > 0]

    def export_candle_state(self) -> dict[str, Any]:
        """Return the complete candle state in JSON-safe Decimal form."""
        buckets = []
        for start, state in sorted(self.trade_buckets.items()):
            buckets.append({
                "start": start,
                "notional": self._fmt(state["notional"]),
                "volume": self._fmt(state["volume"]),
                "price_sum": self._fmt(state["price_sum"]),
                "trade_count": state["trade_count"],
                "open": self._fmt(state["open"]),
                "high": self._fmt(state["high"]),
                "low": self._fmt(state["low"]),
                "close": self._fmt(state["close"]),
                "delta": self._fmt(state["delta"]),
                "venues": {
                    venue: {
                        "notional": self._fmt(values["notional"]),
                        "volume": self._fmt(values["volume"]),
                        "price_sum": self._fmt(values["price_sum"]),
                        "trade_count": values["trade_count"],
                    }
                    for venue, values in sorted(state["venues"].items())
                },
            })
        return {"schema_version": 1, "interval_ms": CANDLE_INTERVAL_MS, "buckets": buckets}

    def restore_candle_state(self, payload: dict[str, Any]) -> int:
        """Restore persisted buckets and return the number loaded."""
        if payload.get("schema_version") != 1 or payload.get("interval_ms") != CANDLE_INTERVAL_MS:
            raise ValueError("unsupported candle state")
        restored: dict[int, dict[str, Any]] = {}
        for raw in payload.get("buckets", []):
            start = int(raw["start"])
            state = {
                "notional": _dec(raw["notional"]), "volume": _dec(raw["volume"]),
                "price_sum": _dec(raw["price_sum"]), "trade_count": int(raw["trade_count"]),
                "open": _dec(raw["open"]) if raw.get("open") is not None else None,
                "high": _dec(raw["high"]) if raw.get("high") is not None else None,
                "low": _dec(raw["low"]) if raw.get("low") is not None else None,
                "close": _dec(raw["close"]) if raw.get("close") is not None else None,
                "delta": _signed_dec(raw["delta"]) if raw.get("delta") is not None else Decimal("0"),
                "venues": defaultdict(lambda: {"notional": Decimal("0"), "volume": Decimal("0"), "price_sum": Decimal("0"), "trade_count": 0}),
            }
            for venue, values in raw.get("venues", {}).items():
                state["venues"][venue] = {
                    "notional": _dec(values["notional"]), "volume": _dec(values["volume"]),
                    "price_sum": _dec(values["price_sum"]), "trade_count": int(values["trade_count"]),
                }
            restored[start] = state
        self.trade_buckets = restored
        self.cvd = sum((state["delta"] for state in restored.values()), Decimal("0"))
        return len(restored)

    def restore_candle_snapshot(self, rows: list[dict[str, Any]]) -> int:
        """Migrate the public candle format when no full state key exists."""
        payload = {"schema_version": 1, "interval_ms": CANDLE_INTERVAL_MS, "buckets": []}
        for row in rows:
            volume = _dec(row["volume"])
            vwap = _dec(row["vwap"])
            payload["buckets"].append({
                "start": int(row["bucket_start_ts_ms"]), "notional": self._fmt(vwap * volume),
                "volume": self._fmt(volume), "price_sum": self._fmt(vwap), "trade_count": 1,
                "open": row["open"], "high": row["high"], "low": row["low"], "close": row["close"],
                "delta": "0", "venues": {},
            })
        return self.restore_candle_state(payload)

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
        levels = [(_dec(level["price"]), _dec(level["quantity"])) for level in raw]
        return sorted(levels, key=lambda x: x[0], reverse=reverse)[:15]

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
                effective = self._effective_price(price, venue, side == "asks")
                bucket = (effective / tick).to_integral_value(rounding=rounding) * tick
                grouped[bucket][venue] += quantity
        result = []
        for price in sorted(grouped, reverse=reverse):
            contributions = {venue: self._fmt(qty) for venue, qty in sorted(grouped[price].items())}
            result.append({"price": self._fmt(price), "total_quantity": self._fmt(sum(grouped[price].values(), Decimal("0"))), "venues": contributions})
        return result

    def _uncross(self, bids: list[dict[str, Any]], asks: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Net crossed composite liquidity until best bid is below best ask."""
        bid_index = ask_index = 0
        while bid_index < len(bids) and ask_index < len(asks):
            bid, ask = bids[bid_index], asks[ask_index]
            if Decimal(bid["price"]) < Decimal(ask["price"]):
                break
            matched = min(Decimal(bid["total_quantity"]), Decimal(ask["total_quantity"]))
            self._reduce_level(bid, matched)
            self._reduce_level(ask, matched)
            if Decimal(bid["total_quantity"]) == 0:
                bid_index += 1
            if Decimal(ask["total_quantity"]) == 0:
                ask_index += 1
        return ([level for level in bids[bid_index:] if Decimal(level["total_quantity"]) > 0], [level for level in asks[ask_index:] if Decimal(level["total_quantity"]) > 0])

    def _reduce_level(self, level: dict[str, Any], quantity: Decimal) -> None:
        level["total_quantity"] = self._fmt(Decimal(level["total_quantity"]) - quantity)
        for venue in list(level["venues"]):
            available = Decimal(level["venues"][venue])
            consumed = min(available, quantity)
            quantity -= consumed
            available -= consumed
            if available == 0:
                del level["venues"][venue]
            else:
                level["venues"][venue] = self._fmt(available)
            if quantity == 0:
                break

    @staticmethod
    def _fee(value: str | Decimal) -> Decimal:
        fee = _dec(value)
        if fee >= 1:
            raise ValueError("taker fee must be less than 1")
        return fee

    def _effective_price(self, price: Decimal, venue: str, is_ask: bool) -> Decimal:
        fee = self.taker_fees.get(venue, Decimal("0"))
        return price * (Decimal("1") + fee if is_ask else Decimal("1") - fee)

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
