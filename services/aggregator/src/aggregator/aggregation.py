from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
import time
from typing import Any, Mapping

# Ten seconds is the canonical live primitive interval.  Longer market views
# (including the dashboard's 30-second candle feed) are derived from these
# completed buckets, never the other way around.
CANDLE_INTERVAL_MS = 10_000
DEFAULT_BAR_FREQUENCIES_MS = {
    "10s": CANDLE_INTERVAL_MS,
}


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

    def __init__(self, price_tick: str | None = None, depth: int = 10, freshness_ms: int = 500, venues: tuple[str, ...] | None = None, taker_fees: Mapping[str, str | Decimal] | None = None, trade_freshness_ms: int = 60_000, history_ms: int = 2 * 60 * 60 * 1000) -> None:
        self.tick = _dec(price_tick) if price_tick else None
        if self.tick is not None and self.tick <= 0:
            raise ValueError("price_tick must be positive")
        self.depth = depth
        self.freshness_ms = freshness_ms
        self.trade_freshness_ms = trade_freshness_ms
        self.history_ms = history_ms
        self.venues = {venue.lower() for venue in venues} if venues is not None else None
        self.taker_fees = {str(venue).lower(): self._fee(value) for venue, value in (taker_fees or {}).items()}
        self.books: dict[str, VenueBook] = {}
        self.book_buckets: dict[int, dict[str, VenueBook]] = {}
        self.trade_buckets: dict[int, dict[str, Any]] = {}
        self.latest_trades: dict[str, dict[str, Any]] = {}
        self._last_trade_ts_ms: dict[str, int] = {}
        self._last_synthetic_price: Decimal | None = None
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
        book_bucket = _bucket(int(event.get("exchange_ts_ms") or received))
        self.book_buckets.setdefault(book_bucket, {})[venue] = self.books[venue]
        self._trim_buckets(book_bucket)
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
        state = self.trade_buckets.setdefault(bucket, {"notional": Decimal("0"), "volume": Decimal("0"), "buy_volume": Decimal("0"), "sell_volume": Decimal("0"), "price_sum": Decimal("0"), "trade_count": 0, "open": None, "high": None, "low": None, "close": None, "delta": Decimal("0"), "fill_intervals": [], "venues": defaultdict(lambda: {"notional": Decimal("0"), "volume": Decimal("0"), "buy_volume": Decimal("0"), "sell_volume": Decimal("0"), "price_sum": Decimal("0"), "trade_count": 0, "open": None, "high": None, "low": None, "close": None, "fill_intervals": []})})
        state["notional"] += price * quantity
        state["volume"] += quantity
        if event.get("taker_side") == "buy":
            state["buy_volume"] += quantity
        elif event.get("taker_side") == "sell":
            state["sell_volume"] += quantity
        state["price_sum"] += price
        state["trade_count"] += 1
        state["open"] = price if state["open"] is None else state["open"]
        state["high"] = price if state["high"] is None else max(state["high"], price)
        state["low"] = price if state["low"] is None else min(state["low"], price)
        state["close"] = price
        state["delta"] += quantity if event.get("taker_side") == "buy" else -quantity
        previous_trade_ts = self._last_trade_ts_ms.get(venue)
        if previous_trade_ts is not None and ts >= previous_trade_ts:
            interval = Decimal(ts - previous_trade_ts)
            state["fill_intervals"].append(interval)
        self._last_trade_ts_ms[venue] = ts
        venue_state = state["venues"][venue]
        venue_state["notional"] += price * quantity
        venue_state["volume"] += quantity
        if event.get("taker_side") == "buy":
            venue_state["buy_volume"] += quantity
        elif event.get("taker_side") == "sell":
            venue_state["sell_volume"] += quantity
        venue_state["price_sum"] += price
        venue_state["trade_count"] += 1
        venue_state["open"] = price if venue_state["open"] is None else venue_state["open"]
        venue_state["high"] = price if venue_state["high"] is None else max(venue_state["high"], price)
        venue_state["low"] = price if venue_state["low"] is None else min(venue_state["low"], price)
        venue_state["close"] = price
        if previous_trade_ts is not None and ts >= previous_trade_ts:
            venue_state["fill_intervals"].append(Decimal(ts - previous_trade_ts))
        self.latest_trades[venue] = {"price": price, "received_ts_ms": received}
        self._trim_buckets(bucket)
        return self.spot_snapshot(bucket, str(event.get("instrument", "BTCUSDT")).upper(), state)

    def book_snapshot(self, now_ms: int, instrument: str) -> dict[str, Any]:
        active = {v: b for v, b in self.books.items() if now_ms - b.received_ts_ms <= self.freshness_ms}
        stale = sorted(set(self.books) - set(active))
        tick = self._resolve_tick(active)
        bid_buckets = self._aggregate_side(active, "bids", tick, reverse=True, rounding=ROUND_DOWN)
        ask_buckets = self._aggregate_side(active, "asks", tick, reverse=False, rounding=ROUND_CEILING)

        bid_buckets, ask_buckets = self._uncross(bid_buckets, ask_buckets)
        bids, asks = bid_buckets[: self.depth], ask_buckets[: self.depth]
        best_bid = Decimal(bids[0]["price"]) if bids else None
        best_ask = Decimal(asks[0]["price"]) if asks else None
        mid = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
        spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
        bid_depth = sum((Decimal(level["total_quantity"]) for level in bids), Decimal("0"))
        ask_depth = sum((Decimal(level["total_quantity"]) for level in asks), Decimal("0"))
        imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth) if bid_depth + ask_depth else None
        return {"schema_version": 1, "event_type": "aggregated_book", "instrument": instrument, "generated_ts_ms": now_ms, "depth": self.depth, "price_tick": str(tick), "bucket_method": "effective_price_bid_floor_ask_ceiling_uncrossed", "taker_fees": {venue: self._fmt(fee) for venue, fee in sorted(self.taker_fees.items())}, "venues": sorted(active), "stale_venues": stale, "bids": bids, "asks": asks, "best_bid": self._fmt(best_bid), "best_ask": self._fmt(best_ask), "mid_price": self._fmt(mid), "spread": self._fmt(spread), "bid_depth": self._fmt(bid_depth), "ask_depth": self._fmt(ask_depth), "imbalance": self._fmt(imbalance)}

    def primitive_book_snapshot(self, event: dict[str, Any], generated_ts_ms: int) -> dict[str, Any]:
        """Return one venue's canonical book state without cross-venue aggregation."""
        venue = str(event["venue"]).lower()
        instrument = str(event.get("instrument", "BTCUSDT")).upper()
        bids = self._levels(event.get("bids", []), reverse=True)[: self.depth]
        asks = self._levels(event.get("asks", []), reverse=False)[: self.depth]
        return {
            "schema_version": 1,
            "primitive_schema_version": 2,
            "event_type": "market_book",
            "instrument": instrument,
            "venue": venue,
            "generated_ts_ms": generated_ts_ms,
            "exchange_ts_ms": event.get("exchange_ts_ms"),
            "depth": self.depth,
            "bids": [{"price": self._fmt(price), "quantity": self._fmt(quantity)} for price, quantity in bids],
            "asks": [{"price": self._fmt(price), "quantity": self._fmt(quantity)} for price, quantity in asks],
        }

    def spot_snapshot(self, bucket: int, instrument: str, state: dict[str, Any]) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        venues = {
            venue: {
                "price": self._fmt(item["price"]),
                # Keep the dashboard's historical field name while exposing
                # the latest price used by the synthetic average.
                "average_price": self._fmt(item["price"]),
                "volume": self._fmt(state["venues"].get(venue, {}).get("volume", Decimal("0"))),
                "last_received_ts_ms": item["received_ts_ms"],
            }
            for venue, item in self.latest_trades.items()
            if now_ms - item["received_ts_ms"] <= self.trade_freshness_ms
        }
        prices = [Decimal(item["price"]) for item in venues.values()]
        total = state["volume"]
        synthetic = sum(prices, Decimal("0")) / len(prices) if prices else None
        previous = self._last_synthetic_price
        log_return = (synthetic / previous).ln() if synthetic and previous else None
        if synthetic:
            self._last_synthetic_price = synthetic
        stale = sorted(set(self.latest_trades) - set(venues))
        return {
            "schema_version": 1, "event_type": "aggregated_spot", "instrument": instrument,
            "price": self._fmt(synthetic), "method": "simple_average_fresh_venues",
            "generated_ts_ms": now_ms, "bucket_start_ts_ms": bucket,
            "bucket_end_ts_ms": bucket + CANDLE_INTERVAL_MS, "total_volume": self._fmt(total),
            "venues": venues, "used_venues": sorted(venues), "stale_venues": stale,
            "log_return": self._fmt(log_return), "venue_count": len(prices),
        }

    def primitive_bars(self, instrument: str, frequencies_ms: Mapping[str, int] | None = None) -> list[dict[str, Any]]:
        """Return per-venue canonical bars with time-bucketed book state."""
        frequencies_ms = frequencies_ms or DEFAULT_BAR_FREQUENCIES_MS
        current_bucket = max(self.trade_buckets, default=0)
        venues = sorted({venue for state in self.trade_buckets.values() for venue in state["venues"]})
        output: list[dict[str, Any]] = []
        for frequency, interval in frequencies_ms.items():
            if not venues or not current_bucket:
                continue
            starts = range((min(self.trade_buckets) // interval) * interval, current_bucket, interval)
            for venue in venues:
                previous: dict[str, Any] | None = None
                for start in starts:
                    end = start + interval
                    if end > current_bucket:
                        break
                    parts = [state["venues"][venue] | {"start": bucket} for bucket, state in self.trade_buckets.items() if start <= bucket < end and venue in state["venues"]]
                    parts.sort(key=lambda part: part["start"])
                    # Candle state written before primitive_schema_version=2
                    # has no per-venue OHLC values. Do not let that legacy
                    # state crash the live consumer; newer buckets repopulate
                    # the complete primitive row naturally.
                    parts = [part for part in parts if all(part.get(key) is not None for key in ("open", "high", "low", "close"))]
                    if parts:
                        count = sum(part["trade_count"] for part in parts)
                        volume = sum((part["volume"] for part in parts), Decimal("0"))
                        buy = sum((part["buy_volume"] for part in parts), Decimal("0"))
                        sell = sum((part["sell_volume"] for part in parts), Decimal("0"))
                        prices = sum((part["price_sum"] for part in parts), Decimal("0"))
                        intervals = [value for part in parts for value in part["fill_intervals"]]
                        row = {
                            "p_open": parts[0]["open"], "p_high": max(part["high"] for part in parts), "p_low": min(part["low"] for part in parts), "p_trade": parts[-1]["close"], "p_close": parts[-1]["close"], "p_trade_mean": prices / count, "v_trade": volume, "v_buy": buy, "v_sell": sell, "cnt_trade": count,
                            "dt_fill_mean_ms": sum(intervals, Decimal("0")) / len(intervals) if intervals else None, "dt_fill_max_ms": max(intervals) if intervals else None, "dt_fill_min_ms": min(intervals) if intervals else None,
                        }
                        previous = row
                    elif previous is None:
                        continue
                    else:
                        row = {**previous, "v_trade": Decimal("0"), "v_buy": Decimal("0"), "v_sell": Decimal("0"), "cnt_trade": 0, "dt_fill_mean_ms": None, "dt_fill_max_ms": None, "dt_fill_min_ms": None}
                    book = self._book_at_or_before(venue, end)
                    payload = {key: self._fmt(value) if isinstance(value, Decimal) else value for key, value in row.items()}
                    payload.update({"schema_version": 1, "primitive_schema_version": 2, "event_type": "primitive_bar", "instrument": instrument, "venue": venue, "frequency": frequency, "interval_ms": interval, "bucket_start_ts_ms": start, "bucket_end_ts_ms": end})
                    if book:
                        for level in range(1, 11):
                            bid = book.bids[level - 1] if len(book.bids) >= level else (None, None)
                            ask = book.asks[level - 1] if len(book.asks) >= level else (None, None)
                            payload[f"p_bid_{level}"] = self._fmt(bid[0])
                            payload[f"q_bid_{level}"] = self._fmt(bid[1])
                            payload[f"p_ask_{level}"] = self._fmt(ask[0])
                            payload[f"q_ask_{level}"] = self._fmt(ask[1])
                        payload["book_available_ts_ms"] = book.received_ts_ms
                    else:
                        for level in range(1, 11):
                            payload.update({f"p_bid_{level}": None, f"q_bid_{level}": None, f"p_ask_{level}": None, f"q_ask_{level}": None})
                        payload["book_available_ts_ms"] = None
                    output.append(payload)
        return output

    def primitive_bars_for_bucket(self, instrument: str, start: int) -> list[dict[str, Any]]:
        """Build only one closed canonical 10s bucket (the live hot path)."""
        state = self.trade_buckets.get(start)
        if state is None:
            return []
        end = start + CANDLE_INTERVAL_MS
        output = []
        for venue in sorted(self.venues or set(self.latest_trades)):
            part = state["venues"].get(venue)
            if not part or not part.get("trade_count"):
                continue
            count = part["trade_count"]
            payload = {
                "schema_version": 1, "primitive_schema_version": 2, "event_type": "primitive_bar",
                "instrument": instrument, "venue": venue, "frequency": "10s", "interval_ms": CANDLE_INTERVAL_MS,
                "bucket_start_ts_ms": start, "bucket_end_ts_ms": end,
                "p_open": self._fmt(part["open"]), "p_high": self._fmt(part["high"]), "p_low": self._fmt(part["low"]),
                "p_trade": self._fmt(part["close"]), "p_close": self._fmt(part["close"]),
                "p_trade_mean": self._fmt(part["price_sum"] / count), "v_trade": self._fmt(part["volume"]),
                "v_buy": self._fmt(part["buy_volume"]), "v_sell": self._fmt(part["sell_volume"]), "cnt_trade": count,
            }
            intervals = part["fill_intervals"]
            payload.update({"dt_fill_mean_ms": self._fmt(sum(intervals, Decimal("0")) / len(intervals)) if intervals else None,
                            "dt_fill_max_ms": self._fmt(max(intervals)) if intervals else None,
                            "dt_fill_min_ms": self._fmt(min(intervals)) if intervals else None})
            book = self.books.get(venue)
            for level in range(1, 11):
                bid = book.bids[level - 1] if book and len(book.bids) >= level else (None, None)
                ask = book.asks[level - 1] if book and len(book.asks) >= level else (None, None)
                payload.update({f"p_bid_{level}": self._fmt(bid[0]), f"q_bid_{level}": self._fmt(bid[1]), f"p_ask_{level}": self._fmt(ask[0]), f"q_ask_{level}": self._fmt(ask[1])})
            payload["book_available_ts_ms"] = book.received_ts_ms if book else None
            output.append(payload)
        return output

    def _book_at_or_before(self, venue: str, end_ts_ms: int) -> VenueBook | None:
        candidates = [bucket for bucket, books in self.book_buckets.items() if bucket < end_ts_ms and venue in books]
        return self.book_buckets[max(candidates)][venue] if candidates else None

    def candle_snapshot(self, instrument: str, interval_ms: int = CANDLE_INTERVAL_MS) -> list[dict[str, Any]]:
        """Return dashboard candles resampled from canonical 10-second buckets."""
        if interval_ms <= 0 or interval_ms % CANDLE_INTERVAL_MS:
            raise ValueError("candle snapshot interval must be a multiple of 10 seconds")
        grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for start, state in self.trade_buckets.items():
            if state["volume"] > 0:
                grouped[(start // interval_ms) * interval_ms].append(state | {"start": start})
        candles = []
        for start, parts in sorted(grouped.items()):
            parts.sort(key=lambda part: part["start"])
            volume = sum((part["volume"] for part in parts), Decimal("0"))
            if not volume:
                continue
            candles.append({"instrument": instrument, "bucket_start_ts_ms": start,
                            "open": self._fmt(parts[0]["open"]),
                            "high": self._fmt(max(part["high"] for part in parts)),
                            "low": self._fmt(min(part["low"] for part in parts)),
                            "close": self._fmt(parts[-1]["close"]),
                            "volume": self._fmt(volume),
                            "vwap": self._fmt(sum((part["notional"] for part in parts), Decimal("0")) / volume)})
        return candles

    def resampled_bars(self, instrument: str, frequencies_ms: Mapping[str, int] | None = None) -> list[dict[str, Any]]:
        """Return completed multi-frequency bars from retained 10-second buckets."""
        frequencies_ms = frequencies_ms or DEFAULT_BAR_FREQUENCIES_MS
        bars: list[dict[str, Any]] = []
        current_bucket = max(self.trade_buckets, default=0)
        for frequency, interval in frequencies_ms.items():
            grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for start, state in self.trade_buckets.items():
                if state["volume"] <= 0:
                    continue
                grouped[(start // interval) * interval].append(state | {"start": start})
            for start, parts in sorted(grouped.items()):
                end = start + interval
                if end > current_bucket:
                    continue
                parts.sort(key=lambda part: part["start"])
                notional = sum((part["notional"] for part in parts), Decimal("0"))
                volume = sum((part["volume"] for part in parts), Decimal("0"))
                delta = sum((part["delta"] for part in parts), Decimal("0"))
                venue_prices = {}
                for venue in sorted({venue for part in parts for venue in part["venues"]}):
                    venue_state = {
                        key: sum((part["venues"].get(venue, {}).get(key, Decimal("0")) for part in parts), Decimal("0"))
                        for key in ("price_sum", "trade_count")
                    }
                    if venue_state["trade_count"]:
                        venue_prices[venue] = self._fmt(venue_state["price_sum"] / venue_state["trade_count"])
                trade_count = sum(part["trade_count"] for part in parts)
                buy_volume = sum((part["buy_volume"] for part in parts), Decimal("0"))
                sell_volume = sum((part["sell_volume"] for part in parts), Decimal("0"))
                fill_intervals = [value for part in parts for value in part["fill_intervals"]]
                bars.append({"schema_version": 1, "primitive_schema_version": 2, "event_type": "market_bar", "instrument": instrument, "frequency": frequency, "interval_ms": interval, "bucket_start_ts_ms": start, "bucket_end_ts_ms": end, "open": self._fmt(parts[0]["open"]), "high": self._fmt(max(part["high"] for part in parts)), "low": self._fmt(min(part["low"] for part in parts)), "close": self._fmt(parts[-1]["close"]), "vwap": self._fmt(notional / volume), "volume": self._fmt(volume), "delta": self._fmt(delta), "trade_count": trade_count, "p_open": self._fmt(parts[0]["open"]), "p_high": self._fmt(max(part["high"] for part in parts)), "p_low": self._fmt(min(part["low"] for part in parts)), "p_trade": self._fmt(parts[-1]["close"]), "p_close": self._fmt(parts[-1]["close"]), "p_trade_mean": self._fmt(sum((part["price_sum"] for part in parts), Decimal("0")) / trade_count), "v_trade": self._fmt(volume), "v_buy": self._fmt(buy_volume), "v_sell": self._fmt(sell_volume), "cnt_trade": trade_count, "dt_fill_mean_ms": self._fmt(sum(fill_intervals, Decimal("0")) / len(fill_intervals)) if fill_intervals else None, "dt_fill_max_ms": self._fmt(max(fill_intervals)) if fill_intervals else None, "dt_fill_min_ms": self._fmt(min(fill_intervals)) if fill_intervals else None, "venue_count": len(venue_prices), "venue_prices": venue_prices})
        return bars

    def export_candle_state(self) -> dict[str, Any]:
        """Return the complete candle state in JSON-safe Decimal form."""
        buckets = []
        for start, state in sorted(self.trade_buckets.items()):
            buckets.append({
                "start": start,
                "notional": self._fmt(state["notional"]),
                "volume": self._fmt(state["volume"]),
                "buy_volume": self._fmt(state.get("buy_volume", Decimal("0"))),
                "sell_volume": self._fmt(state.get("sell_volume", Decimal("0"))),
                "price_sum": self._fmt(state["price_sum"]),
                "trade_count": state["trade_count"],
                "open": self._fmt(state["open"]),
                "high": self._fmt(state["high"]),
                "low": self._fmt(state["low"]),
                "close": self._fmt(state["close"]),
                "delta": self._fmt(state["delta"]),
                "fill_intervals": [self._fmt(value) for value in state.get("fill_intervals", [])],
                "venues": {
                    venue: {
                        "notional": self._fmt(values["notional"]),
                        "volume": self._fmt(values["volume"]),
                        "buy_volume": self._fmt(values.get("buy_volume", Decimal("0"))),
                        "sell_volume": self._fmt(values.get("sell_volume", Decimal("0"))),
                        "price_sum": self._fmt(values["price_sum"]),
                        "trade_count": values["trade_count"],
                        "open": self._fmt(values.get("open")),
                        "high": self._fmt(values.get("high")),
                        "low": self._fmt(values.get("low")),
                        "close": self._fmt(values.get("close")),
                        "fill_intervals": [self._fmt(value) for value in values.get("fill_intervals", [])],
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
                "buy_volume": _dec(raw.get("buy_volume", "0")), "sell_volume": _dec(raw.get("sell_volume", "0")),
                "price_sum": _dec(raw["price_sum"]), "trade_count": int(raw["trade_count"]),
                "open": _dec(raw["open"]) if raw.get("open") is not None else None,
                "high": _dec(raw["high"]) if raw.get("high") is not None else None,
                "low": _dec(raw["low"]) if raw.get("low") is not None else None,
                "close": _dec(raw["close"]) if raw.get("close") is not None else None,
                "delta": _signed_dec(raw["delta"]) if raw.get("delta") is not None else Decimal("0"),
                "fill_intervals": [_dec(value) for value in raw.get("fill_intervals", [])],
                "venues": defaultdict(lambda: {"notional": Decimal("0"), "volume": Decimal("0"), "buy_volume": Decimal("0"), "sell_volume": Decimal("0"), "price_sum": Decimal("0"), "trade_count": 0, "open": None, "high": None, "low": None, "close": None, "fill_intervals": []}),
            }
            for venue, values in raw.get("venues", {}).items():
                state["venues"][venue] = {
                    "notional": _dec(values["notional"]), "volume": _dec(values["volume"]),
                    "buy_volume": _dec(values.get("buy_volume", "0")), "sell_volume": _dec(values.get("sell_volume", "0")),
                    "price_sum": _dec(values["price_sum"]), "trade_count": int(values["trade_count"]),
                    "open": _dec(values["open"]) if values.get("open") is not None else None,
                    "high": _dec(values["high"]) if values.get("high") is not None else None,
                    "low": _dec(values["low"]) if values.get("low") is not None else None,
                    "close": _dec(values["close"]) if values.get("close") is not None else None,
                    "fill_intervals": [_dec(value) for value in values.get("fill_intervals", [])],
                }
            restored[start] = state
        self.trade_buckets = restored
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
            if start < current - self.history_ms:
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
