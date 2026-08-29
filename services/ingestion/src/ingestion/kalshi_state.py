"""Kalshi orderbook application and safe event-set rollover state."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from ingestion.kalshi_discovery import KalshiEventSet
from ingestion.models import KalshiOrderBookSnapshot


class KalshiStateError(ValueError):
    """Raised when a snapshot/delta cannot be applied safely."""


def _decimal(value: str, field: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise KalshiStateError(f"{field} is not numeric") from exc
    if not parsed.is_finite():
        raise KalshiStateError(f"{field} must be finite")
    return parsed


@dataclass(slots=True)
class KalshiBookState:
    """One binary market book, keyed independently from other contracts."""

    last_sequence: int = 0
    yes: dict[Decimal, Decimal] = field(default_factory=dict)
    no: dict[Decimal, Decimal] = field(default_factory=dict)

    def apply(self, event: KalshiOrderBookSnapshot) -> None:
        if event.event_type == "kalshi_orderbook_snapshot":
            self.yes.clear()
            self.no.clear()
            self._load(self.yes, event.yes_bids)
            self._load(self.no, event.no_bids)
            self.last_sequence = event.sequence
            return
        if event.event_type != "kalshi_orderbook_delta":
            raise KalshiStateError(f"unsupported event type {event.event_type}")
        if self.last_sequence == 0:
            raise KalshiStateError("cannot apply delta before a snapshot")
        if event.sequence != self.last_sequence + 1:
            raise KalshiStateError(
                f"orderbook sequence gap: expected {self.last_sequence + 1}, got {event.sequence}"
            )
        if event.delta_price_dollars is None or event.delta_fp is None or event.delta_side is None:
            raise KalshiStateError("orderbook delta is missing price, quantity, or side")
        price = _decimal(event.delta_price_dollars, "delta price")
        delta = _decimal(event.delta_fp, "delta quantity")
        levels = self.yes if event.delta_side == "yes" else self.no
        quantity = levels.get(price, Decimal("0")) + delta
        if quantity < 0:
            raise KalshiStateError("orderbook delta would create negative quantity")
        if quantity == 0:
            levels.pop(price, None)
        else:
            levels[price] = quantity
        self.last_sequence = event.sequence

    @staticmethod
    def _load(target: dict[Decimal, Decimal], levels) -> None:
        for level in levels:
            price = _decimal(level.price, "price")
            quantity = _decimal(level.quantity, "quantity")
            if quantity < 0:
                raise KalshiStateError("snapshot quantity cannot be negative")
            if quantity:
                target[price] = quantity

    def levels(self) -> tuple[tuple[str, str], tuple[tuple[str, str], ...]]:
        yes = tuple((format(price, "f"), format(qty, "f")) for price, qty in sorted(self.yes.items()))
        no = tuple((format(price, "f"), format(qty, "f")) for price, qty in sorted(self.no.items()))
        return yes, no


@dataclass(slots=True)
class KalshiRolloverState:
    """Stages a new event before making it the only accepted event."""

    active: KalshiEventSet | None = None
    pending: KalshiEventSet | None = None
    generation: int = 0

    def stage(self, candidate: KalshiEventSet) -> bool:
        if self.active and self.active.event_ticker == candidate.event_ticker:
            return False
        self.pending = candidate
        return True

    def confirm(self, event_ticker: str) -> tuple[str, ...]:
        if self.pending is None or self.pending.event_ticker != event_ticker:
            raise KalshiStateError("cannot confirm an un-staged event")
        old_markets = self.active.market_tickers if self.active else ()
        self.active = self.pending
        self.pending = None
        self.generation += 1
        return old_markets

    def accepts(self, event_ticker: str) -> bool:
        return bool(
            self.active and self.active.event_ticker == event_ticker
        ) or bool(self.pending and self.pending.event_ticker == event_ticker)
