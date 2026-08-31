from __future__ import annotations

import math
import re
import time
from typing import Any

import dash_ag_grid as dag

_STRIKE_RE = re.compile(r"-T(?P<strike>\d+(?:\.\d+)?)$")
DEFAULT_MAX_AGE_MS = 5 * 60 * 1000


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _cents_display(value: Any) -> str:
    parsed = _float(value)
    return f"{parsed * 100:.0f}¢" if parsed is not None else "-"


def _cents(value: Any) -> float | None:
    parsed = _float(value)
    return round(parsed * 100, 6) if parsed is not None else None


def _quantity(value: Any) -> str:
    parsed = _float(value)
    if parsed is None:
        return "-"
    if abs(parsed) >= 1000:
        return f"{parsed:,.0f}"
    return f"{parsed:,.2f}"


def _age(received_ts_ms: Any, *, now_ms: int) -> str:
    parsed = _float(received_ts_ms)
    if parsed is None:
        return "-"
    seconds = max(0, int((now_ms - parsed) / 1000))
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    return f"{minutes // 60}h"


def _strike(market_ticker: str) -> float | None:
    match = _STRIKE_RE.search(market_ticker)
    if match is None:
        return None
    return _float(match.group("strike"))


def _contract_label(market_ticker: str) -> str:
    strike = _strike(market_ticker)
    if strike is None:
        return market_ticker
    return f"BTC > {strike:,.2f}"


def _active_event(payloads: list[dict[str, Any]]) -> str | None:
    candidates = [
        payload for payload in payloads
        if isinstance(payload.get("event_ticker"), str) and payload.get("event_ticker")
    ]
    latest = max(candidates, key=lambda payload: _float(payload.get("received_ts_ms")) or 0, default=None)
    return latest.get("event_ticker") if latest else None


def contract_rows(
    tickers: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    *,
    now_ms: int | None = None,
    max_age_ms: int = DEFAULT_MAX_AGE_MS,
) -> list[dict[str, Any]]:
    current_ms = now_ms if now_ms is not None else int(time.time() * 1000)

    def fresh(payload: dict[str, Any]) -> bool:
        received = _float(payload.get("received_ts_ms"))
        return received is not None and current_ms - received <= max_age_ms

    tickers = [payload for payload in tickers if fresh(payload)]
    trades = [payload for payload in trades if fresh(payload)]
    event_ticker = _active_event(tickers + trades)
    latest_by_market: dict[str, dict[str, Any]] = {}
    last_trade_by_market: dict[str, dict[str, Any]] = {}

    for payload in tickers:
        market = payload.get("market_ticker")
        if not isinstance(market, str) or not market:
            continue
        if event_ticker and payload.get("event_ticker") != event_ticker:
            continue
        latest_by_market.setdefault(market, payload)

    for payload in trades:
        market = payload.get("market_ticker")
        if not isinstance(market, str) or not market:
            continue
        if event_ticker and payload.get("event_ticker") != event_ticker:
            continue
        last_trade_by_market.setdefault(market, payload)

    rows: list[dict[str, Any]] = []
    for market, ticker in latest_by_market.items():
        bid = _float(ticker.get("yes_bid_dollars"))
        ask = _float(ticker.get("yes_ask_dollars"))
        spread = ask - bid if bid is not None and ask is not None else None
        trade = last_trade_by_market.get(market, {})
        strike = _strike(market)
        last = _float(ticker.get("last_price_dollars"))
        last_trade = _float(trade.get("yes_price_dollars"))
        ticker_received = _float(ticker.get("received_ts_ms"))
        trade_received = _float(trade.get("received_ts_ms"))
        last_activity = max(
            (timestamp for timestamp in (ticker_received, trade_received) if timestamp is not None),
            default=None,
        )
        rows.append({
            "event": ticker.get("event_ticker") or event_ticker or "-",
            "contract": _contract_label(market),
            "market_ticker": market,
            "strike": strike,
            "bid_value": _cents(bid),
            "ask_value": _cents(ask),
            "spread_value": _cents(spread),
            "last_value": _cents(last),
            "volume_value": _float(ticker.get("volume")),
            "open_interest_value": _float(ticker.get("open_interest")),
            "last_trade_value": _cents(last_trade),
            "last_trade_qty_value": _float(trade.get("count")),
            "bid": _cents_display(bid),
            "ask": _cents_display(ask),
            "spread": _cents_display(spread),
            "last": _cents_display(last),
            "volume": _quantity(ticker.get("volume")),
            "open_interest": _quantity(ticker.get("open_interest")),
            "last_trade": _cents_display(last_trade),
            "last_trade_qty": _quantity(trade.get("count")),
            "last_trade_side": str(trade.get("taker_side") or "-").upper(),
            "age": _age(last_activity, now_ms=current_ms),
            "has_trade": bool(trade),
        })

    return sorted(rows, key=lambda row: (row["strike"] is None, row["strike"] or 0.0, row["market_ticker"]))


def select_contract_window(
    rows: list[dict[str, Any]],
    spot: float | None,
    *,
    lower: int = 8,
    upper: int = 8,
) -> list[dict[str, Any]]:
    """Keep the ATM contract and a bounded strike window around it."""
    if not rows or lower < 0 or upper < 0:
        return [] if not rows else rows

    strike_rows = [row for row in rows if _float(row.get("strike")) is not None]
    if not strike_rows:
        return rows

    atm = min(
        strike_rows,
        key=lambda row: abs(
            float(row["strike"]) - spot
            if spot is not None
            else ((row.get("bid_value") or 0) + (row.get("ask_value") or 0)) / 2 - 50
        ),
    )
    atm_index = rows.index(atm)
    return rows[max(0, atm_index - lower):atm_index + upper + 1]


def contract_table(rows: list[dict[str, Any]]) -> dag.AgGrid:
    return dag.AgGrid(
        id="kalshi-contract-grid",
        rowData=rows,
        columnDefs=[
            {"field": "contract", "headerName": "CONTRACT", "minWidth": 150, "pinned": "left"},
            {"field": "bid", "headerName": "BID", "type": "rightAligned", "width": 88, "cellClass": "quote-bid"},
            {"field": "ask", "headerName": "ASK", "type": "rightAligned", "width": 88, "cellClass": "quote-ask"},
            {"field": "spread", "headerName": "SPR", "type": "rightAligned", "width": 82},
            {"field": "last", "headerName": "LAST", "type": "rightAligned", "width": 88},
            {"field": "volume", "headerName": "VOL", "type": "rightAligned", "width": 110},
            {"field": "open_interest", "headerName": "OI", "type": "rightAligned", "width": 110},
            {"field": "last_trade", "headerName": "TRADE", "type": "rightAligned", "width": 88},
            {"field": "last_trade_qty", "headerName": "QTY", "type": "rightAligned", "width": 96},
            {"field": "last_trade_side", "headerName": "SIDE", "width": 78, "cellStyle": {"function": "kalshiTradeSide(params)"}},
            {"field": "age", "headerName": "AGE", "width": 72},
        ],
        defaultColDef={"sortable": True, "resizable": True},
        columnSize="responsiveSizeToFit",
        className="ag-theme-quartz-dark contract-grid",
        style={"width": "100%", "height": "430px"},
        dashGridOptions={
            "animateRows": False,
            "headerHeight": 30,
            "rowHeight": 28,
            "suppressCellFocus": True,
            "suppressMovableColumns": True,
        },
    )
