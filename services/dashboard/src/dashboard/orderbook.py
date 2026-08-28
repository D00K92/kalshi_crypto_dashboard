from __future__ import annotations

from typing import Any

from dash import html

VENUE_COLORS = {"binance": "#f0b90b", "coinbase": "#1652f0", "bybit": "#f5a623"}


def _row(side: str, level: dict[str, Any], max_volume: float) -> html.Div:
    total = float(level.get("total_quantity") or 0)
    width = max(0.0, min(100.0, total / max_volume * 100)) if max_volume else 0
    segments = level.get("venues") or {}
    bar = html.Div(
        [html.Div(title=f"{venue}: {quantity}", style={"width": f"{float(quantity) / total * 100:.4f}%", "backgroundColor": VENUE_COLORS.get(venue, "#94a3b8"), "height": "100%"}) for venue, quantity in segments.items() if float(quantity) > 0],
        className=f"depth-bar {side}", style={"width": f"{width:.3f}%"},
    )
    return html.Div([html.Div(level.get("price", "—"), className="book-price"), html.Div(bar, className="book-bar-cell"), html.Div(level.get("total_quantity", "0"), className="book-volume")], className=f"book-row {side}")


def orderbook_ladder(book: dict[str, Any]) -> html.Div:
    asks = list(reversed(book.get("asks") or []))
    bids = list(book.get("bids") or [])
    levels = asks + bids
    volumes = [float(level.get("total_quantity") or 0) for level in levels]
    max_volume = max(volumes, default=0)
    spread = html.Div([html.Span("SPREAD", className="spread-label"), html.Span("—", className="spread-value")], className="book-spread")
    rows = [_row("ask", level, max_volume) for level in asks] + [spread] + [_row("bid", level, max_volume) for level in bids]
    if not levels:
        rows = [html.Div("Waiting for live order book…", className="book-empty")]
    return html.Div([html.Div([html.Span("PRICE"), html.Span("DEPTH"), html.Span("VOLUME")], className="book-header"), html.Div(rows, className="book-rows")], className="orderbook-ladder")
