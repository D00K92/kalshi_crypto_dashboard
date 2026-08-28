from __future__ import annotations

import os
from datetime import datetime, timezone

import redis
from dash import Dash, Input, Output, dcc, html

from dashboard.data import RedisReader, redis_client_from_env
from dashboard.orderbook import orderbook_ladder
from dashboard.plots import candle_figure

REDIS_PREFIX = os.getenv("AGGREGATOR_OUTPUT_PREFIX", "market")
INSTRUMENT = os.getenv("DASHBOARD_INSTRUMENT", "BTCUSDT")
reader = RedisReader(redis_client_from_env(), REDIS_PREFIX, INSTRUMENT)

CARD = {"background": "#111827", "border": "1px solid #243244", "borderRadius": "8px", "padding": "12px", "minWidth": 0}


def layout() -> html.Div:
    return html.Div([
        html.Div([html.Div("KALSHI QUANT TERMINAL", className="title"), html.Div(id="status", className="status")], className="header"),
        dcc.Interval(id="refresh", interval=1000, n_intervals=0),
        html.Div([
            html.Div([html.H3("BTCUSDT", className="panel-title"), html.Div(id="spot"), dcc.Graph(id="candles", config={"displayModeBar": False})], style=CARD),
            html.Div([html.H3("Aggregated order book", className="panel-title"), html.Div(id="book-status"), html.Div(id="orderbook")], style=CARD),
            html.Div([html.H3("Kalshi contract monitor", className="panel-title"), html.Div("KXBTCD chain", className="muted"), html.Div("Chart placeholder — Kalshi analytics will be connected in the next phase.", className="placeholder"), html.Div(id="kalshi-chain", className="placeholder")], style=CARD),
        ], className="top-grid"),
        html.Div([html.H3("Active KXBTCD contracts", className="panel-title"), html.Div("Contract table placeholder — data source will be added with the Kalshi adapter.", className="placeholder")], style={**CARD, "marginTop": "14px"}),
    ], className="shell")


app = Dash(__name__, title="Kalshi Quant Terminal", update_title=None)
app.layout = layout


@app.callback(Output("spot", "children"), Output("candles", "figure"), Output("orderbook", "children"), Output("book-status", "children"), Output("status", "children"), Input("refresh", "n_intervals"))
def refresh(_: int):
    data = reader.read()
    price = data.spot.get("price")
    try:
        formatted_price = f"{float(price):.3f}" if price is not None else "—"
    except (TypeError, ValueError):
        formatted_price = "—"
    spot = f"Synthetic VWAP: {formatted_price} USDT · volume {data.spot.get('total_volume', '0')}"
    stale = data.book.get("stale_venues", [])
    book_status = f"venues: {', '.join(data.book.get('venues', [])) or 'none'}" + (f" · stale: {', '.join(stale)}" if stale else "")
    updated = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    return spot, candle_figure(data.candles), orderbook_ladder(data.book), book_status, f"Redis live · refreshed {updated}"


server = app.server


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8050")), debug=False)
