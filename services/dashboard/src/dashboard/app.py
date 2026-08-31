from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timezone

import redis
from dash import Dash, Input, Output, dcc, html

from dashboard.data import RedisReader, redis_client_from_env
from dashboard.kalshi_contracts import contract_table, select_contract_window
from dashboard.kalshi_monitor import kalshi_monitor
from dashboard.orderbook import orderbook_ladder
from dashboard.plots import candle_figure, volume_figure

REDIS_PREFIX = os.getenv("AGGREGATOR_OUTPUT_PREFIX", "market")
INSTRUMENT = os.getenv("DASHBOARD_INSTRUMENT", "BTCUSDT")
reader = RedisReader(redis_client_from_env(), REDIS_PREFIX, INSTRUMENT)

CARD = {"background": "#111827", "border": "1px solid #243244", "borderRadius": "8px", "padding": "12px", "minWidth": 0}


def layout() -> html.Div:
    return html.Div([
        html.Div([html.Div("KALSHI QUANT TERMINAL", className="title"), html.Div(id="status", className="status")], className="header"),
        dcc.Interval(id="refresh", interval=1000, n_intervals=0),
        dcc.Store(id="market-data"),
        html.Div([
            html.Div([html.H3("BTCUSDT", className="panel-title"), dcc.Graph(id="candles", config={"displayModeBar": False}), dcc.Graph(id="volume", config={"displayModeBar": False})], style=CARD),
            html.Div([html.Div(id="orderbook")], style={"minWidth": 0}),
            html.Div([html.H3("Kalshi contract monitor", className="panel-title"), html.Div(id="kalshi-chain")], style=CARD),
        ], className="top-grid"),
        html.Div([html.H3("Active KXBTCD contracts", className="panel-title"), html.Div(id="kalshi-contracts")], style={**CARD, "marginTop": "14px"}),
    ], className="shell")


app = Dash(__name__, title="Kalshi Quant Terminal", update_title=None)
app.layout = layout
server = app.server


@server.get("/healthz")
def healthz():
    return {"status": "ok"}, 200


@server.get("/readyz")
def readyz():
    try:
        reader.client.ping()
    except redis.RedisError:
        return {"status": "not_ready"}, 503
    return {"status": "ready"}, 200


@app.callback(Output("market-data", "data"), Input("refresh", "n_intervals"))
def refresh_data(_: int):
    return asdict(reader.read())


def _snapshot(payload: dict | None) -> dict:
    return payload or {"book": {"bids": [], "asks": [], "venues": [], "stale_venues": []}, "spot": {"price": None}, "candles": [], "kalshi_contracts": [], "redis_ok": False, "redis_error": "no data"}


@app.callback(Output("candles", "figure"), Input("market-data", "data"))
def refresh_candles(payload: dict | None):
    data = _snapshot(payload)
    return candle_figure(data["candles"], data["spot"].get("price"))


@app.callback(Output("volume", "figure"), Input("market-data", "data"))
def refresh_volume(payload: dict | None):
    return volume_figure(_snapshot(payload)["candles"])


@app.callback(Output("orderbook", "children"), Input("market-data", "data"))
def refresh_orderbook(payload: dict | None):
    return orderbook_ladder(_snapshot(payload)["book"])


def _kalshi_rows(payload: dict | None) -> tuple[list[dict], dict]:
    data = _snapshot(payload)
    price = data["spot"].get("price")
    try:
        spot_price = float(price) if price is not None else None
    except (TypeError, ValueError):
        spot_price = None
    return select_contract_window(data["kalshi_contracts"], spot_price), data["spot"]


@app.callback(Output("kalshi-chain", "children"), Input("market-data", "data"))
def refresh_kalshi_monitor(payload: dict | None):
    rows, spot = _kalshi_rows(payload)
    return kalshi_monitor(rows, spot)


@app.callback(Output("kalshi-contracts", "children"), Input("market-data", "data"))
def refresh_kalshi_contracts(payload: dict | None):
    rows, _ = _kalshi_rows(payload)
    return contract_table(rows)


@app.callback(Output("status", "children"), Input("market-data", "data"))
def refresh_status(payload: dict | None):
    data = _snapshot(payload)
    updated = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    return f"Redis live · refreshed {updated}" if data["redis_ok"] else f"Redis unavailable · {data['redis_error']}"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8050")), debug=False)
