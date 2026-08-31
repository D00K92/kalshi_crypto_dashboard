from __future__ import annotations

import os
from datetime import datetime, timezone

import redis
from dash import Dash, Input, Output, State, dcc, html

from dashboard.data import RedisReader, redis_client_from_env
from dashboard.kalshi_contracts import contract_table
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
        dcc.Interval(id="market-refresh", interval=1000, n_intervals=0),
        dcc.Interval(id="kalshi-refresh", interval=1000, n_intervals=0),
        dcc.Store(id="book-data"),
        dcc.Store(id="spot-data"),
        dcc.Store(id="candle-data"),
        dcc.Store(id="status-data"),
        dcc.Store(id="kalshi-data"),
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


@app.callback(
    Output("book-data", "data"),
    Output("spot-data", "data"),
    Output("candle-data", "data"),
    Output("status-data", "data"),
    Input("market-refresh", "n_intervals"),
)
def refresh_market_data(_: int):
    data = reader.read_market_data()
    return data["book"], data["spot"], data["candles"], {"redis_ok": data["redis_ok"], "redis_error": data["redis_error"]}


@app.callback(Output("kalshi-data", "data"), Input("kalshi-refresh", "n_intervals"), State("spot-data", "data"))
def refresh_kalshi_data(_: int, spot_payload: dict | None):
    return reader.read_kalshi_data((spot_payload or {}).get("price"))


def _kalshi_snapshot(payload: dict | None) -> dict:
    return payload or {"contracts": [], "spot": None, "redis_ok": False, "redis_error": "no data"}


@app.callback(Output("candles", "figure"), Input("candle-data", "data"), Input("spot-data", "data"))
def refresh_candles(candles: list[dict] | None, spot: dict | None):
    return candle_figure(candles or [], (spot or {}).get("price"))


@app.callback(Output("volume", "figure"), Input("candle-data", "data"))
def refresh_volume(candles: list[dict] | None):
    return volume_figure(candles or [])


@app.callback(Output("orderbook", "children"), Input("book-data", "data"))
def refresh_orderbook(book: dict | None):
    return orderbook_ladder(book or {"bids": [], "asks": [], "venues": [], "stale_venues": []})


@app.callback(Output("kalshi-chain", "children"), Input("kalshi-data", "data"))
def refresh_kalshi_monitor(payload: dict | None):
    data = _kalshi_snapshot(payload)
    return kalshi_monitor(data["contracts"], {"price": data["spot"]})


@app.callback(Output("kalshi-contracts", "children"), Input("kalshi-data", "data"))
def refresh_kalshi_contracts(payload: dict | None):
    return contract_table(_kalshi_snapshot(payload)["contracts"])


@app.callback(Output("status", "children"), Input("status-data", "data"))
def refresh_status(data: dict | None):
    data = data or {"redis_ok": False, "redis_error": "no data"}
    updated = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    return f"Redis live · refreshed {updated}" if data["redis_ok"] else f"Redis unavailable · {data['redis_error']}"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8050")), debug=False)
