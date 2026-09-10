from __future__ import annotations

import os
from datetime import datetime, timezone

import redis
from dash import Dash, Input, Output, State, dcc, html

from dashboard.data import RedisReader, redis_client_from_env
from dashboard.kalshi_contracts import contract_table
from dashboard.kalshi_monitor import kalshi_market_figure, kalshi_monitor_layout, kalshi_monitor_summary
from dashboard.plots import candle_figure, volatility_cone_figure, volume_figure

REDIS_PREFIX = os.getenv("AGGREGATOR_OUTPUT_PREFIX", "market")
INSTRUMENT = os.getenv("DASHBOARD_INSTRUMENT", "BTCUSDT")
reader = RedisReader(redis_client_from_env(), REDIS_PREFIX, INSTRUMENT)

CARD = {"background": "#111827", "border": "1px solid #243244", "borderRadius": "8px", "padding": "12px", "minWidth": 0}


def layout() -> html.Div:
    return html.Div([
        html.Div([html.Div("KALSHI QUANT TERMINAL", className="title"), html.Div(id="status", className="status")], className="header"),
        dcc.Interval(id="market-refresh", interval=250, n_intervals=0),
        dcc.Interval(id="candle-refresh", interval=500, n_intervals=0),
        dcc.Interval(id="kalshi-monitor-refresh", interval=1000, n_intervals=0),
        dcc.Interval(id="kalshi-table-refresh", interval=2000, n_intervals=0),
        dcc.Interval(id="volatility-refresh", interval=10_000, n_intervals=0),
        dcc.Store(id="spot-data"),
        dcc.Store(id="candle-data"),
        dcc.Store(id="forming-candle"),
        dcc.Store(id="volatility-data"),
        dcc.Store(id="status-data"),
        dcc.Store(id="kalshi-monitor-data"),
        dcc.Store(id="kalshi-table-data"),
        html.Div([
            html.Div([html.H3("BTCUSDT", className="panel-title"), dcc.Graph(id="candles", config={"displayModeBar": False}), dcc.Graph(id="volume", config={"displayModeBar": False}), dcc.Graph(id="volatility-cone", config={"displayModeBar": False})], style=CARD),
            html.Div([html.H3("Kalshi contract monitor", className="panel-title"), kalshi_monitor_layout()], style=CARD),
        ], className="top-grid"),
        html.Div([
            html.H3("Active KXBTCD contracts", className="panel-title"),
            contract_table(),
            html.Div(
                "Waiting for next hourly contract…",
                id="kalshi-contracts-waiting",
                className="placeholder",
                style={"display": "none"},
            ),
        ], style={**CARD, "marginTop": "14px"}),
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
    Output("spot-data", "data"),
    Output("status-data", "data"),
    Input("market-refresh", "n_intervals"),
)
def refresh_market_data(_: int):
    data = reader.read_fast_market_data()
    return data["spot"], {"redis_ok": data["redis_ok"], "redis_error": data["redis_error"]}


@app.callback(Output("candle-data", "data"), Input("candle-refresh", "n_intervals"))
def refresh_candle_data(_: int):
    return reader.read_candle_data()["candles"]


@app.callback(Output("volatility-data", "data"), Input("volatility-refresh", "n_intervals"))
def refresh_volatility_data(_: int):
    return reader.read_volatility_data()


@app.callback(
    Output("forming-candle", "data"),
    Input("spot-data", "data"), Input("candle-data", "data"),
    State("forming-candle", "data"),
)
def update_forming_candle(spot: dict | None, candles: list[dict] | None, previous: dict | None):
    """Keep the display-only candle moving between canonical bar publications."""
    try:
        price = float((spot or {})["price"])
        timestamp = int((spot or {}).get("generated_ts_ms") or 0)
    except (KeyError, TypeError, ValueError):
        return previous
    interval = 30_000
    start = (timestamp // interval) * interval
    if not previous or previous.get("bucket_start_ts_ms") != start:
        base = next((row for row in reversed(candles or []) if row.get("bucket_start_ts_ms") == start), None)
        opening = float(base["open"]) if base else price
        return {"bucket_start_ts_ms": start, "open": opening, "high": max(opening, price), "low": min(opening, price), "close": price, "volume": (base or {}).get("volume", "0")}
    previous = dict(previous)
    previous["high"] = max(float(previous["high"]), price)
    previous["low"] = min(float(previous["low"]), price)
    previous["close"] = price
    return previous


def _read_kalshi_data(spot_payload: dict | None) -> dict:
    return reader.read_kalshi_data((spot_payload or {}).get("price"))


@app.callback(Output("kalshi-monitor-data", "data"), Input("kalshi-monitor-refresh", "n_intervals"), State("spot-data", "data"))
def refresh_kalshi_monitor_data(_: int, spot_payload: dict | None):
    return _read_kalshi_data(spot_payload)


@app.callback(Output("kalshi-table-data", "data"), Input("kalshi-table-refresh", "n_intervals"), State("spot-data", "data"))
def refresh_kalshi_table_data(_: int, spot_payload: dict | None):
    return _read_kalshi_data(spot_payload)


def _kalshi_snapshot(payload: dict | None) -> dict:
    return payload or {
        "contracts": [], "spot": None, "waiting_for_hourly_contract": False,
        "redis_ok": False, "redis_error": "no data",
    }


@app.callback(Output("candles", "figure"), Input("candle-data", "data"), Input("spot-data", "data"), Input("forming-candle", "data"))
def refresh_candles(candles: list[dict] | None, spot: dict | None, forming: dict | None):
    return candle_figure(candles or [], (spot or {}).get("price"), forming)


@app.callback(Output("volume", "figure"), Input("candle-data", "data"))
def refresh_volume(candles: list[dict] | None):
    return volume_figure(candles or [])


@app.callback(Output("volatility-cone", "figure"), Input("volatility-data", "data"))
def refresh_volatility_cone(payload: dict | None):
    return volatility_cone_figure(payload or {})


@app.callback(
    Output("kalshi-monitor-summary", "children"),
    Output("kalshi-monitor-summary", "style"),
    Output("kalshi-market-structure", "figure"),
    Output("kalshi-market-structure", "style"),
    Output("kalshi-monitor-waiting", "style"),
    Input("kalshi-monitor-data", "data"),
)
def refresh_kalshi_monitor(payload: dict | None):
    data = _kalshi_snapshot(payload)
    spot_payload = {"price": data["spot"]}
    waiting = data["waiting_for_hourly_contract"]
    return (
        kalshi_monitor_summary(data["contracts"], spot_payload),
        {"display": "none"} if waiting else {},
        kalshi_market_figure(data["contracts"], data["spot"]),
        {"display": "none"} if waiting else {},
        {} if waiting else {"display": "none"},
    )


@app.callback(
    Output("kalshi-contract-grid", "rowData"),
    Output("kalshi-contract-grid", "style"),
    Output("kalshi-contracts-waiting", "style"),
    Input("kalshi-table-data", "data"),
)
def refresh_kalshi_contracts(payload: dict | None):
    data = _kalshi_snapshot(payload)
    waiting = data["waiting_for_hourly_contract"]
    return (
        data["contracts"],
        {"display": "none"} if waiting else {"width": "100%", "height": "430px"},
        {} if waiting else {"display": "none"},
    )


@app.callback(Output("status", "children"), Input("status-data", "data"))
def refresh_status(data: dict | None):
    data = data or {"redis_ok": False, "redis_error": "no data"}
    updated = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    return f"Redis live · refreshed {updated}" if data["redis_ok"] else f"Redis unavailable · {data['redis_error']}"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8050")), debug=False)
