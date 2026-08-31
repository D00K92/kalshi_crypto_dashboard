from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import plotly.graph_objects as go

VENUE_COLORS = {"binance": "#f0b90b", "coinbase": "#1652f0", "bybit": "#f5a623"}
PAPER = "#111827"
GRID = "#243244"


def _figure(**kwargs: Any) -> go.Figure:
    fig = go.Figure(**kwargs)
    fig.update_layout(template="plotly_dark", paper_bgcolor=PAPER, plot_bgcolor=PAPER, margin=dict(l=42, r=12, t=12, b=28), font=dict(family="monospace", size=11), legend=dict(orientation="h", y=1.08, x=0))
    fig.update_xaxes(gridcolor=GRID, zerolinecolor=GRID)
    fig.update_yaxes(gridcolor=GRID, zerolinecolor=GRID)
    return fig


def candle_figure(rows: list[dict[str, Any]], synthetic_price: Any = None) -> go.Figure:
    # The aggregator retains an hour; the dashboard shows the latest 60 candles.
    rows = rows[-60:]
    x = [datetime.fromtimestamp(int(row["bucket_start_ts_ms"]) / 1000, tz=timezone.utc) for row in rows]
    fig = _figure()
    fig.add_trace(go.Candlestick(x=x, open=[row["open"] for row in rows], high=[row["high"] for row in rows], low=[row["low"] for row in rows], close=[row["close"] for row in rows], name="BTCUSDT", increasing_line_color="#19c37d", decreasing_line_color="#ef5350"))
    fig.update_layout(xaxis_rangeslider_visible=False, xaxis=dict(type="date", tickformat="%H:%M:%S", showticklabels=True), yaxis_title="BTC price", height=300, showlegend=False, uirevision="btc-candles")
    if synthetic_price is not None:
        try:
            displayed_price = f"Synthetic {float(synthetic_price):.2f} USD"
            fig.add_hline(y=float(synthetic_price), line_color="#f8fafc", line_width=1, line_dash="dot")
            fig.add_annotation(x=1, y=float(synthetic_price), xref="paper", yref="y", text=displayed_price, showarrow=False, xanchor="right", yanchor="bottom", font=dict(color="#f8fafc", size=12), bgcolor="rgba(17,24,39,.8)", bordercolor=GRID, borderpad=4)
        except (TypeError, ValueError):
            pass
    return fig


def volume_figure(rows: list[dict[str, Any]]) -> go.Figure:
    rows = rows[-60:]
    x = [datetime.fromtimestamp(int(row["bucket_start_ts_ms"]) / 1000, tz=timezone.utc) for row in rows]
    volumes = [float(row.get("volume") or 0) for row in rows]
    colors = ["#19c37d" if float(row["close"]) >= float(row["open"]) else "#ef5350" for row in rows]
    fig = _figure()
    fig.add_trace(go.Bar(x=x, y=volumes, name="Volume", marker_color=colors, hovertemplate="%{x|%H:%M:%S} · %{y:.6f} BTC<extra></extra>"))
    fig.update_layout(xaxis=dict(type="date", tickformat="%H:%M:%S", showticklabels=True), yaxis_title="BTC volume", height=120, showlegend=False, uirevision="btc-volume")
    return fig


def cvd_figure(rows: list[dict[str, Any]]) -> go.Figure:
    fig = _figure()
    fig.add_trace(go.Scatter(x=[row["bucket_start_ts_ms"] for row in rows], y=[row["cvd"] for row in rows], mode="lines", line=dict(color="#7dd3fc", width=2), name="CVD", fill="tozeroy", fillcolor="rgba(125,211,252,.12)"))
    fig.update_layout(yaxis_title="Δ volume", height=180)
    return fig


def orderbook_figure(book: dict[str, Any]) -> go.Figure:
    fig = _figure()
    rows = [("ask", row) for row in reversed(book.get("asks", []))] + [("bid", row) for row in book.get("bids", [])]
    venues = book.get("venues", []) or ["binance", "coinbase", "bybit"]
    for venue in venues:
        values = [float(row.get("venues", {}).get(venue, 0)) for _, row in rows]
        fig.add_trace(go.Bar(y=[row.get("price") for _, row in rows], x=values, orientation="h", name=venue, marker_color=VENUE_COLORS.get(venue, "#94a3b8"), customdata=[side for side, _ in rows], hovertemplate="%{y} · %{x:.6f} %{customdata}<extra>" + venue + "</extra>"))
    fig.update_layout(barmode="stack", yaxis=dict(title="Price", categoryorder="array", categoryarray=[row.get("price") for _, row in rows]), xaxis_title="Absolute volume", height=620, showlegend=True)
    return fig
