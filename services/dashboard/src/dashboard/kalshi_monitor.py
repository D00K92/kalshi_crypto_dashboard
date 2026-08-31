from __future__ import annotations

from typing import Any

import plotly.graph_objects as go
from dash import dcc, html

PAPER = "#111827"
GRID = "#243244"


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _fmt_price(value: float | None) -> str:
    return f"{value:,.2f}" if value is not None else "-"


def _fmt_prob(value: float | None) -> str:
    return f"{value:.1f}%" if value is not None else "-"


def _chart_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row for row in rows
        if row.get("strike") is not None
        and row.get("bid_value") is not None
        and row.get("ask_value") is not None
    ]


def _atm_row(rows: list[dict[str, Any]], spot: float | None) -> dict[str, Any] | None:
    if not rows:
        return None
    if spot is not None:
        return min(rows, key=lambda row: abs(float(row["strike"]) - spot))
    return min(rows, key=lambda row: abs(((row.get("bid_value") or 0) + (row.get("ask_value") or 0)) / 2 - 50))


def kalshi_market_figure(rows: list[dict[str, Any]], spot: float | None = None) -> go.Figure:
    chart_rows = _chart_rows(rows)
    fig = go.Figure()
    if chart_rows:
        strikes = [row["strike"] for row in chart_rows]
        bids = [row["bid_value"] for row in chart_rows]
        asks = [row["ask_value"] for row in chart_rows]
        last = [row["last_value"] for row in chart_rows]
        hover = [row["market_ticker"] for row in chart_rows]
        fig.add_trace(go.Scatter(x=strikes, y=asks, mode="lines", line=dict(width=0), hoverinfo="skip", showlegend=False))
        fig.add_trace(go.Scatter(
            x=strikes,
            y=bids,
            mode="lines",
            fill="tonexty",
            fillcolor="rgba(0,229,176,0.16)",
            line=dict(color="#00e5b0", width=1.5),
            name="Market",
            customdata=hover,
            hovertemplate="%{customdata}<br>Strike %{x:,.2f}<br>Bid %{y:.1f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=strikes,
            y=last,
            mode="markers",
            marker=dict(color="#e5e7eb", size=4, opacity=0.75),
            name="Last",
            customdata=hover,
            hovertemplate="%{customdata}<br>Strike %{x:,.2f}<br>Last %{y:.1f}<extra></extra>",
        ))
    if spot is not None:
        fig.add_vline(x=spot, line_dash="dot", line_color="#e5e7eb", opacity=0.75)
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=PAPER,
        plot_bgcolor=PAPER,
        margin=dict(l=42, r=14, t=8, b=34),
        font=dict(family="monospace", size=11),
        height=430,
        showlegend=True,
        legend=dict(orientation="h", y=1.08, x=0),
        xaxis_title="Strike",
        yaxis_title="YES probability",
        yaxis=dict(range=[0, 100]),
    )
    fig.update_xaxes(gridcolor=GRID, zerolinecolor=GRID, tickformat=",.0f")
    fig.update_yaxes(gridcolor=GRID, zerolinecolor=GRID, ticksuffix="%")
    return fig


def kalshi_monitor(rows: list[dict[str, Any]], spot_payload: dict[str, Any]) -> html.Div:
    spot = _float(spot_payload.get("price"))
    chart_rows = _chart_rows(rows)
    atm = _atm_row(chart_rows, spot)
    event = rows[0].get("event") if rows else "-"
    best_bid = max((row.get("bid_value") for row in chart_rows if row.get("bid_value") is not None), default=None)
    best_ask = min((row.get("ask_value") for row in chart_rows if row.get("ask_value") is not None), default=None)
    summary = [
        html.Div([html.Span("EVENT"), html.Strong(event)]),
        html.Div([html.Span("SPOT"), html.Strong(_fmt_price(spot))]),
        html.Div([html.Span("ATM"), html.Strong(_fmt_price(atm.get("strike") if atm else None))]),
        html.Div([html.Span("ATM MID"), html.Strong(_fmt_prob(((atm.get("bid_value") or 0) + (atm.get("ask_value") or 0)) / 2 if atm else None))]),
        html.Div([html.Span("BEST BID"), html.Strong(_fmt_prob(best_bid))]),
        html.Div([html.Span("BEST ASK"), html.Strong(_fmt_prob(best_ask))]),
    ]
    return html.Div([
        html.Div(summary, className="kalshi-monitor-strip"),
        dcc.Graph(
            id="kalshi-market-structure",
            figure=kalshi_market_figure(rows, spot),
            config={"displayModeBar": False},
        ),
    ])
