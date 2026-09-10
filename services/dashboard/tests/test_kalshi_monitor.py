from dashboard.kalshi_monitor import kalshi_market_figure, kalshi_monitor, kalshi_monitor_layout, kalshi_monitor_summary


ROWS = [
    {
        "event": "KXBTCD-TEST",
        "contract": "BTC > 70,000.00",
        "market_ticker": "KXBTCD-TEST-T69999.99",
        "strike": 69999.99,
        "bid_value": 91.0,
        "ask_value": 92.0,
        "last_value": 91.0,
        "model_probability": 0.88,
    },
    {
        "event": "KXBTCD-TEST",
        "contract": "BTC > 70,100.00",
        "market_ticker": "KXBTCD-TEST-T70099.99",
        "strike": 70099.99,
        "bid_value": 41.0,
        "ask_value": 42.0,
        "last_value": 41.0,
        "model_probability": 0.38,
    },
]


def test_kalshi_market_figure_draws_market_band_and_last_trade_points():
    figure = kalshi_market_figure(ROWS, 70050)

    assert len(figure.data) == 4
    assert figure.data[1].name == "Market"
    assert list(figure.data[1].x) == [69999.99, 70099.99]
    assert figure.data[2].name == "Model"
    assert list(figure.data[2].y) == [88.0, 38.0]
    assert figure.data[2].line.color == "#facc15"
    assert figure.layout.yaxis.title.text == "YES probability"
    assert figure.layout.yaxis.range == (0, 100)


def test_kalshi_monitor_builds_summary_and_chart():
    component = kalshi_monitor(ROWS, {"price": "70050"})

    assert component.children[0].className == "kalshi-monitor-strip"
    assert component.children[1].id == "kalshi-market-structure"


def test_kalshi_monitor_layout_keeps_chart_identity_stable():
    component = kalshi_monitor_layout()

    assert component.children[0].id == "kalshi-monitor-summary"
    assert component.children[1].id == "kalshi-market-structure"
    assert len(kalshi_monitor_summary(ROWS, {"price": "70050"})) == 6
