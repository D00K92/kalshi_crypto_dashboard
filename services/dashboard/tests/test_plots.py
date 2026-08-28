from dashboard.plots import candle_figure, cvd_figure, orderbook_figure


def test_market_figures_render_empty_state():
    assert len(candle_figure([]).data) == 1
    assert len(cvd_figure([]).data) == 1
    assert len(orderbook_figure({"bids": [], "asks": []}).data) == 3


def test_candle_figure_limits_to_five_minutes_and_uses_datetime():
    rows = [{"bucket_start_ts_ms": i * 5000, "open": "1", "high": "2", "low": "0", "close": "1", "volume": "1"} for i in range(100)]
    figure = candle_figure(rows)
    assert len(figure.data[0].x) == 60
    assert figure.layout.yaxis.title.text == "BTC price"
    assert figure.layout.xaxis.type == "date"


def test_orderbook_preserves_venue_stacking():
    figure = orderbook_figure({"venues": ["binance"], "bids": [{"price": "100", "venues": {"binance": "2"}}], "asks": []})
    assert figure.data[0].name == "binance"
    assert list(figure.data[0].x) == [2.0]
