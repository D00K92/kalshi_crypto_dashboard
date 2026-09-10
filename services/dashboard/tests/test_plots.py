from dashboard.plots import candle_figure, cvd_figure, orderbook_figure, volatility_cone_figure, volume_figure


def test_market_figures_render_empty_state():
    assert len(candle_figure([]).data) == 1
    assert len(volume_figure([]).data) == 1
    assert len(volatility_cone_figure({}).data) == 1
    assert len(cvd_figure([]).data) == 1
    assert len(orderbook_figure({"bids": [], "asks": []}).data) == 3


def test_candle_figure_limits_to_sixty_candles_and_uses_datetime():
    rows = [{"bucket_start_ts_ms": i * 10000, "open": "1", "high": "2", "low": "0", "close": "1", "volume": "1"} for i in range(100)]
    figure = candle_figure(rows)
    assert len(figure.data[0].x) == 60
    assert figure.layout.yaxis.title.text == "BTC price"
    assert figure.layout.xaxis.type == "date"
    assert figure.layout.xaxis.matches is None

    volume = volume_figure(rows)
    assert len(volume.data[0].x) == 60
    assert volume.data[0].name == "Volume"
    assert volume.layout.xaxis.type == "date"


def test_candle_figure_places_synthetic_price_inside_chart():
    figure = candle_figure([{"bucket_start_ts_ms": 0, "open": "1", "high": "2", "low": "0", "close": "1", "volume": "3"}], "1.234")
    assert figure.layout.annotations[0].text == "Synthetic 1.23 USD"
    assert figure.layout.shapes[0].y0 == 1.234


def test_volatility_cone_orders_and_formats_four_forecast_horizons():
    figure = volatility_cone_figure({
        "annualized_volatility": {"1h": 0.24, "5m": 0.21, "30m": 0.23, "15m": 0.22},
    })

    assert list(figure.data[0].x) == [5, 15, 30, 60]
    assert list(figure.data[0].y) == [21, 22, 23, 24]
    assert list(figure.data[0].customdata) == ["5m", "15m", "30m", "1h"]
    assert figure.layout.title.text == "Volatility cone"
    assert figure.layout.yaxis.title.text == "Annualized vol (%)"


def test_candle_figure_replaces_current_bar_with_forming_candle():
    rows = [{"bucket_start_ts_ms": 0, "open": "1", "high": "2", "low": "0", "close": "1", "volume": "3"}]
    figure = candle_figure(rows, forming={"bucket_start_ts_ms": 0, "open": 1, "high": 3, "low": 1, "close": 3, "volume": "3"})
    assert list(figure.data[0].high) == [3]
    assert list(figure.data[0].close) == [3]


def test_orderbook_preserves_venue_stacking():
    figure = orderbook_figure({"venues": ["binance"], "bids": [{"price": "100", "venues": {"binance": "2"}}], "asks": []})
    assert figure.data[0].name == "binance"
    assert list(figure.data[0].x) == [2.0]
