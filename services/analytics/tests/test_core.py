import pytest

from kalshi_crypto_analytics.core import (
    gaussian_probability,
    interpolate_volatility,
    quote_edges,
)
from kalshi_crypto_analytics.schemas import PricingUnavailable, UnavailableReason

VOLS = {"5m": .21, "15m": .22, "30m": .23, "1h": .24}


@pytest.mark.parametrize(("tau", "bracket", "expected"), [
    (.001, ("5m", "5m"), .21), (299.999, ("5m", "5m"), .21),
    (300, ("5m", "15m"), .21),
    (900, ("15m", "30m"), .22), (1800, ("30m", "1h"), .23),
    (3600, ("1h", "1h"), .24),
])
def test_horizon_boundaries(tau, bracket, expected):
    sigma, actual_bracket = interpolate_volatility(tau, VOLS)
    assert actual_bracket == bracket
    assert sigma == pytest.approx(expected)


def test_linear_annualized_volatility_interpolation():
    sigma, _ = interpolate_volatility(600, VOLS)
    assert sigma == pytest.approx(.215)


@pytest.mark.parametrize("tau", [0, -1, float("nan"), float("inf"), 3600.0001])
def test_invalid_lifetime(tau):
    with pytest.raises(PricingUnavailable) as exc:
        interpolate_volatility(tau, VOLS)
    assert exc.value.reason == UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME


@pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), "x"])
def test_invalid_volatility_rejected(bad):
    values = {**VOLS, "15m": bad}
    with pytest.raises(PricingUnavailable) as exc:
        interpolate_volatility(600, values)
    assert exc.value.reason == UnavailableReason.INCOMPLETE_TERM_STRUCTURE


def test_descending_curve_is_interpolated_without_repair():
    values = {**VOLS, "15m": .05}
    sigma, bracket = interpolate_volatility(600, values)
    assert bracket == ("5m", "15m")
    assert sigma == pytest.approx(.13)


def test_gaussian_probability_directionality():
    assert gaussian_probability(100, 100, .2, 300) == pytest.approx(.5)
    assert gaussian_probability(101, 100, .2, 300) > .5
    assert gaussian_probability(99, 100, .2, 300) < .5


@pytest.mark.parametrize(("bid", "ask"), [(None, .5), (.5, None), (.6, .5), (-.1, .5), (.5, 1.1), (float("nan"), .5)])
def test_malformed_crossed_and_one_sided_quotes_are_null(bid, ask):
    assert all(value is None for value in quote_edges(bid, ask, .55).values())


def test_quote_edges():
    result = quote_edges(.4, .5, .6)
    assert result["market_mid_probability"] == pytest.approx(.45)
    assert result["buy_yes_edge_probability"] == pytest.approx(.1)
    assert result["sell_yes_edge_probability"] == pytest.approx(-.2)
