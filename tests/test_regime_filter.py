import pytest
from core.regime_filter import RegimeFilter

@pytest.fixture
def rf():
    return RegimeFilter()

def feed(rf, symbol, high, low, close, n=14):
    for _ in range(n):
        rf.update(symbol, high, low, close)

def test_unknown_before_data(rf):
    assert rf.get_regime('BTCUSDT') == 'UNKNOWN'
    allowed, mult = rf.is_allowed('BTCUSDT', 'all_three')
    assert allowed
    assert mult == 1.0

def test_ranging_regime(rf):
    # Маленький ATR, цена около MA
    feed(rf, 'BTCUSDT', 100.1, 99.9, 100.0, 50)
    assert rf.get_regime('BTCUSDT') == 'RANGING'

def test_trending_regime(rf):
    # Средний ATR
    feed(rf, 'BTCUSDT', 100.5, 99.5, 100.0, 50)
    assert rf.get_regime('BTCUSDT') == 'TRENDING'

def test_volatile_regime(rf):
    # Большой ATR
    feed(rf, 'BTCUSDT', 101.0, 99.0, 100.0, 50)
    assert rf.get_regime('BTCUSDT') == 'VOLATILE'

def test_trending_blocks_mean_reversion(rf):
    feed(rf, 'BTCUSDT', 100.5, 99.5, 100.0, 50)
    allowed, _ = rf.is_allowed('BTCUSDT', 'averages_depth')
    assert not allowed

def test_trending_allows_trend_following(rf):
    feed(rf, 'BTCUSDT', 100.5, 99.5, 100.0, 50)
    allowed, mult = rf.is_allowed('BTCUSDT', 'all_three')
    assert allowed
    assert mult == pytest.approx(1.1)

def test_ranging_allows_mean_reversion(rf):
    feed(rf, 'BTCUSDT', 100.1, 99.9, 100.0, 50)
    allowed, mult = rf.is_allowed('BTCUSDT', 'averages_depth')
    assert allowed
    assert mult == pytest.approx(1.1)

def test_volatile_blocks_non_all_three(rf):
    feed(rf, 'BTCUSDT', 101.0, 99.0, 100.0, 50)
    allowed, _ = rf.is_allowed('BTCUSDT', 'averages_vector')
    assert not allowed
