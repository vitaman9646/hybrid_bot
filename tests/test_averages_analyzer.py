import pytest
import time
from analyzers.averages_analyzer import AveragesAnalyzer, TrendState
from models.signals import TradeData, Direction


def make_trade(symbol, price, qty=1.0, ts=None):
    if ts is None:
        ts = time.time()
    return TradeData(symbol=symbol, price=price, qty=qty,
                     quote_volume=price*qty, side="Buy", timestamp=ts)


def make_config(**kwargs):
    base = dict(short_period=10.0, long_period=60.0,
                min_delta_pct=0.15, oversold_delta=-0.8,
                overbought_delta=0.8)
    base.update(kwargs)
    return base


def feed_prices(analyzer, symbol, prices, base_ts=None):
    if base_ts is None:
        base_ts = time.time() - len(prices)
    last_signal = None
    for i, price in enumerate(prices):
        ts = base_ts + i
        last_signal = analyzer.on_trade(make_trade(symbol, price, ts=ts))
    return last_signal


class TestAveragesAnalyzer:

    def test_init(self):
        a = AveragesAnalyzer(make_config())
        assert a.short_period == 10.0
        assert a.long_period == 60.0

    def test_no_signal_flat(self):
        a = AveragesAnalyzer(make_config(min_delta_pct=0.15))
        feed_prices(a, "BTCUSDT", [60000.0] * 70)
        assert a.get_trend("BTCUSDT") == TrendState.FLAT

    def test_uptrend_detected(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0, min_delta_pct=0.1))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [60200.0]*10, base_ts=now)
        assert a.get_trend("BTCUSDT") == TrendState.UP

    def test_downtrend_detected(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0, min_delta_pct=0.1))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [59800.0]*10, base_ts=now)
        assert a.get_trend("BTCUSDT") == TrendState.DOWN

    def test_delta_positive_uptrend(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0, min_delta_pct=0.01))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [60300.0]*10, base_ts=now)
        assert a.get_delta("BTCUSDT") > 0

    def test_delta_negative_downtrend(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0, min_delta_pct=0.01))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [59700.0]*10, base_ts=now)
        assert a.get_delta("BTCUSDT") < 0

    def test_oversold_with_large_drop(self):
        # long_ma = (min+max)/2 = (58000+60000)/2 = 59000
        # short_ma ≈ 58000 (bucket avg)
        # delta = (58000-59000)/59000 * 100 ≈ -1.69%
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0,
            min_delta_pct=0.1, oversold_delta=-0.5))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [58000.0]*10, base_ts=now)
        delta = a.get_delta("BTCUSDT")
        assert delta < -0.5
        assert a.is_oversold("BTCUSDT") is True

    def test_overbought_with_large_rise(self):
        # long_ma = (min+max)/2 = (60000+62000)/2 = 61000
        # short_ma ≈ 62000
        # delta = (62000-61000)/61000 * 100 ≈ +1.64%
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0,
            min_delta_pct=0.1, overbought_delta=0.5))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [62000.0]*10, base_ts=now)
        delta = a.get_delta("BTCUSDT")
        assert delta > 0.5
        assert a.is_overbought("BTCUSDT") is True

    def test_not_oversold_small_drop(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0,
            min_delta_pct=0.1, oversold_delta=-0.5))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [59600.0]*10, base_ts=now)
        # delta ≈ -0.27%, не достигает -0.5%
        assert a.is_oversold("BTCUSDT") is False

    def test_allows_direction_uptrend(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0, min_delta_pct=0.1))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [60200.0]*10, base_ts=now)
        assert a.allows_direction("BTCUSDT", Direction.LONG) is True
        assert a.allows_direction("BTCUSDT", Direction.SHORT) is False

    def test_allows_direction_downtrend(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0, min_delta_pct=0.1))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [59800.0]*10, base_ts=now)
        assert a.allows_direction("BTCUSDT", Direction.SHORT) is True
        assert a.allows_direction("BTCUSDT", Direction.LONG) is False

    def test_allows_direction_flat(self):
        a = AveragesAnalyzer(make_config(min_delta_pct=0.5))
        feed_prices(a, "BTCUSDT", [60000.0] * 70)
        assert a.allows_direction("BTCUSDT", Direction.LONG) is True
        assert a.allows_direction("BTCUSDT", Direction.SHORT) is True

    def test_signal_generated_uptrend(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0, min_delta_pct=0.1))
        sigs = []
        a.on_signal(sigs.append)
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [60200.0]*10, base_ts=now)
        assert len(sigs) > 0
        assert sigs[-1].direction == Direction.LONG
        assert 0 < sigs[-1].confidence <= 1.0

    def test_signal_fields(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0, min_delta_pct=0.1))
        sigs = []
        a.on_signal(sigs.append)
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [60200.0]*10, base_ts=now)
        assert len(sigs) > 0
        s = sigs[-1]
        assert s.symbol == "BTCUSDT"
        assert s.short_ma > 0
        assert s.long_ma > 0
        assert s.timestamp > 0
        assert isinstance(s.trend_state, TrendState)

    def test_multiple_symbols_independent(self):
        a = AveragesAnalyzer(make_config(
            short_period=10.0, long_period=60.0, min_delta_pct=0.1))
        now = time.time() - 70
        feed_prices(a, "BTCUSDT",
                    [60000.0]*60 + [60200.0]*10, base_ts=now)
        feed_prices(a, "ETHUSDT",
                    [3000.0]*60 + [2980.0]*10, base_ts=now)
        assert a.get_trend("BTCUSDT") == TrendState.UP
        assert a.get_trend("ETHUSDT") == TrendState.DOWN

    def test_stats_keys(self):
        a = AveragesAnalyzer(make_config())
        feed_prices(a, "BTCUSDT", [60000.0] * 10)
        stats = a.get_stats("BTCUSDT")
        for key in ['symbol', 'trend_state', 'short_ma', 'long_ma',
                    'delta_pct', 'is_oversold', 'is_overbought',
                    'signals_generated']:
            assert key in stats

    def test_old_data_evicted(self):
        a = AveragesAnalyzer(make_config(long_period=10.0))
        now = time.time()
        for i in range(20):
            a.on_trade(make_trade("BTCUSDT", 60000, ts=now - 100 + i))
        for i in range(5):
            a.on_trade(make_trade("BTCUSDT", 60000, ts=now + i))
        assert len(a._prices["BTCUSDT"]) <= 15
