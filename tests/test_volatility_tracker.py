# tests/test_volatility_tracker.py
import pytest
import time

from core.volatility_tracker import VolatilityTracker


class TestVolatilityTracker:
    def setup_method(self):
        self.tracker = VolatilityTracker(window_seconds=10)

    def test_empty(self):
        assert self.tracker.get_volatility("BTCUSDT") == 0.0
        assert self.tracker.get_vwap("BTCUSDT") == 0.0
        assert self.tracker.get_trade_count("BTCUSDT") == 0
        assert self.tracker.get_volume_sum("BTCUSDT") == 0.0

    def test_single_price(self):
        self.tracker.update("BTCUSDT", 60000, time.time())
        assert self.tracker.get_volatility("BTCUSDT") == 0.0

    def test_volatility_calculation(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 60600, now + 1)
        self.tracker.update("BTCUSDT", 60300, now + 2)

        vol = self.tracker.get_volatility("BTCUSDT")
        assert vol == pytest.approx(1.0, rel=0.01)

    def test_dead_market_true(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 60001, now + 1)

        assert self.tracker.is_dead_market("BTCUSDT", threshold=0.05)

    def test_dead_market_false(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 61000, now + 1)

        assert not self.tracker.is_dead_market("BTCUSDT", threshold=0.05)

    def test_chaos_true(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 63600, now + 1)

        assert self.tracker.is_chaos("BTCUSDT", threshold=5.0)

    def test_chaos_false(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 60100, now + 1)

        assert not self.tracker.is_chaos("BTCUSDT", threshold=5.0)

    def test_adaptive_trailing_low_vol(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 60010, now + 1)

        spread = self.tracker.get_adaptive_trailing_spread(
            "BTCUSDT", base_spread=0.3
        )
        assert spread == 0.3

    def test_adaptive_trailing_high_vol(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 61200, now + 1)

        spread = self.tracker.get_adaptive_trailing_spread(
            "BTCUSDT", base_spread=0.3
        )
        assert spread > 0.3
        assert spread <= 0.9

    def test_eviction_old_data(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 50000, now - 20)
        self.tracker.update("BTCUSDT", 55000, now - 15)
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 60100, now + 1)

        vol = self.tracker.get_volatility("BTCUSDT")
        # Только данные в окне (10 сек), старые удалены
        assert vol < 1.0

    def test_vwap_with_volume(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now, volume=100)
        self.tracker.update("BTCUSDT", 61000, now + 1, volume=300)

        vwap = self.tracker.get_vwap("BTCUSDT")
        assert vwap == pytest.approx(60750, rel=0.01)

    def test_vwap_without_volume(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 61000, now + 1)

        vwap = self.tracker.get_vwap("BTCUSDT")
        assert vwap == pytest.approx(60500, rel=0.01)

    def test_trade_count(self):
        now = time.time()
        for i in range(10):
            self.tracker.update("BTCUSDT", 60000 + i, now + i * 0.1)

        assert self.tracker.get_trade_count("BTCUSDT") == 10

    def test_volume_sum(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now, volume=100)
        self.tracker.update("BTCUSDT", 60000, now + 1, volume=200)

        assert self.tracker.get_volume_sum("BTCUSDT") == 300

    def test_multiple_symbols(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 60600, now + 1)
        self.tracker.update("ETHUSDT", 3000, now)
        self.tracker.update("ETHUSDT", 3090, now + 1)

        btc_vol = self.tracker.get_volatility("BTCUSDT")
        eth_vol = self.tracker.get_volatility("ETHUSDT")

        assert btc_vol == pytest.approx(1.0, rel=0.01)
        assert eth_vol == pytest.approx(3.0, rel=0.01)

    def test_stats(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now, volume=100)
        self.tracker.update("BTCUSDT", 60600, now + 1, volume=200)

        stats = self.tracker.get_stats("BTCUSDT")

        assert 'volatility_pct' in stats
        assert 'vwap' in stats
        assert 'trade_count' in stats
        assert 'volume_sum' in stats
        assert 'is_dead' in stats
        assert 'is_chaos' in stats
        assert stats['trade_count'] == 2
        assert stats['volume_sum'] == 300

    def test_all_stats(self):
        now = time.time()
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("ETHUSDT", 3000, now)

        all_stats = self.tracker.get_all_stats()
        assert 'BTCUSDT' in all_stats
        assert 'ETHUSDT' in all_stats
