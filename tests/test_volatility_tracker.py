# tests/test_volatility_tracker.py
import pytest
import time
from core.volatility_tracker import VolatilityTracker


class TestVolatilityTracker:
    def setup_method(self):
        self.tracker = VolatilityTracker(window_seconds=10)
    
    def test_empty(self):
        assert self.tracker.get_volatility("BTCUSDT") == 0.0
        assert not self.tracker.is_dead_market("BTCUSDT")
        assert not self.tracker.is_chaos("BTCUSDT")
    
    def test_volatility_calculation(self):
        now = time.time()
        
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 60600, now + 1)  # +1%
        self.tracker.update("BTCUSDT", 60300, now + 2)
        
        vol = self.tracker.get_volatility("BTCUSDT")
        assert vol == pytest.approx(1.0, rel=0.01)
    
    def test_dead_market(self):
        now = time.time()
        
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 60001, now + 1)
        
        assert self.tracker.is_dead_market(
            "BTCUSDT", threshold=0.05
        )
    
    def test_chaos(self):
        now = time.time()
        
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 63600, now + 1)  # +6%
        
        assert self.tracker.is_chaos(
            "BTCUSDT", threshold=5.0
        )
    
    def test_adaptive_trailing(self):
        now = time.time()
        
        # Высокая волатильность
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 61200, now + 1)  # +2%
        
        spread = self.tracker.get_adaptive_trailing_spread(
            "BTCUSDT", base_spread=0.3
        )
        # volatility=2%, adaptive = max(0.3, 2*0.5) = max(0.3, 1.0) = 1.0
        assert spread == 0.9  # min(1.0, 0.3*3)
    
    def test_eviction(self):
        now = time.time()
        
        # Старые данные
        self.tracker.update("BTCUSDT", 50000, now - 20)
        self.tracker.update("BTCUSDT", 55000, now - 15)
        
        # Новые данные
        self.tracker.update("BTCUSDT", 60000, now)
        self.tracker.update("BTCUSDT", 60100, now + 1)
        
        # Волатильность считается только по данным в окне
        vol = self.tracker.get_volatility("BTCUSDT")
        assert vol < 1.0  # Не 20% от старых данных
    
    def test_vwap(self):
        now = time.time()
        
        self.tracker.update("BTCUSDT", 60000, now, volume=100)
        self.tracker.update("BTCUSDT", 61000, now + 1, volume=300)
        
        vwap = self.tracker.get_vwap("BTCUSDT")
        # VWAP = (60000*100 + 61000*300) / 400 = 60750
        assert vwap == pytest.approx(60750, rel=0.01)
