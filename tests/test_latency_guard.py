# tests/test_latency_guard.py
import pytest
import time
from core.latency_guard import LatencyGuard
from models.signals import LatencyLevel, OrderRTT


class TestLatencyGuard:
    def setup_method(self):
        self.config = {
            'warn_threshold_ms': 300,
            'critical_threshold_ms': 500,
            'emergency_threshold_ms': 1000,
            'check_interval': 5,
            'rtt_window_size': 100,
        }
        self.guard = LatencyGuard(self.config)
    
    def test_initial_state(self):
        assert self.guard.current_level == LatencyLevel.NORMAL
        assert self.guard.is_trading_allowed is True
        assert self.guard.is_new_entries_allowed is True
        assert self.guard.should_cancel_pending is False
        assert self.guard.should_emergency_stop is False
    
    def test_normal_latency(self):
        self.guard.record_ping_sent()
        time.sleep(0.01)  # 10ms
        self.guard.record_pong_received()
        
        assert self.guard.current_level == LatencyLevel.NORMAL
        assert self.guard.current_latency_ms < 300
    
    def test_warning_level(self):
        self.guard._ping_sent_time = time.time() - 0.35
        self.guard.record_pong_received()
        
        assert self.guard.current_level == LatencyLevel.WARNING
        assert self.guard.is_new_entries_allowed is False
        assert self.guard.should_cancel_pending is False
    
    def test_critical_level(self):
        self.guard._ping_sent_time = time.time() - 0.6
        self.guard.record_pong_received()
        
        assert self.guard.current_level == LatencyLevel.CRITICAL
        assert self.guard.should_cancel_pending is True
        assert self.guard.should_emergency_stop is False
    
    def test_emergency_level(self):
        self.guard._ping_sent_time = time.time() - 1.5
        self.guard.record_pong_received()
        
        assert self.guard.current_level == LatencyLevel.EMERGENCY
        assert self.guard.should_emergency_stop is True
    
    def test_level_change_callback(self):
        changes = []
        self.guard.on_level_change(
            lambda old, new, ms: changes.append((old, new))
        )
        
        self.guard._ping_sent_time = time.time() - 0.6
        self.guard.record_pong_received()
        
        assert len(changes) == 1
        assert changes[0] == (
            LatencyLevel.NORMAL, LatencyLevel.CRITICAL
        )
    
    def test_order_rtt_tracking(self):
        rtt = OrderRTT(
            order_id="test_1",
            symbol="BTCUSDT",
            sent_at=time.time() - 0.1,
            acknowledged_at=time.time(),
        )
        self.guard.record_order_rtt(rtt)
        
        assert self.guard.avg_order_rtt_ms > 0
        assert self.guard.p95_order_rtt_ms > 0
    
    def test_no_pong_timeout(self):
        self.guard._last_pong_time = time.time() - 60
        self.guard.check_no_pong_timeout(timeout_seconds=30)
        
        assert self.guard.current_level == LatencyLevel.EMERGENCY
    
    def test_stats(self):
        stats = self.guard.get_stats()
        assert 'current_level' in stats
        assert 'ws_latency_ms' in stats
        assert 'avg_order_rtt_ms' in stats
        assert 'p95_order_rtt_ms' in stats
