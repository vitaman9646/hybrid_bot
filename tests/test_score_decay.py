import pytest
import time
import math
from core.score_decay import ScoreDecay, TAU, MIN_CONFIDENCE

@pytest.fixture
def sd():
    return ScoreDecay()

def test_fresh_signal(sd):
    """Новый сигнал — без затухания"""
    conf = sd.apply('BTCUSDT', 'all_three', 0.8)
    assert conf == pytest.approx(0.8, rel=0.05)

def test_decay_formula(sd):
    """Проверяем формулу e^(-t/tau)"""
    sd.register_signal('BTCUSDT')
    sd._signal_times['BTCUSDT'] = time.time() - TAU['all_three']  # прошло tau секунд
    result = sd.get_decayed('BTCUSDT', 'all_three', 1.0)
    assert result.current_confidence == pytest.approx(1/math.e, rel=0.01)

def test_dead_signal(sd):
    """Старый сигнал возвращает 0"""
    sd.register_signal('BTCUSDT')
    sd._signal_times['BTCUSDT'] = time.time() - 300  # 5 минут назад
    conf = sd.apply('BTCUSDT', 'all_three', 0.8)
    assert conf == 0.0

def test_clear(sd):
    """После clear — сигнал как новый"""
    sd.register_signal('BTCUSDT')
    sd._signal_times['BTCUSDT'] = time.time() - 300
    sd.clear('BTCUSDT')
    conf = sd.apply('BTCUSDT', 'all_three', 0.8)
    assert conf == pytest.approx(0.8, rel=0.05)

def test_different_tau(sd):
    """vector_depth затухает быстрее all_three"""
    sd.register_signal('BTCUSDT')
    sd.register_signal('ETHUSDT')
    age = 15.0
    sd._signal_times['BTCUSDT'] = time.time() - age
    sd._signal_times['ETHUSDT'] = time.time() - age
    r1 = sd.get_decayed('BTCUSDT', 'all_three', 1.0)
    r2 = sd.get_decayed('ETHUSDT', 'vector_depth', 1.0)
    assert r2.current_confidence < r1.current_confidence

def test_is_alive(sd):
    result = sd.get_decayed('BTCUSDT', 'all_three', 0.8)
    assert result.is_alive
