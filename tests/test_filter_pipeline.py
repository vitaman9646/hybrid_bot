from __future__ import annotations

import asyncio
import time
import pytest
from unittest.mock import MagicMock, AsyncMock

from core.filter_pipeline import FilterPipeline, FilterResult
from models.signals import Direction


def make_signal(symbol='BTCUSDT', direction='long', price=50000.0, confidence=0.7):
    sig = MagicMock()
    sig.symbol = symbol
    sig.direction = MagicMock()
    sig.direction.value = direction
    sig.entry_price = price
    sig.confidence = confidence
    return sig


def make_pipeline(**kwargs):
    config = {'filter_pipeline': kwargs}
    return FilterPipeline(config)


# ------------------------------------------------------------------
# TimeOfDay filter
# ------------------------------------------------------------------

def test_time_filter_blocks_in_window(monkeypatch):
    from datetime import datetime, timezone
    monkeypatch.setattr(
        'core.filter_pipeline.datetime',
        type('dt', (), {
            'now': staticmethod(lambda tz=None: type('t', (), {'hour': 3})())
        })
    )
    fp = make_pipeline(time_filter_enabled=True, time_block_start_utc=2, time_block_end_utc=6)
    result = fp._check_time_of_day()
    assert not result.passed
    assert 'time_of_day' in result.reason


def test_time_filter_passes_outside_window(monkeypatch):
    monkeypatch.setattr(
        'core.filter_pipeline.datetime',
        type('dt', (), {
            'now': staticmethod(lambda tz=None: type('t', (), {'hour': 10})())
        })
    )
    fp = make_pipeline(time_filter_enabled=True, time_block_start_utc=2, time_block_end_utc=6)
    result = fp._check_time_of_day()
    assert result.passed


def test_time_filter_boundary_start(monkeypatch):
    monkeypatch.setattr(
        'core.filter_pipeline.datetime',
        type('dt', (), {
            'now': staticmethod(lambda tz=None: type('t', (), {'hour': 2})())
        })
    )
    fp = make_pipeline(time_filter_enabled=True, time_block_start_utc=2, time_block_end_utc=6)
    result = fp._check_time_of_day()
    assert not result.passed


def test_time_filter_boundary_end(monkeypatch):
    monkeypatch.setattr(
        'core.filter_pipeline.datetime',
        type('dt', (), {
            'now': staticmethod(lambda tz=None: type('t', (), {'hour': 6})())
        })
    )
    fp = make_pipeline(time_filter_enabled=True, time_block_start_utc=2, time_block_end_utc=6)
    result = fp._check_time_of_day()
    assert result.passed  # 6:00 уже не блокируется


# ------------------------------------------------------------------
# Delta filter
# ------------------------------------------------------------------

def test_delta_passes_strong_long():
    fp = make_pipeline(delta_filter_enabled=True, min_delta_usdt=10000)
    now = time.time()
    # Сильный buy delta
    fp._delta_window['BTCUSDT'] = [(now, 50000), (now, 30000)]
    result = fp._check_delta('BTCUSDT', 'long')
    assert result.passed


def test_delta_blocks_weak_long():
    fp = make_pipeline(delta_filter_enabled=True, min_delta_usdt=100000)
    now = time.time()
    fp._delta_window['BTCUSDT'] = [(now, 5000)]
    result = fp._check_delta('BTCUSDT', 'long')
    assert not result.passed


def test_delta_blocks_wrong_direction():
    fp = make_pipeline(delta_filter_enabled=True, min_delta_usdt=10000)
    now = time.time()
    # Отрицательная delta (sell pressure) для long сигнала
    fp._delta_window['BTCUSDT'] = [(now, -50000)]
    result = fp._check_delta('BTCUSDT', 'long')
    assert not result.passed


def test_delta_passes_strong_short():
    fp = make_pipeline(delta_filter_enabled=True, min_delta_usdt=10000)
    now = time.time()
    fp._delta_window['SOLUSDT'] = [(now, -50000)]
    result = fp._check_delta('SOLUSDT', 'short')
    assert result.passed


def test_delta_empty_window_passes():
    fp = make_pipeline(delta_filter_enabled=True, min_delta_usdt=10000)
    result = fp._check_delta('BTCUSDT', 'long')
    assert result.passed  # нет данных — не блокируем


# ------------------------------------------------------------------
# Volume filter
# ------------------------------------------------------------------

def test_volume_passes_high():
    fp = make_pipeline(volume_filter_enabled=True, min_volume_usdt=100000)
    now = time.time()
    fp._volume_window['ETHUSDT'] = [(now, 200000), (now, 150000)]
    result = fp._check_volume('ETHUSDT')
    assert result.passed


def test_volume_blocks_low():
    fp = make_pipeline(volume_filter_enabled=True, min_volume_usdt=500000)
    now = time.time()
    fp._volume_window['ETHUSDT'] = [(now, 1000)]
    result = fp._check_volume('ETHUSDT')
    assert not result.passed


def test_volume_empty_window_passes():
    fp = make_pipeline(volume_filter_enabled=True, min_volume_usdt=100000)
    result = fp._check_volume('BTCUSDT')
    assert result.passed


# ------------------------------------------------------------------
# Correlation filter
# ------------------------------------------------------------------

def test_correlation_blocks_same_direction():
    fp = make_pipeline(
        correlation_filter_enabled=True,
        correlation_window_sec=60,
        correlated_pairs=[['BTCUSDT', 'ETHUSDT']],
    )
    # Регистрируем BTC long
    fp._register_signal('BTCUSDT', 'long')
    # ETH long должен быть заблокирован
    result = fp._check_correlation('ETHUSDT', 'long')
    assert not result.passed
    assert 'BTCUSDT' in result.reason


def test_correlation_passes_opposite_direction():
    fp = make_pipeline(
        correlation_filter_enabled=True,
        correlation_window_sec=60,
        correlated_pairs=[['BTCUSDT', 'ETHUSDT']],
    )
    fp._register_signal('BTCUSDT', 'long')
    result = fp._check_correlation('ETHUSDT', 'short')
    assert result.passed


def test_correlation_passes_different_group():
    fp = make_pipeline(
        correlation_filter_enabled=True,
        correlation_window_sec=60,
        correlated_pairs=[['BTCUSDT', 'ETHUSDT']],
    )
    fp._register_signal('BTCUSDT', 'long')
    result = fp._check_correlation('SOLUSDT', 'long')
    assert result.passed


def test_correlation_expires():
    fp = make_pipeline(
        correlation_filter_enabled=True,
        correlation_window_sec=1,
        correlated_pairs=[['BTCUSDT', 'ETHUSDT']],
    )
    # Регистрируем старый сигнал
    from core.filter_pipeline import _SymbolDirection
    fp._recent_signals.append(
        _SymbolDirection('BTCUSDT', 'long', timestamp=time.time() - 10)
    )
    result = fp._check_correlation('ETHUSDT', 'long')
    assert result.passed  # устарел


# ------------------------------------------------------------------
# add_trade и evict
# ------------------------------------------------------------------

def test_add_trade_accumulates_delta():
    fp = make_pipeline()
    now = time.time()
    fp.add_trade('BTCUSDT', 50000.0, 1.0, 'Buy', now)
    fp.add_trade('BTCUSDT', 50000.0, 0.5, 'Sell', now)
    window = fp._delta_window['BTCUSDT']
    deltas = [d for _, d in window]
    assert sum(deltas) == pytest.approx(25000.0)  # 50000 - 25000


def test_add_trade_evicts_old():
    fp = make_pipeline()
    fp._delta_window_sec = 1
    old_ts = time.time() - 10
    fp._delta_window['BTCUSDT'] = [(old_ts, 99999)]
    fp.add_trade('BTCUSDT', 100.0, 1.0, 'Buy', time.time())
    # Старая запись должна быть выброшена
    assert len(fp._delta_window['BTCUSDT']) == 1


# ------------------------------------------------------------------
# check() — интеграционный
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_passes_clean_signal(monkeypatch):
    monkeypatch.setattr(
        'core.filter_pipeline.datetime',
        type('dt', (), {
            'now': staticmethod(lambda tz=None: type('t', (), {'hour': 10})())
        })
    )
    fp = make_pipeline(
        time_filter_enabled=True,
        funding_filter_enabled=False,
        mark_filter_enabled=False,
        delta_filter_enabled=False,
        volume_filter_enabled=False,
        correlation_filter_enabled=False,
    )
    sig = make_signal()
    result = await fp.check(sig)
    assert result.passed
    assert fp._stats['passed'] == 1
    assert fp._stats['blocked'] == 0


@pytest.mark.asyncio
async def test_check_blocked_by_time(monkeypatch):
    monkeypatch.setattr(
        'core.filter_pipeline.datetime',
        type('dt', (), {
            'now': staticmethod(lambda tz=None: type('t', (), {'hour': 3})())
        })
    )
    fp = make_pipeline(time_filter_enabled=True, funding_filter_enabled=False, mark_filter_enabled=False)
    sig = make_signal()
    result = await fp.check(sig)
    assert not result.passed
    assert fp._stats['blocked'] == 1


@pytest.mark.asyncio
async def test_check_penalty_reduces_confidence(monkeypatch):
    monkeypatch.setattr(
        'core.filter_pipeline.datetime',
        type('dt', (), {
            'now': staticmethod(lambda tz=None: type('t', (), {'hour': 10})())
        })
    )
    fp = make_pipeline(
        time_filter_enabled=False,
        funding_filter_enabled=False,
        mark_filter_enabled=False,
        delta_filter_enabled=True,
        min_delta_usdt=999999999,  # всегда штраф
        volume_filter_enabled=False,
        correlation_filter_enabled=False,
        delta_penalty=0.5,
    )
    from types import SimpleNamespace
    sig = SimpleNamespace(
        symbol='BTCUSDT',
        direction=SimpleNamespace(value='long'),
        entry_price=50000.0,
        confidence=0.8,
    )
    # Добавляем трейд чтобы window не был пустым — слабая delta → штраф
    import time as _time
    fp._delta_window['BTCUSDT'] = [(_time.time(), 1.0)]  # delta << min_delta_usdt
    result = await fp.check(sig)
    assert result.passed
    assert sig.confidence == pytest.approx(0.4)


@pytest.mark.asyncio
async def test_check_stats_tracking(monkeypatch):
    monkeypatch.setattr(
        'core.filter_pipeline.datetime',
        type('dt', (), {
            'now': staticmethod(lambda tz=None: type('t', (), {'hour': 10})())
        })
    )
    fp = make_pipeline(
        time_filter_enabled=False,
        funding_filter_enabled=False,
        mark_filter_enabled=False,
        delta_filter_enabled=False,
        volume_filter_enabled=False,
        correlation_filter_enabled=False,
    )
    for _ in range(3):
        await fp.check(make_signal())
    assert fp._stats['total'] == 3
    assert fp._stats['passed'] == 3
