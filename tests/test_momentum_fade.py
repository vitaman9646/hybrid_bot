"""Tests for MomentumFadeExit"""
import time
import pytest
from models.signals import Direction
from core.momentum_fade import MomentumFadeExit, MomentumState


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_fade(config=None):
    cfg = {
        'window_s': 8.0,
        'fade_threshold': 0.25,
        'min_profit_pct': 0.10,
        'confirm_s': 3.0,
        'min_ticks': 5,
        'max_loss_pct': 0.05,
    }
    if config:
        cfg.update(config)
    return MomentumFadeExit(cfg)


def feed_ticks(fade, symbol, prices, direction=None, base_ts=None):
    """Загружаем тики с равномерными временными метками."""
    base_ts = base_ts or time.time()
    for i, price in enumerate(prices):
        fade.update(symbol, price, base_ts + i * 0.1)


def make_long_profitable(entry=80000, profit_pct=0.3):
    """Текущая цена выше entry на profit_pct%."""
    return entry * (1 + profit_pct / 100)


def make_short_profitable(entry=80000, profit_pct=0.3):
    return entry * (1 - profit_pct / 100)


# ── update / get_momentum ─────────────────────────────────────────────────────

class TestGetMomentum:

    def test_returns_one_with_insufficient_ticks(self):
        fade = make_fade({'min_ticks': 20})
        fade.update('BTCUSDT', 80000)
        m = fade.get_momentum('BTCUSDT', Direction.LONG)
        assert m == 1.0

    def test_strong_upward_momentum_long(self):
        fade = make_fade()
        now = time.time()
        # Цена уверенно растёт
        prices = [80000 + i * 10 for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        m = fade.get_momentum('BTCUSDT', Direction.LONG)
        assert m > 0.5

    def test_weak_momentum_flat_market(self):
        fade = make_fade()
        now = time.time()
        # Цена почти не движется
        prices = [80000 + (i % 3) for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        m = fade.get_momentum('BTCUSDT', Direction.LONG)
        assert m < 0.6

    def test_direction_score_long_vs_short(self):
        fade = make_fade()
        now = time.time()
        # Цена падает — хорошо для SHORT, плохо для LONG
        prices = [80000 - i * 5 for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        m_short = fade.get_momentum('BTCUSDT', Direction.SHORT)
        m_long = fade.get_momentum('BTCUSDT', Direction.LONG)
        assert m_short > m_long

    def test_momentum_range(self):
        fade = make_fade()
        now = time.time()
        prices = [80000 + i * 2 for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        m = fade.get_momentum('BTCUSDT', Direction.LONG)
        assert 0.0 <= m <= 1.0

    def test_unknown_symbol_returns_one(self):
        fade = make_fade()
        m = fade.get_momentum('UNKNOWN', Direction.LONG)
        assert m == 1.0


# ── should_exit ───────────────────────────────────────────────────────────────

class TestShouldExit:

    def test_no_exit_insufficient_profit(self):
        fade = make_fade({'min_profit_pct': 0.2})
        now = time.time()
        # Слабый momentum
        prices = [80000] * 20
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        current = 80000 * 1.001  # только 0.1% прибыли
        result = fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        assert result is False

    def test_no_exit_in_loss(self):
        fade = make_fade()
        now = time.time()
        prices = [80000] * 20
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        current = 79900  # убыток
        result = fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        assert result is False

    def test_no_exit_strong_momentum(self):
        fade = make_fade({'confirm_s': 0.1})
        now = time.time()
        # Сильный рост
        prices = [80000 + i * 20 for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        current = make_long_profitable()
        result = fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        assert result is False

    def test_exit_after_confirm_s(self):
        fade = make_fade({'confirm_s': 0.1, 'min_ticks': 5})
        now = time.time()
        # Плоский рынок — низкий momentum
        prices = [80000 + (i % 2) for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        current = make_long_profitable(profit_pct=0.3)

        # Первый вызов — триггер
        fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        # Ждём confirm_s
        state = fade._get_state('BTCUSDT')
        state.fade_triggered_at = time.time() - 0.5  # симулируем ожидание

        result = fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        assert result is True

    def test_fade_reset_when_momentum_recovers(self):
        fade = make_fade({'confirm_s': 10.0, 'min_ticks': 5})
        now = time.time()

        # Слабый momentum
        prices = [80000] * 20
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        current = make_long_profitable()
        fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        state = fade._get_state('BTCUSDT')
        assert state.fade_triggered_at > 0

        # Momentum восстанавливается
        prices2 = [80000 + i * 30 for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices2, base_ts=now - 8)
        fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        assert state.fade_triggered_at == 0.0

    def test_short_exit(self):
        fade = make_fade({'confirm_s': 0.1, 'min_ticks': 5})
        now = time.time()
        prices = [80000 + (i % 2) for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        current = make_short_profitable(profit_pct=0.3)

        fade.should_exit('BTCUSDT', Direction.SHORT, 80000, current)
        state = fade._get_state('BTCUSDT')
        state.fade_triggered_at = time.time() - 0.5

        result = fade.should_exit('BTCUSDT', Direction.SHORT, 80000, current)
        assert result is True

    def test_no_double_signal(self):
        """exit_signaled не сбрасывается пока momentum не восстановится."""
        fade = make_fade({'confirm_s': 0.1, 'min_ticks': 5})
        now = time.time()
        prices = [80000 + (i % 2) for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        current = make_long_profitable()

        fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        state = fade._get_state('BTCUSDT')
        state.fade_triggered_at = time.time() - 0.5

        r1 = fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        r2 = fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        assert r1 is True
        assert r2 is True  # продолжает сигналить пока fade активен
        assert fade._stats['exits_signaled'] == 1  # но счётчик не дублируется


# ── reset ─────────────────────────────────────────────────────────────────────

class TestReset:

    def test_reset_clears_ticks(self):
        fade = make_fade()
        feed_ticks(fade, 'BTCUSDT', [80000, 80100, 80200])
        fade.reset('BTCUSDT')
        state = fade._get_state('BTCUSDT')
        assert len(state.ticks) == 0

    def test_reset_clears_fade_state(self):
        fade = make_fade()
        state = fade._get_state('BTCUSDT')
        state.fade_triggered_at = time.time()
        state.exit_signaled = True
        fade.reset('BTCUSDT')
        assert state.fade_triggered_at == 0.0
        assert state.exit_signaled is False


# ── get_stats ─────────────────────────────────────────────────────────────────

class TestStats:

    def test_global_stats(self):
        fade = make_fade()
        feed_ticks(fade, 'BTCUSDT', [80000, 80100])
        feed_ticks(fade, 'ETHUSDT', [3000, 3010])
        stats = fade.get_stats()
        assert stats['updates'] == 4
        assert stats['symbols_tracked'] == 2

    def test_symbol_stats(self):
        fade = make_fade()
        feed_ticks(fade, 'BTCUSDT', [80000, 80100, 80200])
        stats = fade.get_stats('BTCUSDT')
        assert stats['symbol'] == 'BTCUSDT'
        assert stats['ticks_buffered'] == 3

    def test_exits_signaled_counter(self):
        fade = make_fade({'confirm_s': 0.1, 'min_ticks': 5})
        now = time.time()
        prices = [80000 + (i % 2) for i in range(20)]
        feed_ticks(fade, 'BTCUSDT', prices, base_ts=now - 8)
        current = make_long_profitable()

        fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)
        state = fade._get_state('BTCUSDT')
        state.fade_triggered_at = time.time() - 0.5
        fade.should_exit('BTCUSDT', Direction.LONG, 80000, current)

        assert fade.get_stats()['exits_signaled'] == 1
