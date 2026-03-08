"""
tests/test_position_manager.py

Запуск: pytest tests/test_position_manager.py -v
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass
from enum import Enum

from core.position_manager import PositionManager, Position, PositionState


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

def make_config(**overrides) -> dict:
    cfg = {
        'max_positions': 3,
        'order': {
            'size_usdt': 100.0,
            'stop_loss':   {'percent': 1.5, 'type': 'market'},
            'take_profit': {'percent': 0.8},
            'trailing':    {'enabled': True, 'adaptive': False, 'spread': 0.3},
            'max_drawdown_pct': 5.0,
        },
    }
    cfg.update(overrides)
    return cfg


def make_executor(
    place_ok: bool = True,
    tp_id: str = 'tp-001',
    sl_id: str = 'sl-001',
    cancel_ok: bool = True,
    modify_ok: bool = True,
) -> MagicMock:
    ex = MagicMock()
    ex.place_order = AsyncMock(
        return_value={'orderId': 'entry-001'} if place_ok else None
    )
    ex.place_tp = AsyncMock(return_value=tp_id if place_ok else None)
    ex.place_sl = AsyncMock(return_value=sl_id if place_ok else None)
    ex.cancel_order = AsyncMock(return_value=cancel_ok)
    ex.modify_order = AsyncMock(return_value=modify_ok)
    return ex


def make_volatility() -> MagicMock:
    vt = MagicMock()
    vt.get_adaptive_trailing_spread = MagicMock(return_value=0.3)
    return vt


def make_signal(
    symbol: str = 'BTCUSDT',
    direction: str = 'long',
    entry_price: float = 50000.0,
    tp_price: float = 50400.0,
    size_usdt: float = 0.0,
) -> MagicMock:
    sig = MagicMock()
    sig.symbol = symbol
    sig.direction = MagicMock()
    sig.direction.value = direction
    sig.entry_price = entry_price
    sig.tp_price = tp_price
    sig.size_usdt = size_usdt
    return sig


def make_pm(
    config: dict = None,
    executor=None,
    volatility=None,
    calc_qty_result: float = 0.002,
) -> PositionManager:
    pm = PositionManager(
        config=config or make_config(),
        executor=executor or make_executor(),
        volatility_tracker=volatility or make_volatility(),
    )
    # Мокаем _calc_qty чтобы не ходить на биржу
    pm._calc_qty = AsyncMock(return_value=calc_qty_result)
    return pm


# ---------------------------------------------------------------------------
# 1. Блокировки при открытии
# ---------------------------------------------------------------------------

class TestOpenPositionBlocked:

    @pytest.mark.asyncio
    async def test_blocked_already_open(self):
        pm = make_pm()
        sig = make_signal('BTCUSDT')
        await pm.open_position(sig)
        # Второй вход по тому же символу
        result = await pm.open_position(sig)
        assert result is None
        assert pm._stats['blocked_already_open'] == 1

    @pytest.mark.asyncio
    async def test_blocked_max_positions(self):
        pm = make_pm(config=make_config(max_positions=2))
        await pm.open_position(make_signal('BTCUSDT'))
        await pm.open_position(make_signal('ETHUSDT'))
        # Третий — блок
        result = await pm.open_position(make_signal('SOLUSDT'))
        assert result is None
        assert pm._stats['blocked_max_positions'] == 1

    @pytest.mark.asyncio
    async def test_blocked_calc_qty_none(self):
        pm = make_pm(calc_qty_result=None)
        result = await pm.open_position(make_signal('BTCUSDT'))
        assert result is None
        assert 'BTCUSDT' not in pm._positions

    @pytest.mark.asyncio
    async def test_blocked_calc_qty_zero(self):
        pm = make_pm(calc_qty_result=0.0)
        result = await pm.open_position(make_signal('BTCUSDT'))
        assert result is None

    @pytest.mark.asyncio
    async def test_blocked_entry_order_failed(self):
        ex = make_executor(place_ok=False)
        pm = make_pm(executor=ex)
        result = await pm.open_position(make_signal('BTCUSDT'))
        assert result is None
        assert 'BTCUSDT' not in pm._positions


# ---------------------------------------------------------------------------
# 2. Успешное открытие позиции
# ---------------------------------------------------------------------------

class TestOpenPositionSuccess:

    @pytest.mark.asyncio
    async def test_open_returns_position(self):
        pm = make_pm()
        pos = await pm.open_position(make_signal('BTCUSDT', direction='long'))
        assert pos is not None
        assert pos.symbol == 'BTCUSDT'
        assert pos.direction == 'long'
        assert pos.state == PositionState.OPEN

    @pytest.mark.asyncio
    async def test_open_registers_in_positions(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT'))
        assert pm.has_position('BTCUSDT')
        assert len(pm.get_all_positions()) == 1

    @pytest.mark.asyncio
    async def test_open_increments_stats(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT'))
        assert pm._stats['total_opened'] == 1

    @pytest.mark.asyncio
    async def test_open_sets_tp_sl_order_ids(self):
        pm = make_pm()
        pos = await pm.open_position(make_signal('BTCUSDT'))
        assert pos.tp_order_id == 'tp-001'
        assert pos.sl_order_id == 'sl-001'

    @pytest.mark.asyncio
    async def test_open_short(self):
        pm = make_pm()
        pos = await pm.open_position(make_signal('ETHUSDT', direction='short', entry_price=3000.0))
        assert pos.direction == 'short'
        assert pos.tp_price < 3000.0
        assert pos.sl_price > 3000.0

    @pytest.mark.asyncio
    async def test_tp_sl_prices_correct_long(self):
        pm = make_pm()
        entry = 50000.0
        pos = await pm.open_position(make_signal('BTCUSDT', entry_price=entry, tp_price=50400.0))
        # tp_price из сигнала (50400 = +0.8%)
        assert pos.tp_price == pytest.approx(50400.0)
        # sl = entry * (1 - 1.5%)
        assert pos.sl_price == pytest.approx(entry * (1 - 1.5 / 100))

    @pytest.mark.asyncio
    async def test_multiple_different_symbols(self):
        pm = make_pm(config=make_config(max_positions=3))
        for sym in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']:
            pos = await pm.open_position(make_signal(sym))
            assert pos is not None
        assert len(pm.get_all_positions()) == 3


# ---------------------------------------------------------------------------
# 3. Trailing stop
# ---------------------------------------------------------------------------

class TestTrailingStop:

    @pytest.mark.asyncio
    async def test_trailing_stop_updates_on_new_peak_long(self):
        pm = make_pm()
        pos = await pm.open_position(make_signal('BTCUSDT', entry_price=50000.0))
        initial_stop = pos.trailing_stop_price

        # Цена выросла — trailing должен подняться
        await pm.update_price('BTCUSDT', 51000.0)
        assert pos.peak_price == pytest.approx(51000.0)
        assert pos.trailing_stop_price > initial_stop

    @pytest.mark.asyncio
    async def test_trailing_stop_not_updated_on_lower_price_long(self):
        pm = make_pm()
        pos = await pm.open_position(make_signal('BTCUSDT', entry_price=50000.0))
        await pm.update_price('BTCUSDT', 51000.0)
        stop_after_peak = pos.trailing_stop_price

        # Цена упала — stop не должен опуститься
        await pm.update_price('BTCUSDT', 50500.0)
        assert pos.trailing_stop_price == pytest.approx(stop_after_peak)

    @pytest.mark.asyncio
    async def test_trailing_stop_updates_on_new_peak_short(self):
        pm = make_pm()
        pos = await pm.open_position(
            make_signal('BTCUSDT', direction='short', entry_price=50000.0, tp_price=49600.0)
        )
        initial_stop = pos.trailing_stop_price

        # Цена упала — для short это новый peak, trailing должен опуститься
        await pm.update_price('BTCUSDT', 49000.0)
        assert pos.peak_price == pytest.approx(49000.0)
        assert pos.trailing_stop_price < initial_stop

    @pytest.mark.asyncio
    async def test_trailing_calls_modify_order(self):
        ex = make_executor()
        pm = make_pm(executor=ex)
        await pm.open_position(make_signal('BTCUSDT', entry_price=50000.0))
        await pm.update_price('BTCUSDT', 51000.0)
        # modify_order должен был вызваться
        ex.modify_order.assert_called()

    @pytest.mark.asyncio
    async def test_trailing_updates_counter(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT', entry_price=50000.0))
        await pm.update_price('BTCUSDT', 51000.0)
        await pm.update_price('BTCUSDT', 52000.0)
        assert pm._stats['trailing_updates'] >= 2


# ---------------------------------------------------------------------------
# 4. Max drawdown
# ---------------------------------------------------------------------------

class TestMaxDrawdown:

    @pytest.mark.asyncio
    async def test_max_drawdown_closes_position(self):
        cfg = make_config()
        cfg['order']['max_drawdown_pct'] = 2.0
        pm = make_pm(config=cfg)
        await pm.open_position(make_signal('BTCUSDT', entry_price=50000.0))
        # Цена упала на 3% — больше лимита 2%
        await pm.update_price('BTCUSDT', 48500.0)
        assert not pm.has_position('BTCUSDT')

    @pytest.mark.asyncio
    async def test_max_drawdown_not_triggered_within_limit(self):
        cfg = make_config()
        cfg['order']['max_drawdown_pct'] = 5.0
        pm = make_pm(config=cfg)
        await pm.open_position(make_signal('BTCUSDT', entry_price=50000.0))
        # Цена упала на 2% — в пределах лимита
        await pm.update_price('BTCUSDT', 49000.0)
        assert pm.has_position('BTCUSDT')

    @pytest.mark.asyncio
    async def test_max_drawdown_short(self):
        cfg = make_config()
        cfg['order']['max_drawdown_pct'] = 2.0
        pm = make_pm(config=cfg)
        await pm.open_position(
            make_signal('BTCUSDT', direction='short', entry_price=50000.0, tp_price=49600.0)
        )
        # Цена выросла на 3% — drawdown для short
        await pm.update_price('BTCUSDT', 51500.0)
        assert not pm.has_position('BTCUSDT')


# ---------------------------------------------------------------------------
# 5. Закрытие позиции
# ---------------------------------------------------------------------------

class TestClosePosition:

    @pytest.mark.asyncio
    async def test_close_removes_from_positions(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT'))
        result = await pm.close_position('BTCUSDT', reason='manual', current_price=50200.0)
        assert result is True
        assert not pm.has_position('BTCUSDT')

    @pytest.mark.asyncio
    async def test_close_archives_to_closed_list(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT'))
        await pm.close_position('BTCUSDT', reason='tp', current_price=50400.0)
        assert len(pm._closed_positions) == 1
        assert pm._closed_positions[0].close_reason == 'tp'

    @pytest.mark.asyncio
    async def test_close_increments_stats(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT'))
        await pm.close_position('BTCUSDT', reason='sl', current_price=49250.0)
        assert pm._stats['total_closed'] == 1
        assert pm._stats['sl_hits'] == 1

    @pytest.mark.asyncio
    async def test_close_nonexistent_returns_false(self):
        pm = make_pm()
        result = await pm.close_position('XYZUSDT', reason='manual')
        assert result is False

    @pytest.mark.asyncio
    async def test_close_cancels_tp_sl_orders(self):
        ex = make_executor()
        pm = make_pm(executor=ex)
        await pm.open_position(make_signal('BTCUSDT'))
        await pm.close_position('BTCUSDT', reason='manual', current_price=50000.0)
        assert ex.cancel_order.call_count == 2

    @pytest.mark.asyncio
    async def test_close_failed_order_keeps_position(self):
        ex = make_executor()
        # Первый вызов place_order (entry) — успех, второй (close) — фейл
        ex.place_order = AsyncMock(side_effect=[
            {'orderId': 'entry-001'},
            None,
        ])
        pm = make_pm(executor=ex)
        await pm.open_position(make_signal('BTCUSDT'))
        result = await pm.close_position('BTCUSDT', reason='manual', current_price=50000.0)
        assert result is False
        assert pm.has_position('BTCUSDT')

    @pytest.mark.asyncio
    async def test_close_double_call_ignored(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT'))
        pos = pm.get_position('BTCUSDT')
        pos.state = PositionState.CLOSING
        result = await pm.close_position('BTCUSDT', reason='manual')
        assert result is False


# ---------------------------------------------------------------------------
# 6. Sync with exchange
# ---------------------------------------------------------------------------

class TestSyncWithExchange:

    @pytest.mark.asyncio
    async def test_sync_clears_closed_positions(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT'))
        await pm.open_position(make_signal('ETHUSDT'))

        # Биржа говорит что открыт только ETHUSDT
        pm._executor._client.get_positions = MagicMock(return_value={
            'retCode': 0,
            'result': {'list': [
                {'symbol': 'ETHUSDT', 'size': '0.5'},
            ]},
        })
        await pm.sync_with_exchange()
        assert not pm.has_position('BTCUSDT')
        assert pm.has_position('ETHUSDT')

    @pytest.mark.asyncio
    async def test_sync_no_positions_skips(self):
        pm = make_pm()
        # Нет позиций — sync не должен падать
        await pm.sync_with_exchange()

    @pytest.mark.asyncio
    async def test_sync_error_handled_gracefully(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT'))
        pm._executor._client.get_positions = MagicMock(side_effect=Exception("network error"))
        # Не должно бросить исключение
        await pm.sync_with_exchange()
        assert pm.has_position('BTCUSDT')

    @pytest.mark.asyncio
    async def test_sync_retcode_nonzero_ignored(self):
        pm = make_pm()
        await pm.open_position(make_signal('BTCUSDT'))
        pm._executor._client.get_positions = MagicMock(return_value={
            'retCode': 10001,
            'result': {'list': []},
        })
        await pm.sync_with_exchange()
        # Позиция должна остаться
        assert pm.has_position('BTCUSDT')


# ---------------------------------------------------------------------------
# 7. Calc qty / helpers
# ---------------------------------------------------------------------------

class TestCalcQty:

    def test_calc_drawdown_pct_long(self):
        pm = make_pm()
        pos = Position(
            symbol='BTCUSDT', direction='long',
            entry_price=50000.0, qty=0.002,
            tp_price=50400.0, sl_price=49250.0,
        )
        # Цена упала на 2%
        assert pm._calc_drawdown_pct(pos, 49000.0) == pytest.approx(2.0)

    def test_calc_drawdown_pct_short(self):
        pm = make_pm()
        pos = Position(
            symbol='BTCUSDT', direction='short',
            entry_price=50000.0, qty=0.002,
            tp_price=49600.0, sl_price=50750.0,
        )
        # Цена выросла на 2%
        assert pm._calc_drawdown_pct(pos, 51000.0) == pytest.approx(2.0)

    def test_calc_pnl_long_profit(self):
        pm = make_pm()
        pos = Position(
            symbol='BTCUSDT', direction='long',
            entry_price=50000.0, qty=0.002,
            tp_price=50400.0, sl_price=49250.0,
            close_price=50400.0,
        )
        pnl = pm._calc_pnl(pos)
        assert pnl == pytest.approx(0.8)   # (50400-50000)/50000 * 0.002 * 50000

    def test_calc_pnl_short_profit(self):
        pm = make_pm()
        pos = Position(
            symbol='BTCUSDT', direction='short',
            entry_price=50000.0, qty=0.002,
            tp_price=49600.0, sl_price=50750.0,
            close_price=49600.0,
        )
        pnl = pm._calc_pnl(pos)
        assert pnl == pytest.approx(0.8)

    def test_calc_sl_long(self):
        pm = make_pm()
        sl = pm._calc_sl('long', 50000.0)
        assert sl == pytest.approx(50000.0 * (1 - 1.5 / 100))

    def test_calc_sl_short(self):
        pm = make_pm()
        sl = pm._calc_sl('short', 50000.0)
        assert sl == pytest.approx(50000.0 * (1 + 1.5 / 100))

    def test_calc_tp_uses_signal_if_reasonable(self):
        pm = make_pm()
        tp = pm._calc_tp('long', 50000.0, signal_tp=50400.0)
        assert tp == pytest.approx(50400.0)

    def test_calc_tp_uses_config_if_signal_unreasonable(self):
        pm = make_pm()
        # signal_tp слишком далеко (20%)
        tp = pm._calc_tp('long', 50000.0, signal_tp=60000.0)
        assert tp == pytest.approx(50000.0 * (1 + 0.8 / 100))
