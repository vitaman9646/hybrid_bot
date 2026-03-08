"""
tests/test_order_executor.py — тесты для OrderExecutor.place_sl race condition фикса
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch, call
from execution.order_executor import OrderExecutor
from core.latency_guard import LatencyGuard


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_executor() -> OrderExecutor:
    config = {
        'testnet': True,
        'api_key': 'test',
        'api_secret': 'test',
        'rate_limit': 950,
        'max_retries': 3,
        'retry_delay': 0.0,
    }
    latency_guard = MagicMock(spec=LatencyGuard)
    latency_guard.current_level = MagicMock()
    latency_guard.current_level.value = 'normal'
    executor = OrderExecutor(config, latency_guard)
    return executor


def make_ticker_response(last_price: float) -> dict:
    return {
        'retCode': 0,
        'result': {
            'list': [{'lastPrice': str(last_price)}]
        }
    }


def make_sl_success_response(order_id: str = 'sl_order_123') -> dict:
    return {
        'retCode': 0,
        'result': {'orderId': order_id},
    }


# ---------------------------------------------------------------------------
# SL Race Condition Tests
# ---------------------------------------------------------------------------

class TestPlaceSLRaceCondition:

    @pytest.mark.asyncio
    async def test_sl_long_placed_when_price_above_sl(self):
        """Long SL (Sell) размещается когда цена ВЫШЕ trigger"""
        executor = make_executor()

        with patch.object(
            executor, '_client'
        ) as mock_client:
            # Текущая цена выше SL — всё ок
            mock_client.get_tickers = MagicMock(
                return_value=make_ticker_response(50000.0)
            )
            mock_client.place_order = MagicMock(
                return_value=make_sl_success_response()
            )

            result = await executor.place_sl(
                symbol='BTCUSDT',
                side='Sell',         # long позиция → SL это Sell
                trigger_price=49000.0,  # SL ниже текущей цены
                qty=0.001,
            )
            assert result == 'sl_order_123'
            mock_client.place_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_sl_long_skipped_when_price_below_sl(self):
        """Long SL (Sell) НЕ размещается когда цена УЖЕ НИЖЕ trigger (race condition)"""
        executor = make_executor()

        with patch.object(executor, '_client') as mock_client:
            # Цена упала ниже SL — race condition
            mock_client.get_tickers = MagicMock(
                return_value=make_ticker_response(48000.0)
            )
            mock_client.place_order = MagicMock()

            result = await executor.place_sl(
                symbol='BTCUSDT',
                side='Sell',
                trigger_price=49000.0,  # trigger > last_price → skip
                qty=0.001,
            )
            assert result == ''
            mock_client.place_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_sl_short_placed_when_price_below_sl(self):
        """Short SL (Buy) размещается когда цена НИЖЕ trigger"""
        executor = make_executor()

        with patch.object(executor, '_client') as mock_client:
            # Текущая цена ниже SL — всё ок
            mock_client.get_tickers = MagicMock(
                return_value=make_ticker_response(50000.0)
            )
            mock_client.place_order = MagicMock(
                return_value=make_sl_success_response('sl_short_456')
            )

            result = await executor.place_sl(
                symbol='BTCUSDT',
                side='Buy',          # short позиция → SL это Buy
                trigger_price=51000.0,  # SL выше текущей цены
                qty=0.001,
            )
            assert result == 'sl_short_456'
            mock_client.place_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_sl_short_skipped_when_price_above_sl(self):
        """Short SL (Buy) НЕ размещается когда цена УЖЕ ВЫШЕ trigger (race condition)"""
        executor = make_executor()

        with patch.object(executor, '_client') as mock_client:
            # Цена выросла выше SL — race condition
            mock_client.get_tickers = MagicMock(
                return_value=make_ticker_response(52000.0)
            )
            mock_client.place_order = MagicMock()

            result = await executor.place_sl(
                symbol='BTCUSDT',
                side='Buy',
                trigger_price=51000.0,  # trigger < last_price → skip
                qty=0.001,
            )
            assert result == ''
            mock_client.place_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_sl_placed_when_price_equals_sl(self):
        """SL НЕ размещается когда цена РАВНА trigger (граничный случай)"""
        executor = make_executor()

        with patch.object(executor, '_client') as mock_client:
            mock_client.get_tickers = MagicMock(
                return_value=make_ticker_response(49000.0)
            )
            mock_client.place_order = MagicMock()

            result = await executor.place_sl(
                symbol='BTCUSDT',
                side='Sell',
                trigger_price=49000.0,  # trigger == last_price → skip
                qty=0.001,
            )
            assert result == ''
            mock_client.place_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_sl_proceeds_when_ticker_check_fails(self):
        """Если ticker check упал — SL всё равно размещается (fail-safe)"""
        executor = make_executor()

        with patch.object(executor, '_client') as mock_client:
            # Ticker недоступен
            mock_client.get_tickers = MagicMock(side_effect=Exception("network error"))
            mock_client.place_order = MagicMock(
                return_value=make_sl_success_response()
            )

            result = await executor.place_sl(
                symbol='BTCUSDT',
                side='Sell',
                trigger_price=49000.0,
                qty=0.001,
            )
            # Должен попробовать разместить несмотря на ошибку проверки
            assert result == 'sl_order_123'
            mock_client.place_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_sl_trigger_direction_sell(self):
        """Sell SL использует triggerDirection=2 (Falls to trigger)"""
        executor = make_executor()

        with patch.object(executor, '_client') as mock_client:
            mock_client.get_tickers = MagicMock(
                return_value=make_ticker_response(50000.0)
            )
            mock_client.place_order = MagicMock(
                return_value=make_sl_success_response()
            )

            await executor.place_sl(
                symbol='BTCUSDT',
                side='Sell',
                trigger_price=49000.0,
                qty=0.001,
            )

            call_kwargs = mock_client.place_order.call_args.kwargs
            assert call_kwargs['triggerDirection'] == 2

    @pytest.mark.asyncio
    async def test_sl_trigger_direction_buy(self):
        """Buy SL использует triggerDirection=1 (Rises to trigger)"""
        executor = make_executor()

        with patch.object(executor, '_client') as mock_client:
            mock_client.get_tickers = MagicMock(
                return_value=make_ticker_response(50000.0)
            )
            mock_client.place_order = MagicMock(
                return_value=make_sl_success_response()
            )

            await executor.place_sl(
                symbol='BTCUSDT',
                side='Buy',
                trigger_price=51000.0,
                qty=0.001,
            )

            call_kwargs = mock_client.place_order.call_args.kwargs
            assert call_kwargs['triggerDirection'] == 1

    @pytest.mark.asyncio
    async def test_sl_uses_reduce_only(self):
        """SL ордер всегда reduceOnly=True"""
        executor = make_executor()

        with patch.object(executor, '_client') as mock_client:
            mock_client.get_tickers = MagicMock(
                return_value=make_ticker_response(50000.0)
            )
            mock_client.place_order = MagicMock(
                return_value=make_sl_success_response()
            )

            await executor.place_sl('BTCUSDT', 'Sell', 49000.0, 0.001)

            call_kwargs = mock_client.place_order.call_args.kwargs
            assert call_kwargs['reduceOnly'] is True
            assert call_kwargs['orderFilter'] == 'StopOrder'
