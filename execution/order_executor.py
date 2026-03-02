# execution/order_executor.py
from __future__ import annotations

import asyncio
import logging
import time
from functools import partial
from typing import Optional

from pybit.unified_trading import HTTP

from execution.rate_limiter import RateLimiter
from core.latency_guard import LatencyGuard
from models.signals import OrderRTT

logger = logging.getLogger(__name__)


class OrderExecutor:
    """
    Исполнитель ордеров для Bybit.
    - Rate limiting
    - Retry с backoff
    - Latency tracking (order RTT)
    - Slippage monitoring
    """
    
    def __init__(
        self,
        config: dict,
        latency_guard: LatencyGuard,
    ):
        self._client = HTTP(
            testnet=config.get('testnet', True),
            api_key=config.get('api_key', ''),
            api_secret=config.get('api_secret', ''),
        )
        self._rate_limiter = RateLimiter(
            max_requests=config.get('rate_limit', 950)
        )
        self._latency_guard = latency_guard
        
        # Retry
        self._max_retries = config.get('max_retries', 3)
        self._retry_delay = config.get('retry_delay', 0.1)
        
        # Slippage tracking
        self._slippage_log: list[dict] = []
        self._max_slippage_log = 1000
        
        # Order RTT tracking
        self._pending_orders: dict[str, float] = {}
    
    @property
    def rate_limiter(self) -> RateLimiter:
        return self._rate_limiter
    
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        price: Optional[float] = None,
        qty: Optional[float] = None,
        qty_usdt: Optional[float] = None,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
    ) -> Optional[dict]:
        """
        Выставить ордер с retry, rate limiting и latency tracking.
        """
        # Проверяем latency guard
        if not reduce_only and not self._latency_guard.is_new_entries_allowed:
            logger.warning(
                f"New entries blocked by latency guard "
                f"({self._latency_guard.current_level.value})"
            )
            return None
        
        await self._rate_limiter.acquire()
        
        # Конвертируем USDT в qty если нужно
        if qty is None and qty_usdt is not None:
            qty = await self._usdt_to_qty(symbol, qty_usdt)
            if qty is None:
                return None
        
        # Собираем параметры
        params = {
            'category': 'linear',
            'symbol': symbol,
            'side': side,
            'orderType': order_type,
            'qty': str(qty),
            'reduceOnly': reduce_only,
            'timeInForce': time_in_force,
        }
        
        if price is not None and order_type == 'Limit':
            params['price'] = str(price)
        
        # Retry loop
        for attempt in range(self._max_retries):
            try:
                sent_at = time.time()
                
                # Выполняем в executor чтобы не блокировать event loop
                result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    partial(self._client.place_order, **params),
                )
                
                acknowledged_at = time.time()
                rtt_ms = (acknowledged_at - sent_at) * 1000
                
                if result['retCode'] == 0:
                    order_id = result['result']['orderId']
                    
                    # Записываем RTT
                    rtt = OrderRTT(
                        order_id=order_id,
                        symbol=symbol,
                        sent_at=sent_at,
                        acknowledged_at=acknowledged_at,
                    )
                    self._latency_guard.record_order_rtt(rtt)
                    
                    logger.info(
                        f"Order placed: {symbol} {side} "
                        f"{order_type} qty={qty} "
                        f"id={order_id} "
                        f"rtt={rtt_ms:.0f}ms"
                    )
                    
                    # Slippage tracking для market orders
                    if order_type == 'Market' and price:
                        self._track_slippage(
                            symbol, price, result['result']
                        )
                    
                    return result['result']
                else:
                    error_msg = result.get('retMsg', 'Unknown error')
                    logger.error(
                        f"Order rejected: {error_msg} "
                        f"(attempt {attempt + 1}/{self._max_retries})"
                    )
                    
                    # Некоторые ошибки не стоит ретраить
                    if self._is_permanent_error(result['retCode']):
                        return None
                    
            except Exception as e:
                logger.error(
                    f"Order error (attempt {attempt + 1}): {e}"
                )
            
            if attempt < self._max_retries - 1:
                delay = self._retry_delay * (attempt + 1)
                await asyncio.sleep(delay)
        
        logger.error(
            f"Order failed after {self._max_retries} attempts: "
            f"{symbol} {side} {order_type}"
        )
        return None
    
    async def place_tp(
        self,
        symbol: str,
        side: str,
        price: float,
        qty: float,
    ) -> str:
        """Take Profit — лимитный ордер reduce_only"""
        result = await self.place_order(
            symbol=symbol,
            side=side,
            order_type='Limit',
            price=price,
            qty=qty,
            reduce_only=True,
            time_in_force='GTC',
        )
        return result['orderId'] if result else ""
    
    async def place_sl(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        qty: float,
    ) -> str:
        """Stop Loss — conditional order"""
        await self._rate_limiter.acquire()
        
        try:
            # Определяем triggerDirection
            # 1 = triggered when price RISES to trigger_price
            # 2 = triggered when price FALLS to trigger_price
            trigger_direction = 2 if side == 'Sell' else 1
            
            sent_at = time.time()
            
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                partial(
                    self._client.place_order,
                    category='linear',
                    symbol=symbol,
                    side=side,
                    orderType='Market',
                    qty=str(qty),
                    triggerPrice=str(trigger_price),
                    triggerDirection=trigger_direction,
                    reduceOnly=True,
                    orderFilter='StopOrder',
                ),
            )
            
            if result['retCode'] == 0:
                order_id = result['result']['orderId']
                rtt = OrderRTT(
                    order_id=order_id,
                    symbol=symbol,
                    sent_at=sent_at,
                    acknowledged_at=time.time(),
                )
                self._latency_guard.record_order_rtt(rtt)
                
                logger.info(
                    f"SL placed: {symbol} trigger={trigger_price} "
                    f"id={order_id}"
                )
                return order_id
            else:
                logger.error(
                    f"SL placement failed: {result.get('retMsg')}"
                )
                
        except Exception as e:
            logger.error(f"SL placement error: {e}")
        
        return ""
    
    async def cancel_order(
        self, symbol: str, order_id: str
    ) -> bool:
        """Отменить ордер"""
        if not order_id:
            return False
        
        await self._rate_limiter.acquire()
        
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                partial(
                    self._client.cancel_order,
                    category='linear',
                    symbol=symbol,
                    orderId=order_id,
                ),
            )
            
            success = result['retCode'] == 0
            if success:
                logger.info(
                    f"Order cancelled: {symbol} {order_id}"
                )
            else:
                logger.warning(
                    f"Cancel failed: {result.get('retMsg')} "
                    f"({order_id})"
                )
            return success
            
        except Exception as e:
            logger.error(f"Cancel error: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: str) -> bool:
        """Отменить все ордера по символу"""
        await self._rate_limiter.acquire()
        
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                partial(
                    self._client.cancel_all_orders,
                    category='linear',
                    symbol=symbol,
                ),
            )
            
            success = result['retCode'] == 0
            if success:
                logger.info(
                    f"All orders cancelled for {symbol}"
                )
            return success
            
        except Exception as e:
            logger.error(f"Cancel all error: {e}")
            return False
    
    async def modify_order(
        self,
        symbol: str,
        order_id: str,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        qty: Optional[float] = None,
    ) -> bool:
        """Модифицировать ордер"""
        await self._rate_limiter.acquire()
        
        params = {
            'category': 'linear',
            'symbol': symbol,
            'orderId': order_id,
        }
        
        if price is not None:
            params['price'] = str(price)
        if trigger_price is not None:
            params['triggerPrice'] = str(trigger_price)
        if qty is not None:
            params['qty'] = str(qty)
        
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                partial(self._client.amend_order, **params),
            )
            
            success = result['retCode'] == 0
            if not success:
                logger.error(
                    f"Modify failed: {result.get('retMsg')}"
                )
            return success
            
        except Exception as e:
            logger.error(f"Modify error: {e}")
            return False
    
    async def get_positions(
        self, symbol: str = ""
    ) -> list[dict]:
        """Получить открытые позиции"""
        await self._rate_limiter.acquire()
        
        try:
            params = {'category': 'linear', 'settleCoin': 'USDT'}
            if symbol:
                params['symbol'] = symbol
            
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                partial(self._client.get_positions, **params),
            )
            
            if result['retCode'] == 0:
                return result['result']['list']
            
        except Exception as e:
            logger.error(f"Get positions error: {e}")
        
        return []
    
    async def _usdt_to_qty(
        self, symbol: str, usdt_amount: float
    ) -> Optional[float]:
        """Конвертировать USDT в количество контрактов"""
        await self._rate_limiter.acquire()
        
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                partial(
                    self._client.get_tickers,
                    category='linear',
                    symbol=symbol,
                ),
            )
            
            if result['retCode'] == 0:
                last_price = float(
                    result['result']['list'][0]['lastPrice']
                )
                if last_price > 0:
                    qty = usdt_amount / last_price
                    # Округляем до допустимого шага
                    # TODO: получить step size из instrument info
                    return round(qty, 6)
            
        except Exception as e:
            logger.error(f"Price fetch error for {symbol}: {e}")
        
        return None
    
    def _track_slippage(
        self,
        symbol: str,
        expected_price: float,
        order_result: dict,
    ):
        """Отслеживание slippage"""
        actual_price = float(
            order_result.get('avgPrice', expected_price)
        )
        if actual_price > 0 and expected_price > 0:
            slippage = abs(
                actual_price - expected_price
            ) / expected_price * 100
            
            entry = {
                'symbol': symbol,
                'expected': expected_price,
                'actual': actual_price,
                'slippage_pct': round(slippage, 6),
                'timestamp': time.time(),
            }
            self._slippage_log.append(entry)
            
            if len(self._slippage_log) > self._max_slippage_log:
                self._slippage_log = (
                    self._slippage_log[-500:]
                )
            
            if slippage > 0.1:
                logger.warning(
                    f"High slippage: {symbol} "
                    f"{slippage:.4f}%"
                )
    
    def _is_permanent_error(self, ret_code: int) -> bool:
        """Ошибки которые не имеет смысла ретраить"""
        permanent_codes = {
            10001,  # Parameter error
            10003,  # Invalid api key
            10004,  # Invalid sign
            10005,  # Permission denied
            110001, # Order not modified
            110012, # Insufficient balance
            110043, # Exceed max order size
            110044, # Exceed max position size
        }
        return ret_code in permanent_codes
    
    def get_avg_slippage(self) -> float:
        if not self._slippage_log:
            return 0.0
        return sum(
            s['slippage_pct'] for s in self._slippage_log
        ) / len(self._slippage_log)
    
    def get_stats(self) -> dict:
        return {
            'rate_limiter': self._rate_limiter.get_stats(),
            'avg_slippage_pct': round(
                self.get_avg_slippage(), 6
            ),
            'slippage_entries': len(self._slippage_log),
      }
