from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from pybit.unified_trading import WebSocket

from core.latency_guard import LatencyGuard
from core.orderbook import OrderBookManager
from core.volatility_tracker import VolatilityTracker
from models.signals import TradeData

logger = logging.getLogger(__name__)


@dataclass
class _WsConnection:
    """Один WebSocket объект с его символами и состоянием."""
    index: int
    symbols: list[str]
    ws: Optional[WebSocket] = None
    connected: bool = False
    reconnect_count: int = 0
    last_message_time: float = field(default_factory=time.time)
    trade_count: int = 0
    ob_count: int = 0

    def touch(self):
        self.last_message_time = time.time()

    def is_stale(self, timeout_sec: float = 60.0) -> bool:
        return time.time() - self.last_message_time > timeout_sec


class BybitDataFeed:
    """
    WebSocket подключение к Bybit.

    Изменения v2 (multi-connection):
    - Символы делятся на N групп, каждая группа → отдельный WebSocket.
    - Если один WS падает — остальные продолжают работать.
    - _health_monitor детектирует зависший WS и переподключает только его.
    - _ping_loop убран (pybit делает ping сам).
    """

    SYMBOLS_PER_CONNECTION = 4

    def __init__(
        self,
        config: dict,
        latency_guard: LatencyGuard,
        orderbook_manager: OrderBookManager,
        volatility_tracker: VolatilityTracker,
    ):
        self._config = config
        self._latency_guard = latency_guard
        self._orderbook_manager = orderbook_manager
        self._volatility_tracker = volatility_tracker

        self._testnet = config.get('testnet', True)
        self._channel_type = config.get('ws_channel_type', 'linear')
        self._reconnect_delay = config.get('ws_reconnect_delay', 5)
        self._max_reconnect = config.get('ws_max_reconnect_attempts', 50)
        self._stale_timeout = config.get('ws_stale_timeout_sec', 60)
        self._health_interval = config.get('ws_health_interval_sec', 30)

        all_symbols: list[str] = config.get('symbols', [])
        self._connections: list[_WsConnection] = self._make_connections(all_symbols)

        self._trade_callbacks: list[Callable] = []
        self._orderbook_callbacks: list[Callable] = []

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._last_trade_time: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def _symbols(self) -> list[str]:
        result = []
        for conn in self._connections:
            result.extend(conn.symbols)
        return result

    @property
    def _trade_count(self) -> int:
        return sum(c.trade_count for c in self._connections)

    @property
    def _orderbook_update_count(self) -> int:
        return sum(c.ob_count for c in self._connections)

    @property
    def _connected(self) -> bool:
        return any(c.connected for c in self._connections)

    @property
    def _reconnect_count(self) -> int:
        return sum(c.reconnect_count for c in self._connections)

    def on_trade(self, callback: Callable):
        self._trade_callbacks.append(callback)

    def on_orderbook_update(self, callback: Callable):
        self._orderbook_callbacks.append(callback)

    async def start(self):
        self._running = True
        self._loop = asyncio.get_running_loop()

        logger.info(
            f"Starting Bybit DataFeed "
            f"({'testnet' if self._testnet else 'mainnet'}) "
            f"{len(self._connections)} connections x "
            f"up to {self.SYMBOLS_PER_CONNECTION} symbols each"
        )

        await asyncio.gather(
            *[self._connect(conn) for conn in self._connections]
        )

        try:
            await asyncio.gather(
                asyncio.create_task(self._health_monitor()),
            )
        except asyncio.CancelledError:
            logger.info("DataFeed tasks cancelled")

    async def stop(self):
        self._running = False
        for conn in self._connections:
            self._exit_ws(conn)
        logger.info("DataFeed stopped")

    def add_symbol(self, symbol: str):
        if symbol in self._symbols:
            return
        target = min(self._connections, key=lambda c: len(c.symbols))
        target.symbols.append(symbol)
        if target.ws and target.connected:
            target.ws.trade_stream(symbol=symbol, callback=self._make_trade_cb(target))
            target.ws.orderbook_stream(depth=50, symbol=symbol, callback=self._make_ob_cb(target))
            logger.info(f"Dynamically added {symbol} -> connection #{target.index}")

    def remove_symbol(self, symbol: str):
        for conn in self._connections:
            if symbol in conn.symbols:
                conn.symbols.remove(symbol)
                logger.info(f"Removed symbol: {symbol}")
                return

    def get_stats(self) -> dict:
        return {
            'connected': self._connected,
            'connections': [
                {
                    'index': c.index,
                    'symbols': c.symbols,
                    'connected': c.connected,
                    'reconnects': c.reconnect_count,
                    'trades': c.trade_count,
                    'ob_updates': c.ob_count,
                    'stale': c.is_stale(self._stale_timeout),
                }
                for c in self._connections
            ],
            'total_trades': self._trade_count,
            'total_ob_updates': self._orderbook_update_count,
            'total_reconnects': self._reconnect_count,
            'latency': self._latency_guard.get_stats(),
        }

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _make_connections(self, symbols: list[str]) -> list[_WsConnection]:
        n = self.SYMBOLS_PER_CONNECTION
        groups = [symbols[i:i + n] for i in range(0, len(symbols), n)]
        return [_WsConnection(index=i, symbols=grp) for i, grp in enumerate(groups)]

    async def _connect(self, conn: _WsConnection):
        try:
            conn.ws = WebSocket(
                testnet=self._testnet,
                channel_type=self._channel_type,
            )

            trade_cb = self._make_trade_cb(conn)
            ob_cb = self._make_ob_cb(conn)

            for symbol in conn.symbols:
                conn.ws.trade_stream(symbol=symbol, callback=trade_cb)
                conn.ws.orderbook_stream(depth=50, symbol=symbol, callback=ob_cb)

            conn.connected = True
            conn.last_message_time = time.time()
            logger.info(f"Connection #{conn.index} ready: {conn.symbols}")

        except Exception as e:
            conn.connected = False
            logger.error(f"Connection #{conn.index} failed: {e}")

    async def _reconnect(self, conn: _WsConnection):
        if conn.reconnect_count >= self._max_reconnect:
            logger.critical(
                f"Connection #{conn.index}: max reconnects reached, "
                f"giving up on {conn.symbols}"
            )
            return

        conn.connected = False
        self._exit_ws(conn)

        conn.reconnect_count += 1
        delay = min(self._reconnect_delay * conn.reconnect_count, 60)

        logger.warning(
            f"Connection #{conn.index}: reconnecting in {delay}s "
            f"(attempt {conn.reconnect_count}/{self._max_reconnect})"
        )

        await asyncio.sleep(delay)
        await self._connect(conn)

    def _exit_ws(self, conn: _WsConnection):
        if conn.ws:
            try:
                conn.ws.exit()
            except Exception:
                pass
            conn.ws = None
        conn.connected = False

    # ------------------------------------------------------------------
    # Callbacks (called from pybit threading context)
    # ------------------------------------------------------------------

    def _make_trade_cb(self, conn: _WsConnection) -> Callable:
        def _cb(message: dict):
            conn.touch()
            conn.trade_count += 1
            self._dispatch_trade(message)
        return _cb

    def _make_ob_cb(self, conn: _WsConnection) -> Callable:
        def _cb(message: dict):
            conn.touch()
            conn.ob_count += 1
            self._dispatch_orderbook(message)
        return _cb

    def _dispatch_trade(self, message: dict):
        try:
            data_list = message.get('data', [])
            if not data_list:
                return

            for item in data_list:
                symbol = item.get('s', '')
                self._last_trade_time[symbol] = time.time()

                trade = TradeData(
                    symbol=symbol,
                    price=float(item.get('p', 0)),
                    qty=float(item.get("v", 0)),
                    quote_volume=float(item.get("v", 0)) * float(item.get("p", 0)),
                    trade_id=item.get("i", ""),
                    side=item.get('S', 'Buy'),
                    timestamp=float(item.get('T', time.time() * 1000)) / 1000.0,
                )

                self._volatility_tracker.update(
                    symbol=trade.symbol,
                    price=trade.price,
                    timestamp=trade.timestamp,
                    volume=trade.qty,
                )

            for callback in list(self._trade_callbacks):
                try:
                    callback(trade)
                except Exception as e:
                    logger.error(f"Trade callback error: {e}")

        except Exception as e:
            logger.error(f"Trade message processing error: {e}")

    def _dispatch_orderbook(self, message: dict):
        try:
            self._orderbook_manager.process_message(message)

            for callback in list(self._orderbook_callbacks):
                try:
                    callback(message)
                except Exception as e:
                    logger.error(f"OrderBook callback error: {e}")

        except Exception as e:
            logger.error(f"OrderBook message processing error: {e}")

    # ------------------------------------------------------------------
    # Health monitor
    # ------------------------------------------------------------------

    async def _health_monitor(self):
        while self._running:
            await asyncio.sleep(self._health_interval)

            now = time.time()

            for conn in self._connections:
                if conn.connected and conn.is_stale(self._stale_timeout):
                    logger.warning(
                        f"Connection #{conn.index} stale "
                        f"({now - conn.last_message_time:.0f}s no data), "
                        f"reconnecting..."
                    )
                    asyncio.create_task(self._reconnect(conn))

                for symbol in conn.symbols:
                    last = self._last_trade_time.get(symbol, 0)
                    if last > 0 and now - last > 60:
                        logger.warning(
                            f"No trades for {symbol} in {now - last:.0f}s "
                            f"(connection #{conn.index})"
                        )

            stats_lines = " | ".join(
                f"#{c.index}:{c.trade_count}t/{c.ob_count}ob"
                f"{'OK' if c.connected else 'DOWN'}"
                for c in self._connections
            )
            logger.info(
                f"DataFeed health [{stats_lines}] "
                f"latency={self._latency_guard.current_latency_ms:.0f}ms"
            )
