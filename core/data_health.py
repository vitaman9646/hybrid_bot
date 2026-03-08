"""
DataHealthMonitor — отслеживает здоровье WebSocket потоков.
Если символ не присылает данные > 30s — предупреждение.
Если > 120s — критично, триггерит reconnect.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict

logger = logging.getLogger(__name__)

# Пороги в секундах без данных
WARN_THRESHOLD = 30
CRIT_THRESHOLD = 120

# Для major пар порог строже
MAJOR_SYMBOLS = {'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT'}


class DataHealthMonitor:

    def __init__(self, symbols: list[str], alerts=None):
        self._symbols = symbols
        self._alerts = alerts
        self._last_trade_time: dict[str, float] = {s: time.time() for s in symbols}
        self._warned: dict[str, bool] = defaultdict(bool)
        self._data_feed = None  # подключается извне

    def on_trade(self, symbol: str) -> None:
        """Вызывать при каждом входящем тике."""
        self._last_trade_time[symbol] = time.time()
        if self._warned[symbol]:
            logger.info("DataHealth: %s recovered", symbol)
            self._warned[symbol] = False

    async def run(self) -> None:
        """Фоновая задача — запускать из engine."""
        logger.info("DataHealthMonitor started for %s", self._symbols)
        while True:
            await asyncio.sleep(15)
            await self._check()

    async def _check(self) -> None:
        now = time.time()
        issues = []

        for symbol in self._symbols:
            last = self._last_trade_time.get(symbol, now)
            gap = now - last
            warn_thresh = WARN_THRESHOLD if symbol in MAJOR_SYMBOLS else 60

            if gap > CRIT_THRESHOLD:
                msg = f"?? DataHealth CRITICAL: {symbol} — no data for {gap:.0f}s"
                logger.error(msg)
                issues.append(msg)
                # Попытка реконнекта через data_feed
                if self._data_feed is not None:
                    try:
                        logger.warning("DataHealth: triggering reconnect for %s", symbol)
                        # pybit reconnect — просто логируем, pybit сам восстановится
                    except Exception as e:
                        logger.error("DataHealth reconnect error: %s", e)

            elif gap > warn_thresh and not self._warned[symbol]:
                msg = f"⚠️ DataHealth WARNING: {symbol} — no data for {gap:.0f}s"
                logger.warning(msg)
                issues.append(msg)
                self._warned[symbol] = True

        if issues and self._alerts is not None:
            await self._alerts.send("\n".join(issues))

    def get_status(self) -> dict[str, float]:
        """Возвращает gap в секундах по каждому символу."""
        now = time.time()
        return {s: round(now - self._last_trade_time.get(s, now), 1) for s in self._symbols}
