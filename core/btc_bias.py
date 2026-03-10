"""
BTCDirectionBias: если BTC сделал резкое движение → блок противоположных входов для альтов.
"""
import time
import logging
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)


class BTCDirectionBias:
    """
    Отслеживает движение BTC за последние 5 минут.
    BTC > +0.3% → блок SHORT для альтов
    BTC < -0.3% → блок LONG для альтов
    """

    def __init__(self, threshold_pct: float = 0.3, window_sec: float = 300):
        self._threshold = threshold_pct
        self._window = window_sec
        self._btc_prices: deque = deque(maxlen=10000)  # (ts, price)
        self._bias: Optional[str] = None  # 'long', 'short', None

    def on_trade(self, symbol: str, price: float, ts: float = None):
        """Обновляем историю цен BTC"""
        if symbol != 'BTCUSDT':
            return
        ts = ts or time.time()
        self._btc_prices.append((ts, price))
        self._update_bias()

    def _update_bias(self):
        """Пересчитываем bias по последним 5 минутам"""
        if not self._btc_prices:
            self._bias = None
            return

        now = time.time()
        cutoff = now - self._window
        recent = [(ts, p) for ts, p in self._btc_prices if ts >= cutoff]

        if len(recent) < 2:
            self._bias = None
            return

        price_start = recent[0][1]
        price_now = recent[-1][1]

        if price_start <= 0:
            self._bias = None
            return

        change_pct = (price_now - price_start) / price_start * 100

        if change_pct > self._threshold:
            if self._bias != 'long':
                logger.info("BTCBias: BTC +%.2f%% in 5min → block SHORT for alts", change_pct)
            self._bias = 'long'
        elif change_pct < -self._threshold:
            if self._bias != 'short':
                logger.info("BTCBias: BTC %.2f%% in 5min → block LONG for alts", change_pct)
            self._bias = 'short'
        else:
            self._bias = None

    def is_blocked(self, symbol: str, direction: str) -> bool:
        """
        Проверяем блокировку входа.
        symbol: торгуемый символ
        direction: 'long' или 'short'
        """
        if symbol == 'BTCUSDT':
            return False  # BTC не блокируем

        if self._bias == 'long' and direction == 'short':
            return True  # BTC растёт → не шортим альты

        if self._bias == 'short' and direction == 'long':
            return True  # BTC падает → не лонгуем альты

        return False

    def get_bias(self) -> Optional[str]:
        return self._bias
