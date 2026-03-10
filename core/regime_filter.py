"""
RegimeFilter — определяет режим рынка и фильтрует неподходящие сценарии
Режимы:
  TRENDING  — ATR высокий, цена далеко от MA → разрешаем S2/S4, блокируем S3
  RANGING   — ATR низкий, цена около MA → разрешаем S3, снижаем S2/S4
  VOLATILE  — ATR очень высокий → блокируем всё кроме S4 с высоким порогом
"""
import logging
from collections import deque

logger = logging.getLogger(__name__)

class RegimeFilter:
    def __init__(self):
        self._atr:   dict[str, deque] = {}
        self._prices: dict[str, deque] = {}

    def update(self, symbol: str, high: float, low: float, close: float):
        if symbol not in self._atr:
            self._atr[symbol]    = deque(maxlen=14)
            self._prices[symbol] = deque(maxlen=50)
        tr = high - low
        self._atr[symbol].append(tr)
        self._prices[symbol].append(close)

    def get_regime(self, symbol: str) -> str:
        if symbol not in self._atr or len(self._atr[symbol]) < 14:
            return 'UNKNOWN'
        with_lock = list(self._atr[symbol])
        prices = list(self._prices[symbol])
        atr = sum(with_lock) / len(with_lock)
        price = prices[-1]
        atr_pct = atr / price * 100
        # MA отклонение
        ma = sum(prices) / len(prices)
        ma_dev = abs(price - ma) / ma * 100
        if atr_pct > 1.5:
            return 'VOLATILE'
        elif atr_pct > 0.5 or ma_dev > 0.3:
            return 'TRENDING'
        else:
            return 'RANGING'

    def is_allowed(self, symbol: str, scenario: str) -> tuple[bool, float]:
        """Возвращает (allowed, score_multiplier)"""
        regime = self.get_regime(symbol)
        if regime == 'UNKNOWN':
            return True, 1.0
        mr = 'depth' in scenario  # mean reversion сценарии
        if regime == 'VOLATILE':
            if scenario == 'all_three':
                return True, 0.75
            return False, 0.0
        if regime == 'TRENDING':
            if mr:
                return False, 0.0   # блокируем mean reversion в тренде
            return True, 1.1        # бонус для трендовых сценариев
        if regime == 'RANGING':
            if mr:
                return True, 1.1    # бонус для mean reversion в боковике
            return True, 0.8        # снижаем трендовые
        return True, 1.0
