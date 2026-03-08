from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Callable

from models.signals import TradeData, Direction

logger = logging.getLogger(__name__)


class TrendState(Enum):
    UP      = "up"
    DOWN    = "down"
    FLAT    = "flat"


@dataclass
class AveragesSignal:
    symbol: str
    direction: Direction
    timestamp: float
    short_ma: float
    long_ma: float
    delta_pct: float        # (short - long) / long * 100
    trend_state: TrendState
    confidence: float


class AveragesAnalyzer:
    """
    Реализация алгоритма Averages из документации MoonTrader.

    Сравнивает среднюю цену за короткий и длинный период.
    Для интервалов < 1 мин: средняя цена отрезков по 0.5 сек.
    Для интервалов > 1 мин: (min + max свечи) / 2.

    Роли в гибридной стратегии:
    - Сценарий 2: определяет направление тренда для Vector
    - Сценарий 3: определяет перепроданность/перекупленность
    - Сценарий 4: фильтр направления для всех сигналов
    """

    def __init__(self, config: dict):
        self.short_period: float = config.get('short_period', 60.0)
        self.long_period: float = config.get('long_period', 300.0)
        self.min_delta_pct: float = config.get('min_delta_pct', 0.15)
        self.oversold_delta: float = config.get('oversold_delta', -0.8)
        self.overbought_delta: float = config.get('overbought_delta', 0.8)

        # Размер отрезка для sub-minute расчёта (0.5 сек по документации)
        self._bucket_size: float = 0.5

        # Хранилище цен: symbol → deque of (timestamp, price, volume)
        self._prices: dict[str, deque] = {}

        # Кэш последних MA значений
        self._last_short_ma: dict[str, float] = {}
        self._last_long_ma: dict[str, float] = {}
        self._last_delta: dict[str, float] = {}
        self._trend_state: dict[str, TrendState] = {}

        # Статистика
        self._signals_generated: int = 0
        self._signals_by_symbol: dict[str, int] = {}

        # Callbacks
        self._signal_callbacks: list[Callable] = []

        logger.info(
            f"AveragesAnalyzer init: "
            f"short={self.short_period}s "
            f"long={self.long_period}s "
            f"min_delta={self.min_delta_pct}%"
        )

    def on_signal(self, callback: Callable[[AveragesSignal], None]):
        self._signal_callbacks.append(callback)

    def on_trade(self, trade: TradeData) -> Optional[AveragesSignal]:
        symbol = trade.symbol
        now = trade.timestamp

        if symbol not in self._prices:
            self._prices[symbol] = deque()
            self._trend_state[symbol] = TrendState.FLAT
            self._signals_by_symbol[symbol] = 0

        # Сохраняем цену
        self._prices[symbol].append((now, trade.price, trade.quote_volume))

        # Удаляем данные старше long_period
        cutoff = now - self.long_period
        while self._prices[symbol] and self._prices[symbol][0][0] < cutoff:
            self._prices[symbol].popleft()

        # Считаем MA
        short_ma = self._calc_ma(symbol, now, self.short_period)
        long_ma = self._calc_ma(symbol, now, self.long_period)

        if short_ma is None or long_ma is None or long_ma == 0:
            return None

        # Обновляем кэш
        self._last_short_ma[symbol] = short_ma
        self._last_long_ma[symbol] = long_ma

        delta_pct = (short_ma - long_ma) / long_ma * 100
        self._last_delta[symbol] = delta_pct

        # Определяем тренд
        trend = self._classify_trend(delta_pct)
        self._trend_state[symbol] = trend

        # Генерируем сигнал если дельта достаточна
        signal = self._check_signal(symbol, short_ma, long_ma, delta_pct, trend, now)

        if signal:
            self._signals_generated += 1
            self._signals_by_symbol[symbol] = self._signals_by_symbol.get(symbol, 0) + 1
            for cb in self._signal_callbacks:
                try:
                    cb(signal)
                except Exception as e:
                    logger.error(f"Averages callback error: {e}")

        return signal

    def get_trend(self, symbol: str) -> TrendState:
        return self._trend_state.get(symbol, TrendState.FLAT)

    def get_delta(self, symbol: str) -> float:
        return self._last_delta.get(symbol, 0.0)

    def is_oversold(self, symbol: str) -> bool:
        """Перепроданность — short MA сильно ниже long MA"""
        return self._last_delta.get(symbol, 0.0) <= self.oversold_delta

    def is_overbought(self, symbol: str) -> bool:
        """Перекупленность — short MA сильно выше long MA"""
        return self._last_delta.get(symbol, 0.0) >= self.overbought_delta

    def allows_direction(self, symbol: str, direction: Direction) -> bool:
        """
        Проверка: разрешает ли тренд вход в данном направлении.
        Используется Vector и DepthShot как фильтр.
        """
        trend = self._trend_state.get(symbol, TrendState.FLAT)
        delta = abs(self._last_delta.get(symbol, 0.0))

        # Тренд недостаточно выражен
        if delta < self.min_delta_pct:
            return True  # нет фильтрации в флэте

        if direction == Direction.LONG:
            return trend == TrendState.UP
        else:
            return trend == TrendState.DOWN

    def _calc_ma(
        self, symbol: str, now: float, period: float
    ) -> Optional[float]:
        """
        Рассчитать среднюю цену за период.
        Для периодов < 60 сек: bucket-метод (0.5 сек отрезки).
        Для периодов >= 60 сек: простое среднее всех цен в окне.
        """
        cutoff = now - period
        prices = [
            (ts, price, vol)
            for ts, price, vol in self._prices[symbol]
            if ts >= cutoff
        ]

        if not prices:
            return None

        if period < 60:
            return self._calc_bucket_ma(prices, now, period)
        else:
            return self._calc_candle_ma(prices)

    def _calc_bucket_ma(
        self, prices: list, now: float, period: float
    ) -> float:
        """
        Средняя цена через bucket-метод (0.5 сек отрезки).
        Из документации: средняя цена всех отрезков по 0.5 сек /
        количество отрезков в секундах.
        """
        if not prices:
            return 0.0

        # Группируем по bucket'ам
        buckets: dict[int, list[float]] = {}
        for ts, price, vol in prices:
            bucket_id = int(ts / self._bucket_size)
            if bucket_id not in buckets:
                buckets[bucket_id] = []
            buckets[bucket_id].append(price)

        if not buckets:
            return 0.0

        # Средняя цена каждого bucket'а
        bucket_avgs = [
            sum(p) / len(p) for p in buckets.values()
        ]

        return sum(bucket_avgs) / len(bucket_avgs)

    def _calc_candle_ma(self, prices: list) -> float:
        """
        Средняя цена через (min + max) / 2.
        Из документации: для интервалов > 1 минуты.
        """
        if not prices:
            return 0.0

        all_prices = [p for _, p, _ in prices]
        return (min(all_prices) + max(all_prices)) / 2

    def _classify_trend(self, delta_pct: float) -> TrendState:
        if delta_pct > self.min_delta_pct:
            return TrendState.UP
        elif delta_pct < -self.min_delta_pct:
            return TrendState.DOWN
        else:
            return TrendState.FLAT

    def _check_signal(
        self, symbol, short_ma, long_ma, delta_pct, trend, now
    ) -> Optional[AveragesSignal]:
        """Генерировать сигнал если дельта пересекла порог"""

        abs_delta = abs(delta_pct)

        # Недостаточная дельта — флэт
        if abs_delta < self.min_delta_pct:
            return None

        # Определяем направление
        if trend == TrendState.UP:
            direction = Direction.LONG
        elif trend == TrendState.DOWN:
            direction = Direction.SHORT
        else:
            return None

        # Confidence: насколько дельта превышает минимум
        confidence = min(abs_delta / (self.min_delta_pct * 3), 1.0)

        return AveragesSignal(
            symbol=symbol,
            direction=direction,
            timestamp=now,
            short_ma=short_ma,
            long_ma=long_ma,
            delta_pct=delta_pct,
            trend_state=trend,
            confidence=round(confidence, 3),
        )

    def get_stats(self, symbol: str = None) -> dict:
        if symbol:
            return {
                'symbol': symbol,
                'trend_state': self._trend_state.get(symbol, TrendState.FLAT).value,
                'short_ma': round(self._last_short_ma.get(symbol, 0), 4),
                'long_ma': round(self._last_long_ma.get(symbol, 0), 4),
                'delta_pct': round(self._last_delta.get(symbol, 0), 4),
                'is_oversold': self.is_oversold(symbol),
                'is_overbought': self.is_overbought(symbol),
                'signals_generated': self._signals_by_symbol.get(symbol, 0),
                'data_points': len(self._prices.get(symbol, [])),
            }
        return {
            'total_signals': self._signals_generated,
            'symbols': {s: self.get_stats(s) for s in list(self._prices.keys())},
            'config': {
                'short_period': self.short_period,
                'long_period': self.long_period,
                'min_delta_pct': self.min_delta_pct,
                'oversold_delta': self.oversold_delta,
                'overbought_delta': self.overbought_delta,
            },
        }
