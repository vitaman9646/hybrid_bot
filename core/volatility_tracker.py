# core/volatility_tracker.py
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class PricePoint:
    timestamp: float
    price: float
    volume: float = 0.0


class VolatilityTracker:
    """
    Отслеживает волатильность для:
    - Adaptive trailing spread
    - Dead market / chaos detection
    - Vector frame validation
    - Filter pipeline
    """
    
    def __init__(self, window_seconds: int = 60):
        self.window = window_seconds
        self._prices: dict[str, deque[PricePoint]] = {}
        
        # Кеш рассчитанной волатильности
        self._cache: dict[str, tuple[float, float]] = {}
        self._cache_ttl = 0.5  # пересчитывать каждые 500мс
    
    def update(
        self,
        symbol: str,
        price: float,
        timestamp: float,
        volume: float = 0.0,
    ):
        """Добавить ценовую точку"""
        if symbol not in self._prices:
            self._prices[symbol] = deque()
        
        self._prices[symbol].append(
            PricePoint(timestamp, price, volume)
        )
        
        # Eviction старых точек
        cutoff = timestamp - self.window
        while (
            self._prices[symbol]
            and self._prices[symbol][0].timestamp < cutoff
        ):
            self._prices[symbol].popleft()
        
        # Инвалидируем кеш
        self._cache.pop(symbol, None)
    
    def get_volatility(self, symbol: str) -> float:
        """
        Волатильность в процентах за окно
        (High - Low) / Low * 100
        """
        # Проверяем кеш
        if symbol in self._cache:
            cached_time, cached_vol = self._cache[symbol]
            if time.time() - cached_time < self._cache_ttl:
                return cached_vol
        
        if (
            symbol not in self._prices
            or len(self._prices[symbol]) < 2
        ):
            return 0.0
        
        prices = [p.price for p in self._prices[symbol]]
        high = max(prices)
        low = min(prices)
        
        if low == 0:
            return 0.0
        
        vol = (high - low) / low * 100
        
        # Обновляем кеш
        self._cache[symbol] = (time.time(), vol)
        
        return vol
    
    def get_vwap(self, symbol: str) -> float:
        """Volume-Weighted Average Price"""
        if (
            symbol not in self._prices
            or not self._prices[symbol]
        ):
            return 0.0
        
        total_volume = sum(
            p.volume for p in self._prices[symbol]
        )
        if total_volume == 0:
            # Fallback: простая средняя
            prices = [p.price for p in self._prices[symbol]]
            return sum(prices) / len(prices)
        
        vwap = sum(
            p.price * p.volume for p in self._prices[symbol]
        ) / total_volume
        
        return vwap
    
    def get_trade_count(self, symbol: str) -> int:
        """Количество трейдов в окне"""
        if symbol not in self._prices:
            return 0
        return len(self._prices[symbol])
    
    def get_volume_sum(self, symbol: str) -> float:
        """Суммарный объём в окне"""
        if symbol not in self._prices:
            return 0.0
        return sum(p.volume for p in self._prices[symbol])
    
    def is_dead_market(
        self, symbol: str, threshold: float = 0.05
    ) -> bool:
        """Рынок слишком спокойный для торговли"""
        return self.get_volatility(symbol) < threshold
    
    def is_chaos(
        self, symbol: str, threshold: float = 5.0
    ) -> bool:
        """Рынок слишком хаотичный"""
        return self.get_volatility(symbol) > threshold
    
    def get_adaptive_trailing_spread(
        self,
        symbol: str,
        base_spread: float = 0.3,
    ) -> float:
        """
        Адаптивный trailing spread.
        При высокой волатильности — шире, при низкой — уже.
        """
        vol = self.get_volatility(symbol)
        
        if vol == 0:
            return base_spread
        
        # Trailing = max(base, volatility * 0.5)
        # Но не более чем base * 3
        adaptive = max(base_spread, vol * 0.5)
        adaptive = min(adaptive, base_spread * 3)
        
        return round(adaptive, 4)
    
    def get_stats(self, symbol: str) -> dict:
        return {
            'symbol': symbol,
            'volatility_pct': round(
                self.get_volatility(symbol), 4
            ),
            'vwap': round(self.get_vwap(symbol), 4),
            'trade_count': self.get_trade_count(symbol),
            'volume_sum': round(self.get_volume_sum(symbol), 2),
            'is_dead': self.is_dead_market(symbol),
            'is_chaos': self.is_chaos(symbol),
        }
    
    def get_all_stats(self) -> dict[str, dict]:
        return {
            symbol: self.get_stats(symbol)
            for symbol in self._prices
        }
