from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Callable

from models.signals import Direction
from core.orderbook import OrderBookManager

logger = logging.getLogger(__name__)


class TakeProfitType(Enum):
    CLASSIC    = "classic"     # % от цены входа
    HISTORICAL = "historical"  # % от движения за последние 2 сек
    DEPTH      = "depth"       # следующий крупный уровень в стакане


@dataclass
class DepthLevel:
    price: float
    volume_usdt: float
    distance_pct: float   # расстояние от текущей цены


@dataclass
class DepthShotSignal:
    symbol: str
    direction: Direction
    timestamp: float
    entry_price: float        # цена уровня объёма
    current_price: float
    distance_pct: float       # расстояние от текущей цены до уровня
    volume_at_level: float    # объём на уровне в USDT
    tp_price: float
    confidence: float
    tp_type: TakeProfitType


class WallTracker:
    """v3: Отслеживает время жизни объёмных стен. Игнорирует стены < min_age_s."""

    def __init__(self, min_age_s: float = 7.0, max_drop_pct: float = 0.3):
        self.min_age_s = min_age_s
        self.max_drop_pct = max_drop_pct  # стена надёжна если не уменьшилась >30%
        self._walls: dict = {}  # (symbol, side, price_level) -> {first_seen, max_size, current_size}

    def update(self, symbol: str, side: str, price_level: float, size_usdt: float) -> None:
        key = (symbol, side, round(price_level, 2))
        now = time.time()
        if key in self._walls:
            self._walls[key]['current_size'] = size_usdt
            self._walls[key]['max_size'] = max(self._walls[key]['max_size'], size_usdt)
        else:
            self._walls[key] = {
                'first_seen': now,
                'max_size': size_usdt,
                'current_size': size_usdt,
            }

    def is_reliable(self, symbol: str, side: str, price_level: float) -> bool:
        key = (symbol, side, round(price_level, 2))
        wall = self._walls.get(key)
        if not wall:
            return False
        age = time.time() - wall['first_seen']
        if age < self.min_age_s:
            return False
        if wall['max_size'] > 0:
            retention = wall['current_size'] / wall['max_size']
            if retention < (1.0 - self.max_drop_pct):
                return False
        return True

    def cleanup(self, max_age_s: float = 300.0) -> None:
        """Удаляем старые записи."""
        now = time.time()
        self._walls = {k: v for k, v in self._walls.items()
                       if now - v['first_seen'] < max_age_s}


class DepthShotAnalyzer:
    """
    Реализация алгоритма Depth Shot из документации MoonTrader.

    Отличие от обычных шотов: ордер ставится не на фиксированном
    расстоянии от цены, а на уровне крупного объёма в стакане.

    Роли в гибридной стратегии:
    - Сценарий 1: Vector даёт направление → Depth ищет уровень
    - Сценарий 3: Averages даёт перепроданность → Depth ищет стену
    - Сценарий 4: все три вместе → Depth даёт точную цену входа
    """

    def __init__(
        self,
        config: dict,
        orderbook_manager: OrderBookManager,
    ):
        self._ob = orderbook_manager

        # Основные параметры из документации
        self.min_volume_usdt: float = config.get('min_volume_usdt', 500_000)
        # v3: адаптивный порог стен по символу
        self._symbol_thresholds: dict = config.get('symbol_thresholds', {
            'BTCUSDT':  200_000,
            'ETHUSDT':  100_000,
            'SOLUSDT':   50_000,
            'BNBUSDT':   50_000,
            'XRPUSDT':   30_000,
            'DOGEUSDT':  30_000,
            'ADAUSDT':   30_000,
            'AVAXUSDT':  30_000,
        })
        self.min_distance_pct: float = config.get('min_distance_pct', 0.3)
        self.max_distance_pct: float = config.get('max_distance_pct', 2.5)
        self.buffer_pct: float = config.get('buffer_pct', 0.2)
        self.stop_if_outside: bool = config.get('stop_if_outside', True)

        # Take Profit параметры
        self.tp_type: TakeProfitType = TakeProfitType(
            config.get('tp_type', 'classic')
        )
        self.tp_pct: float = config.get('tp_pct', 0.3)

        # Задержки на перемещение ордера
        self.follow_delay: float = config.get('follow_delay', 0.5)
        self.move_away_delay: float = config.get('move_away_delay', 1.0)

        # Статистика
        self._signals_generated: int = 0
        self._signals_by_symbol: dict[str, int] = {}
        self._last_scan_time: dict[str, float] = {}

        # v3: WallTracker
        wall_cfg = config.get('wall_tracker', {})
        self._wall_tracker = WallTracker(
            min_age_s=wall_cfg.get('min_age_s', 7.0),
            max_drop_pct=wall_cfg.get('max_drop_pct', 0.3),
        )

        # Callbacks
        self._signal_callbacks: list[Callable] = []

        logger.info(
            f"DepthShotAnalyzer init: "
            f"min_vol={self.min_volume_usdt:,.0f} USDT "
            f"distance={self.min_distance_pct}-{self.max_distance_pct}% "
            f"tp_type={self.tp_type.value}"
        )

    def _get_min_volume(self, symbol: str) -> float:
        """Адаптивный порог стены по символу."""
        return self._symbol_thresholds.get(symbol.upper(), self.min_volume_usdt)

    def on_signal(self, callback: Callable[[DepthShotSignal], None]):
        self._signal_callbacks.append(callback)

    def scan(
        self,
        symbol: str,
        direction: Direction,
        current_price: float = None,
    ) -> Optional[DepthShotSignal]:
        """
        Основной метод — сканирует стакан в поисках уровня объёма.
        Вызывается когда Vector или Averages дают сигнал.

        direction: в какую сторону ищем уровень
          LONG  → ищем крупный BID (поддержка снизу)
          SHORT → ищем крупный ASK (сопротивление сверху)
        """
        book = self._ob.get_book(symbol)

        if not book.is_initialized:
            logger.debug(f"OrderBook not initialized for {symbol}")
            return None

        if current_price is None:
            mid = book.mid_price
            if mid is None:
                return None
            current_price = mid

        # Определяем сторону стакана
        side = 'bid' if direction == Direction.LONG else 'ask'

        # Ищем уровень объёма
        result = book.find_volume_level(
            side=side,
            target_volume_usdt=self._get_min_volume(symbol),
            min_distance_pct=self.min_distance_pct,
            max_distance_pct=self.max_distance_pct,
        )

        if result is None:
            logger.debug(
                f"No volume level found for {symbol} {direction.value} "
                f"min_vol={self._get_min_volume(symbol):,.0f}"
            )
            return None

        level_price, level_volume = result

        # v3: WallTracker — обновляем и проверяем надёжность стены
        self._wall_tracker.update(symbol, side, level_price, level_volume)
        if not self._wall_tracker.is_reliable(symbol, side, level_price):
            logger.debug(
                "[%s] Wall at %.4f not reliable yet (age < %.1fs or shrunk)",
                symbol, level_price, self._wall_tracker.min_age_s
            )
            return None
        distance_pct = abs(level_price - current_price) / current_price * 100

        # Рассчитываем TP
        tp_price = self._calc_tp(
            direction, level_price, current_price, book, symbol
        )

        confidence = self._calculate_confidence(
            level_volume, distance_pct
        )

        signal = DepthShotSignal(
            symbol=symbol,
            direction=direction,
            timestamp=time.time(),
            entry_price=level_price,
            current_price=current_price,
            distance_pct=distance_pct,
            volume_at_level=level_volume,
            tp_price=tp_price,
            confidence=confidence,
            tp_type=self.tp_type,
        )

        self._signals_generated += 1
        self._signals_by_symbol[symbol] = (
            self._signals_by_symbol.get(symbol, 0) + 1
        )
        self._last_scan_time[symbol] = time.time()

        for cb in self._signal_callbacks:
            try:
                cb(signal)
            except Exception as e:
                logger.error(f"DepthShot callback error: {e}")

        logger.debug(
            f"DepthShot signal: {symbol} {direction.value} "
            f"entry={level_price} dist={distance_pct:.3f}% "
            f"vol={level_volume:,.0f} tp={tp_price}"
        )

        return signal

    def is_level_still_valid(
        self, symbol: str, entry_price: float, direction: Direction
    ) -> bool:
        """
        Проверить что уровень объёма всё ещё существует.
        Используется для отмены ордера если стена исчезла (spoofing).
        """
        if not self.stop_if_outside:
            return True

        book = self._ob.get_book(symbol)
        if not book.is_initialized:
            return False

        side = 'bid' if direction == Direction.LONG else 'ask'

        # Ищем объём вблизи нашего уровня (±buffer)
        if side == 'bid':
            levels = book.get_bids(50)
        else:
            levels = book.get_asks(50)

        for level in levels:
            price_diff_pct = abs(level.price - entry_price) / entry_price * 100
            if price_diff_pct <= self.buffer_pct:
                if level.quote_volume >= self._get_min_volume(symbol) * 0.5:
                    return True

        return False

    def get_orderbook_imbalance(self, symbol: str) -> float:
        """
        Дисбаланс стакана: bid_vol / (bid_vol + ask_vol) в топ-10.
        > 0.55 → давление покупателей (LONG)
        < 0.45 → давление продавцов (SHORT)
        """
        book = self._ob.get_book(symbol)
        if not book.is_initialized:
            return 0.5

        bids = book.get_bids(10)
        asks = book.get_asks(10)

        bid_vol = sum(b.qty * b.price for b in bids)
        ask_vol = sum(a.qty * a.price for a in asks)
        total = bid_vol + ask_vol

        if total == 0:
            return 0.5

        return bid_vol / total

    def _calc_tp(
        self,
        direction: Direction,
        entry_price: float,
        current_price: float,
        book,
        symbol: str,
    ) -> float:
        """Рассчитать Take Profit в зависимости от типа"""

        if self.tp_type == TakeProfitType.CLASSIC:
            if direction == Direction.LONG:
                return entry_price * (1 + self.tp_pct / 100)
            else:
                return entry_price * (1 - self.tp_pct / 100)

        elif self.tp_type == TakeProfitType.DEPTH:
            # TP на следующем крупном уровне объёма
            # с противоположной стороны от входа
            opposite_side = 'ask' if direction == Direction.LONG else 'bid'
            result = book.find_volume_level(
                side=opposite_side,
                target_volume_usdt=self._get_min_volume(symbol) * 0.5,
                min_distance_pct=0.1,
                max_distance_pct=self.max_distance_pct * 2,
            )
            if result:
                return result[0]
            # Fallback на classic
            if direction == Direction.LONG:
                return entry_price * (1 + self.tp_pct / 100)
            else:
                return entry_price * (1 - self.tp_pct / 100)

        else:  # HISTORICAL
            distance = abs(current_price - entry_price)
            if direction == Direction.LONG:
                return entry_price + distance * (self.tp_pct / 100)
            else:
                return entry_price - distance * (self.tp_pct / 100)

    def _calculate_confidence(
        self, volume_usdt: float, distance_pct: float
    ) -> float:
        """
        Уверенность сигнала:
        - Чем больше объём → выше
        - Чем ближе уровень (но не слишком близко) → выше
        """
        # Фактор объёма
        vol_factor = min(volume_usdt / (self.min_volume_usdt * 3), 1.0)

        # Фактор расстояния: оптимум в середине диапазона
        mid_distance = (self.min_distance_pct + self.max_distance_pct) / 2
        dist_factor = 1.0 - abs(distance_pct - mid_distance) / mid_distance
        dist_factor = max(0.0, min(dist_factor, 1.0))

        return round(vol_factor * 0.6 + dist_factor * 0.4, 3)

    def get_stats(self, symbol: str = None) -> dict:
        if symbol:
            book = self._ob.get_book(symbol)
            return {
                'symbol': symbol,
                'signals_generated': self._signals_by_symbol.get(symbol, 0),
                'last_scan': self._last_scan_time.get(symbol, 0),
                'ob_initialized': book.is_initialized,
                'ob_imbalance': round(
                    self.get_orderbook_imbalance(symbol), 3
                ),
            }
        return {
            'total_signals': self._signals_generated,
            'symbols': {
                s: self.get_stats(s)
                for s in list(self._signals_by_symbol.keys())
            },
            'config': {
                'min_volume_usdt': self.min_volume_usdt,
                'min_distance_pct': self.min_distance_pct,
                'max_distance_pct': self.max_distance_pct,
                'buffer_pct': self.buffer_pct,
                'tp_type': self.tp_type.value,
                'tp_pct': self.tp_pct,
            },
        }
