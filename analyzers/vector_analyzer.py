from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable

from models.signals import TradeData, Signal, Direction, SignalSource

logger = logging.getLogger(__name__)


class MarketState(Enum):
    DEAD     = "dead"      # vol < min_volatility
    NORMAL   = "normal"    # Сценарий 4: все три алгоритма
    VOLATILE = "volatile"  # Сценарий 1/2: импульсные стратегии
    CHAOS    = "chaos"     # vol > max_volatility, не торгуем


@dataclass
class Frame:
    """Один микро-фрейм Vector алгоритма"""
    start_time: float
    end_time: float
    min_price: float = float('inf')
    max_price: float = float('-inf')
    trade_count: int = 0
    quote_volume: float = 0.0

    @property
    def spread(self) -> float:
        if self.min_price == float('inf'):
            return 0.0
        return self.max_price - self.min_price

    @property
    def spread_pct(self) -> float:
        if self.min_price <= 0 or self.min_price == float('inf'):
            return 0.0
        return (self.spread / self.min_price) * 100

    @property
    def is_complete(self) -> bool:
        return time.time() >= self.end_time

    def add_trade(self, price: float, qty: float, quote_vol: float):
        self.min_price = min(self.min_price, price)
        self.max_price = max(self.max_price, price)
        self.trade_count += 1
        self.quote_volume += quote_vol


@dataclass
class VectorSignal:
    """Сигнал от Vector анализатора"""
    symbol: str
    direction: Direction
    timestamp: float
    spread_pct: float           # размер найденного спреда
    upper_border: float         # верхняя граница последнего фрейма
    lower_border: float         # нижняя граница последнего фрейма
    frame_count: int            # сколько фреймов в анализе
    avg_volume_per_frame: float
    confidence: float           # 0.0 - 1.0
    market_state: MarketState
    is_shot: bool = False       # режим Detect Shot


class VectorAnalyzer:
    """
    Реализация алгоритма Vector из документации MoonTrader.

    Принцип:
    1. Делит Time Frame на фреймы размером Frame Size
    2. В каждом фрейме отслеживает min/max цену и объём
    3. Если спред в каждом фрейме > Min Spread Size
       И количество трейдов > Min Trades Per Frame
       И объём > Min Quote Asset Volume
       → генерирует сигнал

    Дополнительно используется как фильтр состояния рынка:
    DEAD / NORMAL / VOLATILE / CHAOS
    """

    def __init__(self, config: dict):
        # Основные параметры из документации
        self.frame_size: float = config.get('frame_size', 0.2)
        self.time_frame: float = config.get('time_frame', 1.0)
        self.min_spread_size: float = config.get('min_spread_size', 0.5)
        self.min_trades_per_frame: int = config.get('min_trades_per_frame', 2)
        self.min_quote_volume: float = config.get('min_quote_volume', 10_000)

        # Фильтры границ (Upper/Lower Border Range)
        self.use_border_range: bool = config.get('use_border_range', False)
        self.upper_border_min: float = config.get('upper_border_min', 0.0)
        self.upper_border_max: float = config.get('upper_border_max', 0.5)
        self.lower_border_min: float = config.get('lower_border_min', 0.0)
        self.lower_border_max: float = config.get('lower_border_max', 0.5)

        # Detect Shot режим
        self.use_detect_shot: bool = config.get('use_detect_shot', False)
        self.shot_retracement: float = config.get('shot_retracement', 80.0)
        self.shot_direction: str = config.get('shot_direction', 'both')  # up/down/both

        # Пороги состояния рынка (для роли фильтра)
        self.dead_threshold: float = config.get('dead_threshold', 0.1)
        self.chaos_threshold: float = config.get('chaos_threshold', 5.0)

        # Внутреннее состояние — фреймы по символам
        self._frames: dict[str, deque[Frame]] = {}
        self._current_frame: dict[str, Optional[Frame]] = {}
        self._market_state: dict[str, MarketState] = {}

        # Статистика
        self._signals_generated: int = 0
        self._signals_by_symbol: dict[str, int] = {}

        # Callbacks
        self._signal_callbacks: list[Callable] = []

        # Сколько фреймов нужно для анализа
        self._frames_needed = max(
            1, round(self.time_frame / self.frame_size)
        )

        logger.info(
            f"VectorAnalyzer init: "
            f"frame_size={self.frame_size}s "
            f"time_frame={self.time_frame}s "
            f"frames_needed={self._frames_needed} "
            f"min_spread={self.min_spread_size}%"
        )

    def on_signal(self, callback: Callable[[VectorSignal], None]):
        """Регистрация callback для сигналов"""
        self._signal_callbacks.append(callback)

    def on_trade(self, trade: TradeData) -> Optional[VectorSignal]:
        """
        Основной метод — вызывается на каждый трейд.
        Возвращает VectorSignal если условия выполнены.
        """
        symbol = trade.symbol
        now = trade.timestamp

        # Инициализируем структуры для нового символа
        if symbol not in self._frames:
            self._frames[symbol] = deque(maxlen=self._frames_needed + 5)
            self._current_frame[symbol] = None
            self._market_state[symbol] = MarketState.NORMAL
            self._signals_by_symbol[symbol] = 0

        # Управляем текущим фреймом
        self._update_frame(symbol, trade)

        # Проверяем условия для сигнала
        signal = self._check_signal(symbol, now)

        if signal:
            self._signals_generated += 1
            self._signals_by_symbol[symbol] = (
                self._signals_by_symbol.get(symbol, 0) + 1
            )
            for cb in self._signal_callbacks:
                try:
                    cb(signal)
                except Exception as e:
                    logger.error(f"Vector signal callback error: {e}")

        return signal

    def get_market_state(self, symbol: str) -> MarketState:
        """Текущее состояние рынка для символа"""
        return self._market_state.get(symbol, MarketState.NORMAL)

    def _update_frame(self, symbol: str, trade: TradeData):
        """Обновить текущий фрейм или создать новый"""
        now = trade.timestamp
        current = self._current_frame[symbol]

        # Нужен новый фрейм?
        if current is None or now >= current.end_time:
            # Сохраняем завершённый фрейм
            if current is not None:
                self._frames[symbol].append(current)

            # Создаём новый
            frame_start = now
            self._current_frame[symbol] = Frame(
                start_time=frame_start,
                end_time=frame_start + self.frame_size,
            )
            current = self._current_frame[symbol]

        # Добавляем трейд в текущий фрейм
        current.add_trade(
            price=trade.price,
            qty=trade.qty,
            quote_vol=trade.quote_volume,
        )

    def _check_signal(
        self, symbol: str, now: float
    ) -> Optional[VectorSignal]:
        """
        Проверить все условия для генерации сигнала.
        """
        frames = list(self._frames[symbol])

        if len(frames) < self._frames_needed:
            return None

        # Берём последние N фреймов
        recent = frames[-self._frames_needed:]

        # Detect Shot режим
        if self.use_detect_shot:
            return self._check_shot_signal(symbol, recent, now)

        return self._check_standard_signal(symbol, recent, now)

    def _check_standard_signal(
        self,
        symbol: str,
        frames: list[Frame],
        now: float,
    ) -> Optional[VectorSignal]:
        """Стандартная проверка условий Vector"""

        last_frame = frames[-1]

        # 1. Спред в каждом фрейме > Min Spread Size
        for frame in frames:
            if frame.spread_pct < self.min_spread_size:
                self._update_market_state(symbol, frames)
                return None

        # 2. Трейды в каждом фрейме > Min Trades Per Frame
        for frame in frames:
            if frame.trade_count < self.min_trades_per_frame:
                return None

        # 3. Объём в каждом фрейме > Min Quote Asset Volume
        for frame in frames:
            if frame.quote_volume < self.min_quote_volume:
                return None

        # 4. Проверка Border Range (если включено)
        if self.use_border_range and len(frames) >= 2:
            if not self._check_border_range(frames, last_frame):
                return None

        # Определяем направление по движению границ
        direction = self._determine_direction(frames)
        if direction is None:
            return None

        # Обновляем состояние рынка
        self._update_market_state(symbol, frames)
        market_state = self._market_state[symbol]

        # В хаосе не торгуем
        if market_state == MarketState.CHAOS:
            return None

        # Считаем confidence
        confidence = self._calculate_confidence(frames, last_frame)

        avg_volume = sum(f.quote_volume for f in frames) / len(frames)

        signal = VectorSignal(
            symbol=symbol,
            direction=direction,
            timestamp=now,
            spread_pct=last_frame.spread_pct,
            upper_border=last_frame.max_price,
            lower_border=last_frame.min_price,
            frame_count=len(frames),
            avg_volume_per_frame=avg_volume,
            confidence=confidence,
            market_state=market_state,
            is_shot=False,
        )

        logger.debug(
            f"Vector signal: {symbol} {direction.value} "
            f"spread={last_frame.spread_pct:.3f}% "
            f"confidence={confidence:.2f} "
            f"state={market_state.value}"
        )

        return signal

    def _check_shot_signal(
        self,
        symbol: str,
        frames: list[Frame],
        now: float,
    ) -> Optional[VectorSignal]:
        """
        Detect Shot режим: ищем резкий прострел с откатом.
        Анализирует только последний фрейм.
        """
        if not frames:
            return None

        last = frames[-1]

        if last.spread_pct < self.min_spread_size:
            return None
        if last.trade_count < self.min_trades_per_frame:
            return None
        if last.quote_volume < self.min_quote_volume:
            return None

        # Определяем направление прострела
        if self.shot_direction == 'up':
            direction = Direction.LONG
        elif self.shot_direction == 'down':
            direction = Direction.SHORT
        else:
            # Определяем по движению цены
            direction = self._determine_direction([last])
            if direction is None:
                return None

        # Проверяем откат (retracement)
        # Если цена выросла с 100 до 101, откат 80% = возврат до 100.2
        movement = last.spread
        if movement <= 0:
            return None

        # В реальности для detect shot нужны данные последних цен
        # Здесь упрощённая версия — проверяем только наличие спреда
        confidence = min(
            last.spread_pct / (self.min_spread_size * 2), 1.0
        )

        return VectorSignal(
            symbol=symbol,
            direction=direction,
            timestamp=now,
            spread_pct=last.spread_pct,
            upper_border=last.max_price,
            lower_border=last.min_price,
            frame_count=1,
            avg_volume_per_frame=last.quote_volume,
            confidence=confidence,
            market_state=self._market_state.get(
                symbol, MarketState.NORMAL
            ),
            is_shot=True,
        )

    def _check_border_range(
        self, frames: list[Frame], last_frame: Frame
    ) -> bool:
        """
        Проверка Upper/Lower Border Range.
        Изменение границы между фреймами должно попасть в диапазон.
        """
        last_spread = last_frame.spread
        if last_spread <= 0:
            return False

        for i in range(1, len(frames)):
            prev = frames[i - 1]
            curr = frames[i]

            # Изменение верхней границы
            upper_change_pct = (
                (curr.max_price - prev.max_price) / last_spread * 100
            )
            if not (self.upper_border_min
                    <= upper_change_pct
                    <= self.upper_border_max):
                return False

            # Изменение нижней границы
            lower_change_pct = (
                (curr.min_price - prev.min_price) / last_spread * 100
            )
            if not (self.lower_border_min
                    <= lower_change_pct
                    <= self.lower_border_max):
                return False

        return True

    def _determine_direction(
        self, frames: list[Frame]
    ) -> Optional[Direction]:
        """
        Определить направление движения по изменению границ.
        Если верхняя и нижняя границы растут → LONG
        Если обе падают → SHORT
        """
        if len(frames) < 2:
            # Один фрейм — смотрим на положение цены внутри спреда
            f = frames[0]
            if f.spread <= 0:
                return None
            # Если max двигается больше — рост
            return Direction.LONG

        first = frames[0]
        last = frames[-1]

        upper_delta = last.max_price - first.max_price
        lower_delta = last.min_price - first.min_price

        # Оба растут → LONG
        if upper_delta > 0 and lower_delta > 0:
            return Direction.LONG

        # Оба падают → SHORT
        if upper_delta < 0 and lower_delta < 0:
            return Direction.SHORT

        # Разнонаправленное движение — нет чёткого направления
        # Смотрим где больше движение
        if abs(upper_delta) > abs(lower_delta) * 1.5:
            return Direction.LONG
        if abs(lower_delta) > abs(upper_delta) * 1.5:
            return Direction.SHORT

        return None

    def _update_market_state(
        self, symbol: str, frames: list[Frame]
    ):
        """Обновить состояние рынка на основе волатильности"""
        if not frames:
            return

        avg_spread = sum(f.spread_pct for f in frames) / len(frames)

        if avg_spread < self.dead_threshold:
            state = MarketState.DEAD
        elif avg_spread > self.chaos_threshold:
            state = MarketState.CHAOS
        elif avg_spread >= self.min_spread_size:
            # Волатильный рынок — зависит от интенсивности
            if avg_spread > self.min_spread_size * 3:
                state = MarketState.VOLATILE
            else:
                state = MarketState.NORMAL
        else:
            state = MarketState.NORMAL

        old_state = self._market_state.get(symbol)
        if old_state != state:
            logger.info(
                f"Market state changed: {symbol} "
                f"{old_state} → {state.value} "
                f"(avg_spread={avg_spread:.3f}%)"
            )
        self._market_state[symbol] = state

    def _calculate_confidence(
        self, frames: list[Frame], last_frame: Frame
    ) -> float:
        """
        Рассчитать уверенность сигнала (0.0 - 1.0).

        Факторы:
        - Насколько спред превышает минимум
        - Консистентность направления
        - Объём
        """
        # Фактор спреда: насколько спред выше минимума
        if self.min_spread_size > 0:
            spread_factor = min(
                last_frame.spread_pct / (self.min_spread_size * 2),
                1.0
            )
        else:
            spread_factor = 1.0

        # Фактор консистентности направления
        if len(frames) >= 2:
            consistent = 0
            for i in range(1, len(frames)):
                prev_mid = (frames[i-1].max_price + frames[i-1].min_price) / 2
                curr_mid = (frames[i].max_price + frames[i].min_price) / 2
                if curr_mid != prev_mid:
                    consistent += 1
            direction_factor = consistent / (len(frames) - 1)
        else:
            direction_factor = 0.5

        # Фактор объёма
        avg_vol = sum(f.quote_volume for f in frames) / len(frames)
        if self.min_quote_volume > 0:
            volume_factor = min(
                avg_vol / (self.min_quote_volume * 3), 1.0
            )
        else:
            volume_factor = 1.0

        confidence = (
            spread_factor * 0.5
            + direction_factor * 0.3
            + volume_factor * 0.2
        )

        return round(min(confidence, 1.0), 3)

    def get_stats(self, symbol: str = None) -> dict:
        """Статистика анализатора"""
        if symbol:
            frames = list(self._frames.get(symbol, []))
            return {
                'symbol': symbol,
                'market_state': self._market_state.get(
                    symbol, MarketState.NORMAL
                ).value,
                'frames_collected': len(frames),
                'frames_needed': self._frames_needed,
                'signals_generated': self._signals_by_symbol.get(
                    symbol, 0
                ),
                'current_frame_active': (
                    self._current_frame.get(symbol) is not None
                ),
            }

        return {
            'total_signals': self._signals_generated,
            'symbols': {
                sym: self.get_stats(sym)
                for sym in self._frames.keys()
            },
            'config': {
                'frame_size': self.frame_size,
                'time_frame': self.time_frame,
                'frames_needed': self._frames_needed,
                'min_spread_size': self.min_spread_size,
                'min_trades_per_frame': self.min_trades_per_frame,
                'min_quote_volume': self.min_quote_volume,
            },
        }

