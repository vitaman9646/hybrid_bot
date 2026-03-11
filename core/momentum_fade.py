"""
MomentumFadeExit — выход из позиции когда импульс угасает.

Логика:
- Отслеживает тики цены после открытия позиции
- Считает momentum score = скорость движения + направленность тиков
- Если momentum падает ниже порога И позиция в прибыли → сигнал на выход
- Защита: не выходим в убыток если momentum просто низкий

Интеграция в engine.py:
    fade = MomentumFadeExit(config)
    fade.update(symbol, price, timestamp)
    if fade.should_exit(symbol, pos.direction, pos.entry_price, current_price):
        await close_position(symbol, reason='momentum_fade')
"""
from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from models.signals import Direction

logger = logging.getLogger(__name__)


@dataclass
class PriceTick:
    price: float
    ts: float


@dataclass
class MomentumState:
    symbol: str
    ticks: deque = field(default_factory=lambda: deque(maxlen=200))
    last_momentum: float = 0.0
    last_update: float = 0.0
    fade_triggered_at: float = 0.0   # когда впервые зафиксировали fade
    exit_signaled: bool = False


class MomentumFadeExit:
    """
    Детектор угасания импульса для выхода из позиции.

    Momentum score (0.0–1.0):
        velocity_score  = скорость движения цены за window_s секунд
        direction_score = % тиков в направлении позиции
        momentum = velocity_score * 0.6 + direction_score * 0.4

    Выход если:
        1. momentum < fade_threshold (дефолт 0.25)
        2. Позиция в прибыли >= min_profit_pct (дефолт 0.1%)
        3. Fade длится >= confirm_s секунд (дефолт 3.0)
    """

    def __init__(self, config: dict = None):
        cfg = config or {}
        self.window_s: float = cfg.get('window_s', 8.0)          # окно анализа тиков
        self.fade_threshold: float = cfg.get('fade_threshold', 0.25)
        self.min_profit_pct: float = cfg.get('min_profit_pct', 0.10)
        self.confirm_s: float = cfg.get('confirm_s', 3.0)         # fade должен держаться N сек
        self.min_ticks: int = cfg.get('min_ticks', 10)            # минимум тиков для анализа
        self.max_loss_pct: float = cfg.get('max_loss_pct', 0.05)  # не выходим если убыток > X%

        self._states: dict[str, MomentumState] = {}
        self._stats = {'updates': 0, 'exits_signaled': 0, 'symbols': set()}

        logger.info(
            "MomentumFadeExit init: window=%.1fs threshold=%.2f "
            "min_profit=%.2f%% confirm=%.1fs",
            self.window_s, self.fade_threshold,
            self.min_profit_pct, self.confirm_s
        )

    def _get_state(self, symbol: str) -> MomentumState:
        if symbol not in self._states:
            self._states[symbol] = MomentumState(symbol=symbol)
        return self._states[symbol]

    def update(self, symbol: str, price: float, ts: float = None) -> None:
        """Добавить новый тик цены."""
        ts = ts or time.time()
        state = self._get_state(symbol)
        state.ticks.append(PriceTick(price=price, ts=ts))
        state.last_update = ts
        self._stats['updates'] += 1
        self._stats['symbols'].add(symbol)

    def get_momentum(self, symbol: str, direction: Direction) -> float:
        """
        Рассчитать momentum score 0.0–1.0.
        direction: направление открытой позиции.
        """
        state = self._get_state(symbol)
        now = time.time()
        cutoff = now - self.window_s

        # Берём тики за последние window_s секунд
        recent = [t for t in state.ticks if t.ts >= cutoff]
        if len(recent) < self.min_ticks:
            return 1.0  # недостаточно данных → считаем импульс живым

        # velocity_score: нормированная скорость движения цены
        price_start = recent[0].price
        price_end = recent[-1].price
        if price_start == 0:
            return 1.0

        raw_velocity = abs(price_end - price_start) / price_start * 100
        # Нормируем: 0.1% за 8 сек = сильный импульс (score=1.0)
        velocity_score = min(raw_velocity / 0.1, 1.0)

        # direction_score: % тиков движущихся в сторону позиции
        favorable = 0
        for i in range(1, len(recent)):
            delta = recent[i].price - recent[i-1].price
            if direction == Direction.LONG and delta > 0:
                favorable += 1
            elif direction == Direction.SHORT and delta < 0:
                favorable += 1

        total_moves = len(recent) - 1
        direction_score = favorable / total_moves if total_moves > 0 else 0.5

        momentum = round(velocity_score * 0.6 + direction_score * 0.4, 4)
        state.last_momentum = momentum
        return momentum

    def should_exit(
        self,
        symbol: str,
        direction: Direction,
        entry_price: float,
        current_price: float,
    ) -> bool:
        """
        True если нужно выйти из позиции из-за угасания импульса.

        Условия:
        1. Позиция в прибыли >= min_profit_pct
        2. Momentum < fade_threshold
        3. Fade подтверждён в течение confirm_s секунд
        """
        state = self._get_state(symbol)

        # Проверяем прибыль
        if direction == Direction.LONG:
            pnl_pct = (current_price - entry_price) / entry_price * 100
        else:
            pnl_pct = (entry_price - current_price) / entry_price * 100

        # Не выходим в убыток (это задача SL, не MomentumFade)
        if pnl_pct < -self.max_loss_pct:
            state.fade_triggered_at = 0.0
            return False

        # Не выходим если прибыль меньше минимума
        if pnl_pct < self.min_profit_pct:
            state.fade_triggered_at = 0.0
            return False

        # Считаем momentum
        momentum = self.get_momentum(symbol, direction)

        now = time.time()
        if momentum < self.fade_threshold:
            # Фиксируем начало fade
            if state.fade_triggered_at == 0.0:
                state.fade_triggered_at = now
                logger.debug(
                    "[%s] MomentumFade triggered: momentum=%.3f pnl=%.3f%%",
                    symbol, momentum, pnl_pct
                )

            # Проверяем подтверждение
            fade_duration = now - state.fade_triggered_at
            if fade_duration >= self.confirm_s:
                if not state.exit_signaled:
                    state.exit_signaled = True
                    self._stats['exits_signaled'] += 1
                    logger.info(
                        "[%s] MomentumFadeExit: momentum=%.3f pnl=%.3f%% "
                        "fade_duration=%.1fs → EXIT",
                        symbol, momentum, pnl_pct, fade_duration
                    )
                return True
        else:
            # Импульс восстановился — сбрасываем
            if state.fade_triggered_at > 0.0:
                logger.debug(
                    "[%s] MomentumFade reset: momentum recovered to %.3f",
                    symbol, momentum
                )
            state.fade_triggered_at = 0.0
            state.exit_signaled = False

        return False

    def reset(self, symbol: str) -> None:
        """Сброс состояния после закрытия позиции."""
        if symbol in self._states:
            state = self._states[symbol]
            state.ticks.clear()
            state.last_momentum = 0.0
            state.fade_triggered_at = 0.0
            state.exit_signaled = False

    def get_stats(self, symbol: str = None) -> dict:
        if symbol:
            state = self._get_state(symbol)
            return {
                'symbol': symbol,
                'ticks_buffered': len(state.ticks),
                'last_momentum': state.last_momentum,
                'fade_triggered_at': state.fade_triggered_at,
                'exit_signaled': state.exit_signaled,
            }
        return {
            'updates': self._stats['updates'],
            'exits_signaled': self._stats['exits_signaled'],
            'symbols_tracked': len(self._stats['symbols']),
        }
