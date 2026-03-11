"""
RealisticTPLadder — многоуровневое частичное закрытие позиции.

Заменяет примитивный partial_tp_price в Position на лестницу уровней.
Интегрируется с DepthShotV2.get_tp_ladder() для динамических целей.

Использование в PositionManager:
    ladder = RealisticTPLadder.from_depth(depth_v2, symbol, direction, entry)
    pos.tp_ladder = ladder

В update_positions():
    hits = pos.tp_ladder.get_hits(current_price)
    for level in hits:
        await partial_close(qty * level.fraction)
        pos.tp_ladder.mark_done(level)
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

from models.signals import Direction

if TYPE_CHECKING:
    from analyzers.depth_shot_v2 import DepthShotV2

logger = logging.getLogger(__name__)


@dataclass
class TPLevel:
    price: float
    fraction: float     # доля позиции 0.0–1.0
    label: str = ""     # 'wall_1', 'wall_2', 'fallback', ...
    done: bool = False
    hit_time: float = 0.0
    hit_price: float = 0.0


@dataclass
class RealisticTPLadder:
    """
    Лестница TP уровней для одной позиции.

    Атрибуты:
        symbol:    торговый символ
        direction: LONG | SHORT
        entry:     цена входа
        levels:    список TPLevel отсортированных по удалённости от entry
        created_at: время создания
    """
    symbol: str
    direction: Direction
    entry: float
    levels: list[TPLevel] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)

    # ── Фабричные методы ──────────────────────────────────────────────────────

    @classmethod
    def from_depth(
        cls,
        depth: 'DepthShotV2',
        symbol: str,
        direction: Direction,
        entry_price: float,
        current_price: float = None,
    ) -> 'RealisticTPLadder':
        """
        Строит лестницу из стен ордербука (основной путь).
        Если стен нет — fallback на фиксированные уровни.
        """
        ladder_data = depth.get_tp_ladder(
            symbol, direction, entry_price, current_price
        )
        levels = []
        for i, (price, fraction) in enumerate(ladder_data.levels):
            label = f"wall_{i+1}" if len(ladder_data.levels) > 1 else "fallback"
            levels.append(TPLevel(price=price, fraction=fraction, label=label))

        obj = cls(symbol=symbol, direction=direction, entry=entry_price, levels=levels)
        logger.info(
            "[%s] TPLadder from depth: %d levels %s",
            symbol, len(levels),
            [(round(l.price, 2), f"{l.fraction*100:.0f}%") for l in levels]
        )
        return obj

    @classmethod
    def fixed(
        cls,
        symbol: str,
        direction: Direction,
        entry_price: float,
        tp_pcts: list[float] = None,
        fractions: list[float] = None,
    ) -> 'RealisticTPLadder':
        """
        Строит лестницу на фиксированных % от entry.
        tp_pcts: [0.3, 0.6, 1.0] — % от entry
        fractions: [0.4, 0.35, 0.25] — доля позиции
        """
        tp_pcts = tp_pcts or [0.3, 0.6, 1.0]
        fractions = fractions or [0.40, 0.35, 0.25]

        # Нормируем fractions
        total = sum(fractions)
        fractions = [f / total for f in fractions]

        levels = []
        for i, (pct, frac) in enumerate(zip(tp_pcts, fractions)):
            if direction == Direction.LONG:
                price = entry_price * (1 + pct / 100)
            else:
                price = entry_price * (1 - pct / 100)
            levels.append(TPLevel(
                price=round(price, 6),
                fraction=round(frac, 3),
                label=f"fixed_{i+1}",
            ))

        obj = cls(symbol=symbol, direction=direction, entry=entry_price, levels=levels)
        logger.info(
            "[%s] TPLadder fixed: %d levels",
            symbol, len(levels)
        )
        return obj

    # ── Основные методы ───────────────────────────────────────────────────────

    def get_hits(self, current_price: float) -> list[TPLevel]:
        """
        Возвращает уровни которые достигнуты текущей ценой и ещё не закрыты.
        Для LONG: price >= tp_price
        Для SHORT: price <= tp_price
        """
        hits = []
        for level in self.levels:
            if level.done:
                continue
            if self.direction == Direction.LONG:
                if current_price >= level.price:
                    hits.append(level)
            else:
                if current_price <= level.price:
                    hits.append(level)
        return hits

    def mark_done(self, level: TPLevel, actual_price: float = 0.0) -> None:
        """Отмечаем уровень как исполненный."""
        level.done = True
        level.hit_time = time.time()
        level.hit_price = actual_price or level.price
        logger.info(
            "[%s] TPLevel done: %s price=%.4f fraction=%.0f%%",
            self.symbol, level.label, level.hit_price, level.fraction * 100
        )

    def remaining_fraction(self) -> float:
        """Доля позиции ещё не закрытая через лестницу."""
        done_frac = sum(l.fraction for l in self.levels if l.done)
        return round(max(0.0, 1.0 - done_frac), 4)

    def is_complete(self) -> bool:
        """Все уровни исполнены."""
        return all(l.done for l in self.levels)

    def next_level(self) -> Optional[TPLevel]:
        """Следующий незакрытый уровень."""
        for l in self.levels:
            if not l.done:
                return l
        return None

    def breakeven_price(self, buffer_pct: float = 0.05) -> float:
        """
        Цена безубытка с небольшим буфером.
        После первого частичного TP переносим SL сюда.
        """
        buf = self.entry * buffer_pct / 100
        if self.direction == Direction.LONG:
            return round(self.entry + buf, 6)
        else:
            return round(self.entry - buf, 6)

    def should_move_to_breakeven(self) -> bool:
        """True если хотя бы один уровень выполнен → SL на breakeven."""
        return any(l.done for l in self.levels)

    def summary(self) -> dict:
        return {
            'symbol': self.symbol,
            'direction': self.direction.value,
            'entry': self.entry,
            'levels': [
                {
                    'label': l.label,
                    'price': l.price,
                    'fraction': l.fraction,
                    'done': l.done,
                    'hit_price': l.hit_price,
                }
                for l in self.levels
            ],
            'remaining_fraction': self.remaining_fraction(),
            'is_complete': self.is_complete(),
        }
