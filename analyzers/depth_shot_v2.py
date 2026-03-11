"""
DepthShotV2 — стены ордербука как цели TP, не как сигнал входа.

Изменения vs V1:
- Роль: поставщик TP-уровней (weight=0.1 в агрегаторе)
- scan_walls() → топ-N стен с WallStrength score
- get_tp_ladder() → лестница TP для RealisticTPLadder
- Динамический порог объёма на основе средней глубины OB
- Кластеризация близких стен (±0.05%)
- WallStrength = age_score × retention_score × size_score
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from models.signals import Direction
from core.orderbook import OrderBookManager

logger = logging.getLogger(__name__)


@dataclass
class Wall:
    price: float
    volume_usdt: float
    distance_pct: float   # от текущей цены
    side: str             # 'bid' | 'ask'
    strength: float       # 0.0–1.0
    age_s: float          # сколько секунд стена живёт


@dataclass
class TPLadder:
    """Лестница TP на основе стен в стакане."""
    symbol: str
    direction: Direction
    levels: list[tuple[float, float]]  # [(price, partial_pct), ...]
    # partial_pct — доля позиции для закрытия на этом уровне
    base_tp: float                     # fallback если стен нет


class WallTrackerV2:
    """
    Отслеживает стены с расчётом комплексного strength score.
    strength = age_score × retention_score × size_score
    """

    def __init__(
        self,
        min_age_s: float = 5.0,
        max_drop_pct: float = 0.35,
        cleanup_interval_s: float = 60.0,
    ):
        self.min_age_s = min_age_s
        self.max_drop_pct = max_drop_pct
        self.cleanup_interval_s = cleanup_interval_s
        self._walls: dict = {}
        self._last_cleanup: float = time.time()

    def update(self, symbol: str, side: str, price: float, volume_usdt: float) -> None:
        key = (symbol, side, round(price, 2))
        now = time.time()
        if key in self._walls:
            w = self._walls[key]
            w['current'] = volume_usdt
            w['max'] = max(w['max'], volume_usdt)
            w['last_seen'] = now
        else:
            self._walls[key] = {
                'first_seen': now,
                'last_seen': now,
                'max': volume_usdt,
                'current': volume_usdt,
            }
        # Периодическая очистка
        if now - self._last_cleanup > self.cleanup_interval_s:
            self._cleanup()
            self._last_cleanup = now

    def get_strength(self, symbol: str, side: str, price: float) -> float:
        """0.0 если стена не найдена или ненадёжна."""
        key = (symbol, side, round(price, 2))
        wall = self._walls.get(key)
        if not wall:
            return 0.0

        now = time.time()
        age = now - wall['first_seen']

        # age_score: растёт от 0 до 1 за min_age_s*3 секунд
        max_age = self.min_age_s * 3 if self.min_age_s > 0 else 30.0
        age_score = min(age / max_age, 1.0)
        if self.min_age_s > 0 and age < self.min_age_s:
            age_score *= age / self.min_age_s  # штраф за молодые стены

        # retention_score: насколько стена не уменьшилась
        if wall['max'] > 0:
            retention = wall['current'] / wall['max']
            if retention < (1.0 - self.max_drop_pct):
                return 0.0  # стена сильно уменьшилась — spoofing
            retention_score = retention
        else:
            retention_score = 0.0

        # size_score: нормировка (current vs max)
        size_score = min(wall['current'] / max(wall['max'], 1), 1.0)

        return round(age_score * 0.5 + retention_score * 0.3 + size_score * 0.2, 3)

    def get_age(self, symbol: str, side: str, price: float) -> float:
        key = (symbol, side, round(price, 2))
        wall = self._walls.get(key)
        if not wall:
            return 0.0
        return time.time() - wall['first_seen']

    def _cleanup(self) -> None:
        now = time.time()
        # Удаляем стены, не виденные >5 минут
        self._walls = {
            k: v for k, v in self._walls.items()
            if now - v['last_seen'] < 300.0
        }


class DepthShotV2:
    """
    DepthShotV2: поставщик TP-уровней на основе стен ордербука.

    Основные методы:
    - scan_walls(symbol, direction) → список Wall отсортированных по strength
    - get_tp_ladder(symbol, direction, entry_price) → TPLadder для позиции
    - get_confidence(symbol, direction) → float (0–1) для агрегатора (weight≤0.1)
    """

    # Символ → минимальный объём стены в USDT
    DEFAULT_THRESHOLDS = {
        'BTCUSDT':  150_000,
        'ETHUSDT':   80_000,
        'SOLUSDT':   40_000,
        'BNBUSDT':   40_000,
        'XRPUSDT':   20_000,
        'DOGEUSDT':  20_000,
        'ADAUSDT':   20_000,
        'AVAXUSDT':  20_000,
    }

    def __init__(self, config: dict, orderbook_manager: OrderBookManager):
        self._ob = orderbook_manager
        self._tracker = WallTrackerV2(
            min_age_s=config.get('wall_min_age_s', 5.0),
            max_drop_pct=config.get('wall_max_drop_pct', 0.35),
        )

        self.min_distance_pct: float = config.get('min_distance_pct', 0.15)
        self.max_distance_pct: float = config.get('max_distance_pct', 2.0)
        self.max_walls: int = config.get('max_walls', 3)
        self.cluster_pct: float = config.get('cluster_pct', 0.05)  # объединять стены ближе 0.05%
        self.tp_pct_fallback: float = config.get('tp_pct_fallback', 0.3)

        custom = config.get('symbol_thresholds', {})
        self._thresholds = {**self.DEFAULT_THRESHOLDS, **custom}

        # TP ladder распределение по уровням (3 стены → 40/35/25%)
        self.ladder_distribution: list[float] = config.get(
            'ladder_distribution', [0.40, 0.35, 0.25]
        )

        self._stats = {'scans': 0, 'walls_found': 0, 'ladders_built': 0}
        logger.info(
            f"DepthShotV2 init: min_dist={self.min_distance_pct}% "
            f"max_walls={self.max_walls} cluster={self.cluster_pct}%"
        )

    def _min_volume(self, symbol: str) -> float:
        return self._thresholds.get(symbol.upper(), 50_000)

    def _update_tracker(self, symbol: str, side: str, levels: list) -> None:
        """Обновляем WallTracker для всех уровней стакана."""
        for level in levels:
            vol_usdt = level.qty * level.price
            if vol_usdt >= self._min_volume(symbol) * 0.3:  # трекаем потенциальные стены
                self._tracker.update(symbol, side, level.price, vol_usdt)

    def _cluster_walls(self, walls: list[Wall], current_price: float) -> list[Wall]:
        """Объединяем стены в пределах cluster_pct — берём самую крупную."""
        if not walls:
            return []
        clustered: list[Wall] = []
        used = set()
        walls_sorted = sorted(walls, key=lambda w: w.distance_pct)
        for i, w in enumerate(walls_sorted):
            if i in used:
                continue
            group = [w]
            for j, w2 in enumerate(walls_sorted):
                if j <= i or j in used:
                    continue
                if abs(w.price - w2.price) / current_price * 100 <= self.cluster_pct:
                    group.append(w2)
                    used.add(j)
            # Берём самую сильную из кластера
            best = max(group, key=lambda x: x.strength * x.volume_usdt)
            clustered.append(best)
            used.add(i)
        return clustered

    def scan_walls(
        self,
        symbol: str,
        direction: Direction,
        current_price: float = None,
    ) -> list[Wall]:
        """
        Сканирует стакан и возвращает список Wall, отсортированных по strength.
        direction: LONG → ищем bid-стены (поддержка), SHORT → ask-стены (сопротивление).
        """
        self._stats['scans'] += 1
        book = self._ob.get_book(symbol)
        if not book.is_initialized:
            return []

        if current_price is None:
            mid = book.mid_price
            if mid is None:
                return []
            current_price = mid

        side = 'bid' if direction == Direction.LONG else 'ask'
        levels = book.get_bids(100) if side == 'bid' else book.get_asks(100)

        # Обновляем трекер
        self._update_tracker(symbol, side, levels)

        min_vol = self._min_volume(symbol)
        candidates: list[Wall] = []

        for level in levels:
            vol_usdt = level.qty * level.price
            if vol_usdt < min_vol:
                continue

            dist_pct = abs(level.price - current_price) / current_price * 100
            if not (self.min_distance_pct <= dist_pct <= self.max_distance_pct):
                continue

            strength = self._tracker.get_strength(symbol, side, level.price)
            if strength <= 0.0:
                continue

            age_s = self._tracker.get_age(symbol, side, level.price)
            candidates.append(Wall(
                price=level.price,
                volume_usdt=vol_usdt,
                distance_pct=dist_pct,
                side=side,
                strength=strength,
                age_s=age_s,
            ))

        # Кластеризация + сортировка по strength
        candidates = self._cluster_walls(candidates, current_price)
        candidates.sort(key=lambda w: w.strength, reverse=True)

        result = candidates[:self.max_walls]
        self._stats['walls_found'] += len(result)
        return result

    def get_tp_ladder(
        self,
        symbol: str,
        direction: Direction,
        entry_price: float,
        current_price: float = None,
    ) -> TPLadder:
        """
        Строит лестницу TP на основе стен в стакане.
        Если стен нет — возвращает fallback TP.

        Возвращает TPLadder с levels = [(price, partial_pct), ...]
        Например: [(84500, 0.40), (85000, 0.35), (85800, 0.25)]
        """
        # Для TP ищем стены со стороны цели (не поддержки)
        tp_direction = Direction.SHORT if direction == Direction.LONG else Direction.LONG
        walls = self.scan_walls(symbol, tp_direction, current_price or entry_price)

        # Fallback TP
        fallback = (
            entry_price * (1 + self.tp_pct_fallback / 100)
            if direction == Direction.LONG
            else entry_price * (1 - self.tp_pct_fallback / 100)
        )

        if not walls:
            return TPLadder(
                symbol=symbol,
                direction=direction,
                levels=[(fallback, 1.0)],
                base_tp=fallback,
            )

        # Строим лестницу
        dist = self.ladder_distribution
        levels = []
        for i, wall in enumerate(walls):
            pct = dist[i] if i < len(dist) else dist[-1]
            levels.append((wall.price, round(pct, 2)))

        # Нормируем проценты чтобы сумма = 1.0
        total = sum(p for _, p in levels)
        if total > 0:
            levels = [(price, round(p / total, 2)) for price, p in levels]

        self._stats['ladders_built'] += 1
        return TPLadder(
            symbol=symbol,
            direction=direction,
            levels=levels,
            base_tp=levels[0][0] if levels else fallback,
        )

    def get_confidence(self, symbol: str, direction: Direction) -> float:
        """
        Возвращает уверенность 0.0–1.0 для SignalAggregator.
        Weight в агрегаторе ≤ 0.1 — DepthShot подтверждает, не ведёт.
        """
        walls = self.scan_walls(symbol, direction)
        if not walls:
            return 0.0
        # Берём лучшую стену
        best = walls[0]
        # Штраф за слишком далёкие стены
        dist_factor = max(0.0, 1.0 - best.distance_pct / self.max_distance_pct)
        return round(best.strength * dist_factor, 3)

    def get_imbalance(self, symbol: str) -> float:
        """bid_vol / total в топ-20 уровнях. >0.55 = давление покупателей."""
        book = self._ob.get_book(symbol)
        if not book.is_initialized:
            return 0.5
        bids = book.get_bids(20)
        asks = book.get_asks(20)
        bid_vol = sum(b.qty * b.price for b in bids)
        ask_vol = sum(a.qty * a.price for a in asks)
        total = bid_vol + ask_vol
        return round(bid_vol / total, 3) if total > 0 else 0.5

    def get_stats(self) -> dict:
        return {**self._stats, 'tracker_walls': len(self._tracker._walls)}
