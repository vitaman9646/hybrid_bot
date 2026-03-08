from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable

from models.signals import Direction
from analyzers.vector_analyzer import VectorAnalyzer, VectorSignal, MarketState
from analyzers.averages_analyzer import AveragesAnalyzer, TrendState
from analyzers.depth_shot_analyzer import DepthShotAnalyzer, DepthShotSignal

logger = logging.getLogger(__name__)


class ScenarioType(Enum):
    NONE      = "none"
    SCENARIO1 = "vector_depth"       # Vector → Depth Shot
    SCENARIO2 = "averages_vector"    # Averages → Vector
    SCENARIO3 = "averages_depth"     # Averages → Depth Shot (mean reversion)
    SCENARIO4 = "all_three"          # Averages + Vector + Depth (максимальный фильтр)


class AggregationMode(Enum):
    AND      = "and"       # все анализаторы согласны
    OR       = "or"        # хотя бы один
    WEIGHTED = "weighted"  # взвешенная сумма


@dataclass
class AggregatedSignal:
    symbol: str
    direction: Direction
    timestamp: float
    scenario: ScenarioType
    entry_price: float
    tp_price: float
    confidence: float           # итоговая уверенность 0.0-1.0
    score: float                # взвешенная сумма
    vector_confidence: float    # 0 если не участвовал
    averages_confidence: float
    depth_confidence: float
    market_state: MarketState
    notes: str = ""
    size_usdt: float = 0.0      # устанавливается RiskManager перед открытием


class SignalAggregator:
    """
    Связывает Vector, Averages и DepthShot.
    Реализует 4 сценария гибридной стратегии.

    Сценарий 1: Vector (импульс) → Depth (точка входа)
    Сценарий 2: Averages (тренд) → Vector (импульс по тренду)
    Сценарий 3: Averages (перепроданность) → Depth (стена как поддержка)
    Сценарий 4: Averages + Vector + Depth (максимальный фильтр)
    """

    def __init__(
        self,
        config: dict,
        vector: VectorAnalyzer,
        averages: AveragesAnalyzer,
        depth: DepthShotAnalyzer,
    ):
        self._vector = vector
        self._averages = averages
        self._depth = depth

        # Режим агрегации
        self.mode = AggregationMode(config.get('mode', 'weighted'))

        # Веса анализаторов
        self._weights = {
            'vector':   config.get('weight_vector', 0.5),
            'averages': config.get('weight_averages', 0.3),
            'depth':    config.get('weight_depth', 0.2),
        }

        # Порог для входа (weighted mode)
        self.entry_threshold: float = config.get('entry_threshold', 0.6)

        # Порог для Сценария 4 (нормальный рынок)
        self.scenario4_vol_min: float = config.get('scenario4_vol_min', 0.1)
        self.scenario4_vol_max: float = config.get('scenario4_vol_max', 3.0)

        # Orderbook imbalance фильтр
        self.use_imbalance_filter: bool = config.get('use_imbalance_filter', True)
        self.imbalance_threshold: float = config.get('imbalance_threshold', 0.55)

        # Cooldown между сигналами по символу (секунды)
        self.signal_cooldown: float = config.get('signal_cooldown', 10.0)
        self._last_signal_time: dict[str, float] = {}

        # Статистика
        self._total_evaluated: int = 0
        self._total_signals: int = 0
        self._signals_by_scenario: dict[str, int] = {
            s.value: 0 for s in ScenarioType
        }
        self._conflicts_logged: int = 0

        # Callbacks
        self._signal_callbacks: list[Callable] = []

        logger.info(
            f"SignalAggregator init: mode={self.mode.value} "
            f"threshold={self.entry_threshold} "
            f"weights=v{self._weights['vector']}/"
            f"a{self._weights['averages']}/"
            f"d{self._weights['depth']}"
        )

    def on_signal(self, callback: Callable[[AggregatedSignal], None]):
        self._signal_callbacks.append(callback)

    def evaluate(
        self,
        symbol: str,
        vector_signal: Optional[VectorSignal] = None,
        current_price: float = None,
    ) -> Optional[AggregatedSignal]:
        """
        Главный метод — оценивает все условия и выбирает сценарий.
        Вызывается из engine при каждом Vector сигнале или по таймеру.
        """
        self._total_evaluated += 1

        # Cooldown проверка
        if self._is_in_cooldown(symbol):
            return None

        # Получаем текущее состояние всех анализаторов
        market_state = self._vector.get_market_state(symbol)
        trend = self._averages.get_trend(symbol)
        delta_pct = self._averages.get_delta(symbol)

        # Выбираем сценарий по состоянию рынка
        if market_state in (MarketState.DEAD, MarketState.CHAOS):
            return None

        # Пробуем сценарии по приоритету
        signal = None

        if market_state == MarketState.NORMAL:
            # Приоритет: Сценарий 4 → 3 → 2 → 1
            signal = self._try_scenario4(
                symbol, vector_signal, current_price,
                trend, market_state
            )
            if signal is None:
                signal = self._try_scenario3(
                    symbol, current_price, trend, delta_pct, market_state
                )

        elif market_state == MarketState.VOLATILE:
            # Приоритет: Сценарий 2 → 1
            signal = self._try_scenario2(
                symbol, vector_signal, current_price,
                trend, market_state
            )
            if signal is None and vector_signal is not None:
                signal = self._try_scenario1(
                    symbol, vector_signal, current_price, market_state
                )

        if signal:
            self._emit_signal(symbol, signal)

        return signal

    # ─────────────────────────────────────────
    # СЦЕНАРИИ
    # ─────────────────────────────────────────

    def _try_scenario4(
        self, symbol, vector_signal, current_price,
        trend, market_state
    ) -> Optional[AggregatedSignal]:
        """
        Сценарий 4: Averages + Vector + Depth.
        Максимально отфильтрованный вход.
        Все три должны согласиться.
        """
        # 1. Averages: тренд определён
        if trend == TrendState.FLAT:
            return None

        direction = Direction.LONG if trend == TrendState.UP else Direction.SHORT

        # 2. Vector: рынок в нормальном состоянии (не dead, не chaos)
        #    и есть сигнал в направлении тренда
        if vector_signal is None:
            return None
        if vector_signal.direction != direction:
            self._log_conflict(symbol, "S4", vector_signal.direction, direction)
            return None

        # 3. Averages разрешает направление
        if not self._averages.allows_direction(symbol, direction):
            return None

        # 4. Depth: находим уровень
        depth_signal = self._depth.scan(symbol, direction, current_price)
        if depth_signal is None:
            return None

        # 5. Imbalance фильтр
        if not self._check_imbalance(symbol, direction):
            return None

        # Считаем score
        score = (
            vector_signal.confidence * self._weights['vector'] +
            min(abs(self._averages.get_delta(symbol)) / 0.5, 1.0) * self._weights['averages'] +
            depth_signal.confidence * self._weights['depth']
        )

        if self.mode == AggregationMode.WEIGHTED and score < self.entry_threshold:
            return None

        return AggregatedSignal(
            symbol=symbol,
            direction=direction,
            timestamp=time.time(),
            scenario=ScenarioType.SCENARIO4,
            entry_price=depth_signal.entry_price,
            tp_price=depth_signal.tp_price,
            confidence=round(score, 3),
            score=round(score, 3),
            vector_confidence=vector_signal.confidence,
            averages_confidence=min(abs(self._averages.get_delta(symbol)) / 0.5, 1.0),
            depth_confidence=depth_signal.confidence,
            market_state=market_state,
            notes=f"S4: trend={trend.value} spread={vector_signal.spread_pct:.2f}%",
        )

    def _try_scenario2(
        self, symbol, vector_signal, current_price,
        trend, market_state
    ) -> Optional[AggregatedSignal]:
        """
        Сценарий 2: Averages (тренд) → Vector (импульс по тренду).
        Волатильный рынок с чётким трендом.
        """
        if trend == TrendState.FLAT:
            return None
        if vector_signal is None:
            return None

        direction = Direction.LONG if trend == TrendState.UP else Direction.SHORT

        if vector_signal.direction != direction:
            self._log_conflict(symbol, "S2", vector_signal.direction, direction)
            return None

        if not self._averages.allows_direction(symbol, direction):
            return None

        averages_conf = min(abs(self._averages.get_delta(symbol)) / 0.5, 1.0)
        score = (
            vector_signal.confidence * self._weights['vector'] +
            averages_conf * self._weights['averages']
        )

        if self.mode == AggregationMode.WEIGHTED and score < self.entry_threshold * 0.8:
            return None

        # Вход по текущей цене (импульс уже идёт)
        entry = current_price or vector_signal.upper_border
        spread = vector_signal.upper_border - vector_signal.lower_border
        if direction == Direction.LONG:
            tp = entry + spread * 2
        else:
            tp = entry - spread * 2

        return AggregatedSignal(
            symbol=symbol,
            direction=direction,
            timestamp=time.time(),
            scenario=ScenarioType.SCENARIO2,
            entry_price=entry,
            tp_price=tp,
            confidence=round(score, 3),
            score=round(score, 3),
            vector_confidence=vector_signal.confidence,
            averages_confidence=averages_conf,
            depth_confidence=0.0,
            market_state=market_state,
            notes=f"S2: trend={trend.value} impulse={vector_signal.spread_pct:.2f}%",
        )

    def _try_scenario1(
        self, symbol, vector_signal, current_price,
        market_state
    ) -> Optional[AggregatedSignal]:
        """
        Сценарий 1: Vector (импульс) → Depth (уровень объёма).
        Нет чёткого тренда, но есть импульс и уровень.
        """
        if vector_signal is None:
            return None

        direction = vector_signal.direction

        # Depth ищет уровень в направлении импульса
        depth_signal = self._depth.scan(symbol, direction, current_price)
        if depth_signal is None:
            return None

        if not self._check_imbalance(symbol, direction):
            return None

        score = (
            vector_signal.confidence * self._weights['vector'] +
            depth_signal.confidence * self._weights['depth']
        )

        if self.mode == AggregationMode.WEIGHTED and score < self.entry_threshold * 0.7:
            return None

        return AggregatedSignal(
            symbol=symbol,
            direction=direction,
            timestamp=time.time(),
            scenario=ScenarioType.SCENARIO1,
            entry_price=depth_signal.entry_price,
            tp_price=depth_signal.tp_price,
            confidence=round(score, 3),
            score=round(score, 3),
            vector_confidence=vector_signal.confidence,
            averages_confidence=0.0,
            depth_confidence=depth_signal.confidence,
            market_state=market_state,
            notes=f"S1: impulse={vector_signal.spread_pct:.2f}% vol={depth_signal.volume_at_level:,.0f}",
        )

    def _try_scenario3(
        self, symbol, current_price, trend,
        delta_pct, market_state
    ) -> Optional[AggregatedSignal]:
        """
        Сценарий 3: Averages (перепроданность) → Depth (стена как поддержка).
        Mean reversion с коротким TP.
        """
        # Определяем направление по перепроданности/перекупленности
        if self._averages.is_oversold(symbol):
            direction = Direction.LONG
        elif self._averages.is_overbought(symbol):
            direction = Direction.SHORT
        else:
            return None

        # Depth ищет стену
        depth_signal = self._depth.scan(symbol, direction, current_price)
        if depth_signal is None:
            return None

        # Цена должна быть близко к стене (не дальше 0.5%)
        if depth_signal.distance_pct > 0.5:
            return None

        averages_conf = min(abs(delta_pct) / abs(self._averages.oversold_delta), 1.0)
        score = (
            averages_conf * self._weights['averages'] +
            depth_signal.confidence * self._weights['depth']
        )

        if self.mode == AggregationMode.WEIGHTED and score < self.entry_threshold * 0.7:
            return None

        # Короткий TP для mean reversion
        if direction == Direction.LONG:
            tp = depth_signal.entry_price * 1.003  # 0.3%
        else:
            tp = depth_signal.entry_price * 0.997

        return AggregatedSignal(
            symbol=symbol,
            direction=direction,
            timestamp=time.time(),
            scenario=ScenarioType.SCENARIO3,
            entry_price=depth_signal.entry_price,
            tp_price=tp,
            confidence=round(score, 3),
            score=round(score, 3),
            vector_confidence=0.0,
            averages_confidence=averages_conf,
            depth_confidence=depth_signal.confidence,
            market_state=market_state,
            notes=f"S3: delta={delta_pct:.3f}% wall_dist={depth_signal.distance_pct:.3f}%",
        )

    # ─────────────────────────────────────────
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # ─────────────────────────────────────────

    def _check_imbalance(self, symbol: str, direction: Direction) -> bool:
        if not self.use_imbalance_filter:
            return True
        imbalance = self._depth.get_orderbook_imbalance(symbol)
        if direction == Direction.LONG:
            return imbalance >= self.imbalance_threshold
        else:
            return imbalance <= (1 - self.imbalance_threshold)

    def _is_in_cooldown(self, symbol: str) -> bool:
        last = self._last_signal_time.get(symbol, 0)
        return (time.time() - last) < self.signal_cooldown

    def _log_conflict(self, symbol, scenario, got, expected):
        self._conflicts_logged += 1
        logger.debug(
            f"Conflict {scenario} {symbol}: "
            f"Vector={got.value} vs Trend={expected.value}"
        )

    def _emit_signal(self, symbol: str, signal: AggregatedSignal):
        self._total_signals += 1
        self._signals_by_scenario[signal.scenario.value] += 1
        self._last_signal_time[symbol] = time.time()

        logger.info(
            f"SIGNAL [{signal.scenario.value}] {symbol} "
            f"{signal.direction.value} "
            f"entry={signal.entry_price:.2f} "
            f"tp={signal.tp_price:.2f} "
            f"score={signal.score:.2f}"
        )

        for cb in self._signal_callbacks:
            try:
                cb(signal)
            except Exception as e:
                logger.error(f"Aggregator callback error: {e}")

    def get_stats(self) -> dict:
        return {
            'mode': self.mode.value,
            'total_evaluated': self._total_evaluated,
            'total_signals': self._total_signals,
            'conflicts_logged': self._conflicts_logged,
            'by_scenario': self._signals_by_scenario.copy(),
            'weights': self._weights.copy(),
            'threshold': self.entry_threshold,
        }
