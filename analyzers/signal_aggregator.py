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
    SCENARIO1 = "vector_depth"
    SCENARIO2 = "averages_vector"
    SCENARIO3 = "averages_depth"
    SCENARIO4 = "all_three"


class AggregationMode(Enum):
    AND      = "and"
    OR       = "or"
    WEIGHTED = "weighted"


class ExitReason(Enum):
    CLOSE   = "close"
    REVERSE = "reverse"


@dataclass
class OppositeExitSignal:
    symbol: str
    reason: ExitReason
    score: float
    timestamp: float


@dataclass
class AggregatedSignal:
    symbol: str
    direction: Direction
    timestamp: float
    scenario: ScenarioType
    entry_price: float
    tp_price: float
    confidence: float
    score: float
    vector_confidence: float
    averages_confidence: float
    depth_confidence: float
    market_state: MarketState
    notes: str = ""
    size_usdt: float = 0.0


class SignalAggregator:
    """
    Связывает Vector, Averages и DepthShot.
    v2: per-scenario thresholds, умный cooldown, opposite exit.
    """

    def __init__(self, config, vector, averages, depth):
        self._vector = vector
        self._averages = averages
        self._depth = depth

        self.mode = AggregationMode(config.get('mode', 'weighted'))

        self._weights = {
            'vector':   config.get('weight_vector', 0.5),
            'averages': config.get('weight_averages', 0.3),
            'depth':    config.get('weight_depth', 0.2),
        }

        # Per-scenario thresholds
        thresholds = config.get('thresholds', {})
        self._thresholds = {
            ScenarioType.SCENARIO4.value: thresholds.get('all_three',       0.40),
            ScenarioType.SCENARIO2.value: thresholds.get('averages_vector', 0.60),
            ScenarioType.SCENARIO1.value: thresholds.get('vector_depth',    0.55),
            ScenarioType.SCENARIO3.value: thresholds.get('averages_depth',  0.65),
        }
        self.entry_threshold: float = config.get('entry_threshold', 0.40)

        self.scenario4_vol_min: float = config.get('scenario4_vol_min', 0.1)
        self.scenario4_vol_max: float = config.get('scenario4_vol_max', 3.0)
        self.use_imbalance_filter: bool = config.get('use_imbalance_filter', True)
        self.imbalance_threshold: float = config.get('imbalance_threshold', 0.55)

        # Умный cooldown
        cooldown_cfg = config.get('cooldown', {})
        self._cooldown_default:  float = cooldown_cfg.get('default',   60.0)
        self._cooldown_after_tp: float = cooldown_cfg.get('after_tp',  45.0)
        self._cooldown_after_sl: float = cooldown_cfg.get('after_sl', 120.0)

        self._last_signal_time: dict[str, float] = {}
        self._last_exit_reason: dict[str, str]   = {}

        # Opposite exit
        opp = config.get('opposite_exit', {})
        self._opposite_exit_enabled:      bool  = opp.get('enabled', True)
        self._opposite_close_threshold:   float = opp.get('close_threshold',   0.50)
        self._opposite_reverse_threshold: float = opp.get('reverse_threshold', 0.85)

        self._signal_callbacks: list[Callable] = []
        self._exit_callbacks:   list[Callable] = []

        self._total_evaluated   = 0
        self._total_signals     = 0
        self._conflicts_logged  = 0
        self._opposite_exits    = 0
        self._signals_by_scenario: dict[str, int] = {s.value: 0 for s in ScenarioType}

        logger.info(
            "SignalAggregator init: mode=%s "
            "thresholds=S4:%.2f/S2:%.2f/S1:%.2f/S3:%.2f "
            "cooldown=tp:%.0fs/sl:%.0fs "
            "weights=v%.1f/a%.1f/d%.1f",
            self.mode.value,
            self._thresholds[ScenarioType.SCENARIO4.value],
            self._thresholds[ScenarioType.SCENARIO2.value],
            self._thresholds[ScenarioType.SCENARIO1.value],
            self._thresholds[ScenarioType.SCENARIO3.value],
            self._cooldown_after_tp, self._cooldown_after_sl,
            self._weights['vector'], self._weights['averages'], self._weights['depth'],
        )

    def on_signal(self, callback):
        self._signal_callbacks.append(callback)

    def on_opposite_exit(self, callback):
        self._exit_callbacks.append(callback)

    def notify_exit(self, symbol: str, exit_reason: str):
        """Вызывается из engine при закрытии позиции. exit_reason: 'tp'|'sl'"""
        self._last_exit_reason[symbol] = exit_reason
        logger.debug("SignalAggregator: exit notified %s reason=%s", symbol, exit_reason)

    def evaluate(
        self,
        symbol: str,
        vector_signal=None,
        current_price: float = None,
        current_position_direction=None,
    ):
        self._total_evaluated += 1

        # Opposite exit
        if self._opposite_exit_enabled and current_position_direction is not None:
            exit_sig = self._check_opposite_exit(
                symbol, vector_signal, current_price, current_position_direction
            )
            if exit_sig is not None:
                self._emit_exit(symbol, exit_sig)
                return None

        if self._is_in_cooldown(symbol):
            return None

        market_state = self._vector.get_market_state(symbol)
        trend        = self._averages.get_trend(symbol)
        delta_pct    = self._averages.get_delta(symbol)

        if market_state in (MarketState.DEAD, MarketState.CHAOS):
            return None

        # Временный диагностический лог
        if self._total_evaluated % 500 == 0:
            logger.info("[%s] evaluate: state=%s trend=%s vector=%s evaluated=%d",
                        symbol, market_state, trend,
                        vector_signal.direction if vector_signal else None,
                        self._total_evaluated)

        signal = None
        if market_state == MarketState.NORMAL:
            signal = self._try_scenario4(symbol, vector_signal, current_price, trend, market_state)
            if signal is None:
                signal = self._try_scenario3(symbol, current_price, trend, delta_pct, market_state)
        elif market_state == MarketState.VOLATILE:
            signal = self._try_scenario2(symbol, vector_signal, current_price, trend, market_state)
            if signal is None and vector_signal is not None:
                signal = self._try_scenario1(symbol, vector_signal, current_price, market_state)

        if signal:
            self._emit_signal(symbol, signal)
        return signal

    # ── Opposite exit ────────────────────────────────────────────────────

    def _check_opposite_exit(self, symbol, vector_signal, current_price, position_direction):
        opposite = Direction.SHORT if position_direction == Direction.LONG else Direction.LONG
        if vector_signal is None or vector_signal.direction != opposite:
            return None

        trend = self._averages.get_trend(symbol)
        trend_opposite = (
            (opposite == Direction.LONG  and trend == TrendState.UP) or
            (opposite == Direction.SHORT and trend == TrendState.DOWN)
        )

        score = vector_signal.confidence * self._weights['vector']
        if trend_opposite:
            averages_conf = min(abs(self._averages.get_delta(symbol)) / 0.5, 1.0)
            score += averages_conf * self._weights['averages']

        depth_signal = self._depth.scan(symbol, opposite, current_price)
        if depth_signal:
            score += depth_signal.confidence * self._weights['depth']

        if score >= self._opposite_reverse_threshold:
            logger.info("OPPOSITE EXIT [REVERSE] %s: pos=%s score=%.2f",
                        symbol, position_direction.value, score)
            self._opposite_exits += 1
            return OppositeExitSignal(symbol=symbol, reason=ExitReason.REVERSE,
                                      score=round(score, 3), timestamp=time.time())
        elif score >= self._opposite_close_threshold:
            logger.info("OPPOSITE EXIT [CLOSE] %s: pos=%s score=%.2f",
                        symbol, position_direction.value, score)
            self._opposite_exits += 1
            return OppositeExitSignal(symbol=symbol, reason=ExitReason.CLOSE,
                                      score=round(score, 3), timestamp=time.time())
        return None

    # ── Сценарии ─────────────────────────────────────────────────────────

    def _try_scenario4(self, symbol, vector_signal, current_price, trend, market_state):
        if trend == TrendState.FLAT or vector_signal is None:
            return None
        direction = Direction.LONG if trend == TrendState.UP else Direction.SHORT
        if vector_signal.direction != direction:
            self._log_conflict(symbol, "S4", vector_signal.direction, direction)
            return None
        if not self._averages.allows_direction(symbol, direction):
            return None
        depth_signal = self._depth.scan(symbol, direction, current_price)
        if depth_signal is None or not self._check_imbalance(symbol, direction):
            return None
        avg_conf = min(abs(self._averages.get_delta(symbol)) / 0.5, 1.0)
        score = (vector_signal.confidence * self._weights['vector'] +
                 avg_conf * self._weights['averages'] +
                 depth_signal.confidence * self._weights['depth'])
        if self.mode == AggregationMode.WEIGHTED and score < self._thresholds[ScenarioType.SCENARIO4.value]:
            return None
        return AggregatedSignal(
            symbol=symbol, direction=direction, timestamp=time.time(),
            scenario=ScenarioType.SCENARIO4, entry_price=depth_signal.entry_price,
            tp_price=depth_signal.tp_price, confidence=round(score, 3), score=round(score, 3),
            vector_confidence=vector_signal.confidence, averages_confidence=avg_conf,
            depth_confidence=depth_signal.confidence, market_state=market_state,
            notes=f"S4: trend={trend.value} spread={vector_signal.spread_pct:.2f}%",
        )

    def _try_scenario2(self, symbol, vector_signal, current_price, trend, market_state):
        if trend == TrendState.FLAT or vector_signal is None:
            return None
        direction = Direction.LONG if trend == TrendState.UP else Direction.SHORT
        if vector_signal.direction != direction:
            self._log_conflict(symbol, "S2", vector_signal.direction, direction)
            return None
        if not self._averages.allows_direction(symbol, direction):
            return None
        avg_conf = min(abs(self._averages.get_delta(symbol)) / 0.5, 1.0)
        score = (vector_signal.confidence * self._weights['vector'] +
                 avg_conf * self._weights['averages'])
        if self.mode == AggregationMode.WEIGHTED and score < self._thresholds[ScenarioType.SCENARIO2.value]:
            return None
        entry = current_price or vector_signal.upper_border
        spread = vector_signal.upper_border - vector_signal.lower_border
        tp = entry + spread * 2 if direction == Direction.LONG else entry - spread * 2
        return AggregatedSignal(
            symbol=symbol, direction=direction, timestamp=time.time(),
            scenario=ScenarioType.SCENARIO2, entry_price=entry, tp_price=tp,
            confidence=round(score, 3), score=round(score, 3),
            vector_confidence=vector_signal.confidence, averages_confidence=avg_conf,
            depth_confidence=0.0, market_state=market_state,
            notes=f"S2: trend={trend.value} impulse={vector_signal.spread_pct:.2f}%",
        )

    def _try_scenario1(self, symbol, vector_signal, current_price, market_state):
        if vector_signal is None:
            return None
        direction = vector_signal.direction
        depth_signal = self._depth.scan(symbol, direction, current_price)
        if depth_signal is None or not self._check_imbalance(symbol, direction):
            return None
        score = (vector_signal.confidence * self._weights['vector'] +
                 depth_signal.confidence * self._weights['depth'])
        if self.mode == AggregationMode.WEIGHTED and score < self._thresholds[ScenarioType.SCENARIO1.value]:
            return None
        return AggregatedSignal(
            symbol=symbol, direction=direction, timestamp=time.time(),
            scenario=ScenarioType.SCENARIO1, entry_price=depth_signal.entry_price,
            tp_price=depth_signal.tp_price, confidence=round(score, 3), score=round(score, 3),
            vector_confidence=vector_signal.confidence, averages_confidence=0.0,
            depth_confidence=depth_signal.confidence, market_state=market_state,
            notes=f"S1: impulse={vector_signal.spread_pct:.2f}% vol={depth_signal.volume_at_level:,.0f}",
        )

    def _try_scenario3(self, symbol, current_price, trend, delta_pct, market_state):
        if self._averages.is_oversold(symbol):
            direction = Direction.LONG
        elif self._averages.is_overbought(symbol):
            direction = Direction.SHORT
        else:
            return None
        depth_signal = self._depth.scan(symbol, direction, current_price)
        if depth_signal is None or depth_signal.distance_pct > 0.5:
            return None
        avg_conf = min(abs(delta_pct) / abs(self._averages.oversold_delta), 1.0)
        score = (avg_conf * self._weights['averages'] +
                 depth_signal.confidence * self._weights['depth'])
        if self.mode == AggregationMode.WEIGHTED and score < self._thresholds[ScenarioType.SCENARIO3.value]:
            return None
        tp = depth_signal.entry_price * (1.003 if direction == Direction.LONG else 0.997)
        return AggregatedSignal(
            symbol=symbol, direction=direction, timestamp=time.time(),
            scenario=ScenarioType.SCENARIO3, entry_price=depth_signal.entry_price, tp_price=tp,
            confidence=round(score, 3), score=round(score, 3),
            vector_confidence=0.0, averages_confidence=avg_conf,
            depth_confidence=depth_signal.confidence, market_state=market_state,
            notes=f"S3: delta={delta_pct:.3f}% wall_dist={depth_signal.distance_pct:.3f}%",
        )

    # ── Вспомогательные ──────────────────────────────────────────────────

    def _check_imbalance(self, symbol, direction):
        if not self.use_imbalance_filter:
            return True
        imbalance = self._depth.get_orderbook_imbalance(symbol)
        return imbalance >= self.imbalance_threshold if direction == Direction.LONG \
            else imbalance <= (1 - self.imbalance_threshold)

    def _is_in_cooldown(self, symbol: str) -> bool:
        last = self._last_signal_time.get(symbol, 0)
        reason = self._last_exit_reason.get(symbol)
        cooldown = (self._cooldown_after_tp if reason == 'tp' else
                    self._cooldown_after_sl if reason == 'sl' else
                    self._cooldown_default)
        elapsed = time.time() - last
        if elapsed < cooldown:
            logger.debug("Cooldown %s: %.0fs/%.0fs (reason=%s)",
                         symbol, elapsed, cooldown, reason or 'default')
            return True
        return False

    def _log_conflict(self, symbol, scenario, got, expected):
        self._conflicts_logged += 1
        logger.debug("Conflict %s %s: Vector=%s vs Trend=%s",
                     scenario, symbol, got.value, expected.value)

    def _emit_signal(self, symbol, signal):
        self._total_signals += 1
        self._signals_by_scenario[signal.scenario.value] += 1
        self._last_signal_time[symbol] = time.time()
        logger.info("SIGNAL [%s] %s %s entry=%.6f tp=%.6f score=%.2f",
                    signal.scenario.value, symbol, signal.direction.value,
                    signal.entry_price, signal.tp_price, signal.score)
        for cb in list(self._signal_callbacks):
            try:
                cb(signal)
            except Exception as e:
                logger.error("Aggregator callback error: %s", e)

    def _emit_exit(self, symbol, exit_signal):
        for cb in list(self._exit_callbacks):
            try:
                cb(exit_signal)
            except Exception as e:
                logger.error("Aggregator exit callback error: %s", e)

    def get_stats(self) -> dict:
        return {
            'mode': self.mode.value,
            'total_evaluated': self._total_evaluated,
            'total_signals': self._total_signals,
            'conflicts_logged': self._conflicts_logged,
            'opposite_exits': self._opposite_exits,
            'by_scenario': self._signals_by_scenario.copy(),
            'weights': self._weights.copy(),
            'thresholds': self._thresholds.copy(),
        }
