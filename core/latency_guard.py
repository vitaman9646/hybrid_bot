# core/latency_guard.py
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field

from models.signals import LatencyLevel, OrderRTT

logger = logging.getLogger(__name__)


@dataclass
class LatencySnapshot:
    timestamp: float
    ws_latency_ms: float
    order_rtt_ms: float = 0.0
    level: LatencyLevel = LatencyLevel.NORMAL


class LatencyGuard:
    """
    Трёхуровневая защита от задержек:
    - NORMAL  (<300ms):  всё работает
    - WARNING (300-500ms): приостановить новые входы
    - CRITICAL(500-1000ms): отменить pending, закрыть при drawdown
    - EMERGENCY(>1000ms): emergency stop всего бота
    """
    
    def __init__(self, config: dict):
        self.warn_threshold = config.get('warn_threshold_ms', 300)
        self.critical_threshold = config.get(
            'critical_threshold_ms', 500
        )
        self.emergency_threshold = config.get(
            'emergency_threshold_ms', 1000
        )
        self.check_interval = config.get('check_interval', 5)
        self.track_order_rtt = config.get('track_order_rtt', True)
        self.rtt_window_size = config.get('rtt_window_size', 100)
        
        # Текущее состояние
        self._current_level = LatencyLevel.NORMAL
        self._current_ws_latency_ms: float = 0.0
        self._last_pong_time: float = 0.0
        self._ping_sent_time: float = 0.0
        
        # История
        self._history: deque[LatencySnapshot] = deque(maxlen=1000)
        self._order_rtts: deque[OrderRTT] = deque(
            maxlen=self.rtt_window_size
        )
        
        # Callbacks
        self._on_level_change_callbacks: list = []
        
        # Emergency flag
        self._emergency_triggered = False
    
    @property
    def current_level(self) -> LatencyLevel:
        return self._current_level
    
    @property
    def current_latency_ms(self) -> float:
        return self._current_ws_latency_ms
    
    @property
    def is_trading_allowed(self) -> bool:
        """Разрешена ли торговля"""
        return self._current_level == LatencyLevel.NORMAL
    
    @property
    def is_new_entries_allowed(self) -> bool:
        """Разрешены ли новые входы"""
        return self._current_level in (
            LatencyLevel.NORMAL,
        )
    
    @property
    def should_cancel_pending(self) -> bool:
        """Нужно ли отменить pending ордера"""
        return self._current_level in (
            LatencyLevel.CRITICAL,
            LatencyLevel.EMERGENCY,
        )
    
    @property
    def should_emergency_stop(self) -> bool:
        """Нужен ли аварийный стоп"""
        return self._current_level == LatencyLevel.EMERGENCY
    
    @property
    def avg_order_rtt_ms(self) -> float:
        """Средний RTT ордеров"""
        if not self._order_rtts:
            return 0.0
        valid = [r.rtt_ms for r in self._order_rtts if r.rtt_ms > 0]
        if not valid:
            return 0.0
        return sum(valid) / len(valid)
    
    @property
    def p95_order_rtt_ms(self) -> float:
        """95-й перцентиль RTT"""
        if not self._order_rtts:
            return 0.0
        valid = sorted(
            [r.rtt_ms for r in self._order_rtts if r.rtt_ms > 0]
        )
        if not valid:
            return 0.0
        idx = int(len(valid) * 0.95)
        return valid[min(idx, len(valid) - 1)]
    
    def on_level_change(self, callback):
        """Регистрация callback при изменении уровня"""
        self._on_level_change_callbacks.append(callback)
    
    def record_ping_sent(self):
        """Записать время отправки ping"""
        self._ping_sent_time = time.time()
    
    def record_pong_received(self):
        """Записать получение pong и рассчитать latency"""
        now = time.time()
        self._last_pong_time = now
        
        if self._ping_sent_time > 0:
            self._current_ws_latency_ms = (
                (now - self._ping_sent_time) * 1000
            )
        
        # Определяем уровень
        old_level = self._current_level
        self._current_level = self._classify_latency(
            self._current_ws_latency_ms
        )
        
        # Сохраняем snapshot
        snapshot = LatencySnapshot(
            timestamp=now,
            ws_latency_ms=self._current_ws_latency_ms,
            order_rtt_ms=self.avg_order_rtt_ms,
            level=self._current_level,
        )
        self._history.append(snapshot)
        
        # Уведомляем если уровень изменился
        if old_level != self._current_level:
            self._notify_level_change(old_level, self._current_level)
    
    def record_order_rtt(self, rtt: OrderRTT):
        """Записать RTT ордера"""
        self._order_rtts.append(rtt)
        
        if rtt.rtt_ms > self.critical_threshold:
            logger.warning(
                f"Slow order RTT: {rtt.rtt_ms:.0f}ms "
                f"for {rtt.symbol} ({rtt.order_id})"
            )
    
    def check_no_pong_timeout(self, timeout_seconds: float = 30):
        """
        Проверяем не пропал ли pong слишком давно.
        Вызывается периодически.
        """
        if self._last_pong_time == 0:
            return  # Ещё не было ни одного pong
        
        elapsed = time.time() - self._last_pong_time
        if elapsed > timeout_seconds:
            logger.critical(
                f"No pong received for {elapsed:.1f}s!"
            )
            old_level = self._current_level
            self._current_level = LatencyLevel.EMERGENCY
            self._current_ws_latency_ms = elapsed * 1000
            
            if old_level != self._current_level:
                self._notify_level_change(
                    old_level, LatencyLevel.EMERGENCY
                )
    
    def _classify_latency(self, latency_ms: float) -> LatencyLevel:
        """Классификация задержки по уровням"""
        if latency_ms >= self.emergency_threshold:
            return LatencyLevel.EMERGENCY
        elif latency_ms >= self.critical_threshold:
            return LatencyLevel.CRITICAL
        elif latency_ms >= self.warn_threshold:
            return LatencyLevel.WARNING
        else:
            return LatencyLevel.NORMAL
    
    def _notify_level_change(
        self, old: LatencyLevel, new: LatencyLevel
    ):
        """Уведомить о смене уровня"""
        level_actions = {
            LatencyLevel.NORMAL: "Trading resumed",
            LatencyLevel.WARNING: (
                "New entries PAUSED (latency warning)"
            ),
            LatencyLevel.CRITICAL: (
                "CANCEL pending orders, check positions"
            ),
            LatencyLevel.EMERGENCY: (
                "EMERGENCY STOP — all trading halted"
            ),
        }
        
        msg = (
            f"Latency level changed: {old.value} → {new.value} "
            f"({self._current_ws_latency_ms:.0f}ms) "
            f"Action: {level_actions.get(new, 'unknown')}"
        )
        
        if new in (LatencyLevel.CRITICAL, LatencyLevel.EMERGENCY):
            logger.critical(msg)
        elif new == LatencyLevel.WARNING:
            logger.warning(msg)
        else:
            logger.info(msg)
        
        for callback in self._on_level_change_callbacks:
            try:
                callback(old, new, self._current_ws_latency_ms)
            except Exception as e:
                logger.error(f"Level change callback error: {e}")
    
    def get_stats(self) -> dict:
        """Статистика для мониторинга"""
        return {
            'current_level': self._current_level.value,
            'ws_latency_ms': round(self._current_ws_latency_ms, 1),
            'avg_order_rtt_ms': round(self.avg_order_rtt_ms, 1),
            'p95_order_rtt_ms': round(self.p95_order_rtt_ms, 1),
            'total_orders_tracked': len(self._order_rtts),
            'history_size': len(self._history),
            'emergency_triggered': self._emergency_triggered,
      }
