"""
CircuitBreaker — автоматическая защита от серии убытков.

Уровни:
  CLOSED  — торговля разрешена
  SOFT    — пауза новых входов, алерт
  HARD    — стоп входов + Telegram уведомление
  PANIC   — закрыть все позиции рыночными ордерами
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

logger = logging.getLogger(__name__)


class CBState(Enum):
    CLOSED = "closed"   # норма
    SOFT   = "soft"     # пауза новых входов
    HARD   = "hard"     # стоп + алерт
    PANIC  = "panic"    # закрыть всё


@dataclass
class CircuitBreaker:
    # --- конфигурация ---
    max_consecutive_losses: int   = 3
    max_losses_per_hour: int      = 5
    max_drawdown_pct: float       = 5.0    # от пикового баланса
    max_avg_slippage_pct: float   = 0.1   # средний slippage за час
    soft_cooldown_sec: int        = 900    # 15 мин
    hard_cooldown_sec: int        = 1800   # 30 мин

    # --- состояние ---
    _state: CBState               = field(default=CBState.CLOSED, init=False)
    _trip_time: float             = field(default=0.0, init=False)
    _trip_reason: str             = field(default="", init=False)
    _consecutive_losses: int      = field(default=0, init=False)
    _peak_balance: float          = field(default=0.0, init=False)
    _recent_trades: deque         = field(default_factory=lambda: deque(maxlen=200), init=False)

    # --- публичный API ---

    def on_trade_closed(self, pnl: float, slippage_pct: float, balance: float) -> None:
        """Вызывать после каждого закрытия позиции."""
        self._peak_balance = max(self._peak_balance, balance)
        self._recent_trades.append({
            'time': time.time(),
            'pnl': pnl,
            'slippage': abs(slippage_pct),
        })

        if pnl < 0:
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0

        self._evaluate(balance)

    def check(self) -> tuple[bool, CBState, str]:
        """
        Возвращает (can_trade, state, reason).
        Вызывать перед каждым новым ордером.
        """
        if self._state == CBState.CLOSED:
            return True, CBState.CLOSED, ""

        # Проверяем истечение cooldown
        elapsed = time.time() - self._trip_time
        cooldown = (
            self.hard_cooldown_sec
            if self._state == CBState.HARD
            else self.soft_cooldown_sec
        )
        if elapsed >= cooldown and self._state != CBState.PANIC:
            logger.info("CircuitBreaker cooldown expired, resetting to CLOSED")
            self._reset()
            return True, CBState.CLOSED, ""

        remaining = int(cooldown - elapsed)
        reason = f"CB {self._state.value}: {self._trip_reason} | resume in {remaining}s"

        can_trade = False  # SOFT/HARD/PANIC — все блокируют новые входы
        return can_trade, self._state, reason

    def force_panic(self, reason: str) -> None:
        """Внешний вызов для перехода в PANIC (например из FilterPipeline)."""
        self._trip(CBState.PANIC, reason)

    @property
    def state(self) -> CBState:
        return self._state

    @property
    def trip_reason(self) -> str:
        return self._trip_reason

    def status_str(self) -> str:
        if self._state == CBState.CLOSED:
            return "CB: OK"
        elapsed = int(time.time() - self._trip_time)
        return f"CB: {self._state.value.upper()} | {self._trip_reason} | {elapsed}s ago"

    # --- внутренние методы ---

    def _evaluate(self, balance: float) -> None:
        if self._state == CBState.PANIC:
            return  # уже в панике, не переоцениваем

        # 1. Consecutive losses → HARD
        if self._consecutive_losses >= self.max_consecutive_losses:
            self._trip(CBState.HARD, f"{self._consecutive_losses} consecutive losses")
            return

        # 2. Losses per hour → SOFT
        hour_ago = time.time() - 3600
        recent_losses = sum(1 for t in self._recent_trades if t['time'] > hour_ago and t['pnl'] < 0)
        if recent_losses >= self.max_losses_per_hour:
            self._trip(CBState.SOFT, f"{recent_losses} losses in last hour")
            return

        # 3. Drawdown → HARD
        if self._peak_balance > 0:
            dd_pct = (self._peak_balance - balance) / self._peak_balance * 100
            if dd_pct >= self.max_drawdown_pct:
                self._trip(CBState.HARD, f"drawdown {dd_pct:.1f}% >= {self.max_drawdown_pct}%")
                return

        # 4. Avg slippage → SOFT
        recent = [t for t in self._recent_trades if time.time() - t['time'] < 3600]
        if len(recent) >= 5:
            avg_slip = sum(t['slippage'] for t in recent) / len(recent)
            if avg_slip > self.max_avg_slippage_pct:
                self._trip(CBState.SOFT, f"avg slippage {avg_slip:.3f}% > {self.max_avg_slippage_pct}%")

    def _trip(self, state: CBState, reason: str) -> None:
        if self._state == state:
            return
        self._state = state
        self._trip_time = time.time()
        self._trip_reason = reason
        logger.warning("?? CircuitBreaker %s: %s", state.value.upper(), reason)

    def _reset(self) -> None:
        self._state = CBState.CLOSED
        self._consecutive_losses = 0
        self._trip_reason = ""
