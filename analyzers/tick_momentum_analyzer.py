"""
TickMomentumAnalyzer — детектор ценового импульса на тиках.
Сигнал: price_change за window секунд > thresh И continuation_3s > 0.02%
"""
from __future__ import annotations
import time
import logging
from collections import deque
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Per-symbol параметры (из бэктеста OOS +43.20)
TICK_MOM_PARAMS = {
    'BTCUSDT':  {'window': 20, 'thresh': 0.0015, 'sl': 0.012, 'tp': 1.2},
    'ETHUSDT':  {'window': 20, 'thresh': 0.002,  'sl': 0.010, 'tp': 1.2},
    'SOLUSDT':  {'window': 30, 'thresh': 0.0025, 'sl': 0.015, 'tp': 1.5},
    'XRPUSDT':  {'window': 20, 'thresh': 0.002,  'sl': 0.012, 'tp': 1.0},
    'DOGEUSDT': {'window': 30, 'thresh': 0.002,  'sl': 0.015, 'tp': 1.0},
    'AVAXUSDT': {'window': 10, 'thresh': 0.002,  'sl': 0.015, 'tp': 1.2},
    'BNBUSDT':  {'window': 20, 'thresh': 0.002,  'sl': 0.012, 'tp': 1.2},
}

CONTINUATION_THRESH = 0.0002  # 0.02%
COOLDOWN_SEC = 120

@dataclass
class TickMomentumSignal:
    symbol: str
    direction: str        # 'long' or 'short'
    timestamp: float
    move_pct: float       # размер импульса
    entry_price: float
    sl_pct: float
    tp_mult: float
    confidence: float
    size_mult: float = 1.0


class TickMomentumAnalyzer:
    """
    Анализирует поток тиков и генерирует сигнал при резком движении цены.
    Вызывается из engine.py при каждом trade event.
    """

    def __init__(self):
        self._price_windows: dict[str, deque] = {}   # symbol → deque[(ts, price)]
        self._cont_windows: dict[str, deque] = {}    # symbol → deque[(ts, price)] 3s
        self._last_signal: dict[str, float] = {}     # symbol → ts последнего сигнала

    def on_trade(self, symbol: str, price: float, qty: float, side: str) -> Optional[TickMomentumSignal]:
        """Вызывается при каждом тике. Возвращает сигнал или None."""
        params = TICK_MOM_PARAMS.get(symbol)
        if params is None:
            return None

        now = time.time()
        window = params['window']

        # Инициализация
        if symbol not in self._price_windows:
            self._price_windows[symbol] = deque()
            self._cont_windows[symbol] = deque()
            self._last_signal[symbol] = 0.0

        pw = self._price_windows[symbol]
        cw = self._cont_windows[symbol]

        # Добавляем тик
        pw.append((now, price))
        cw.append((now, price))

        # Чистим старые
        while pw and now - pw[0][0] > window:
            pw.popleft()
        while cw and now - cw[0][0] > 3:
            cw.popleft()

        # Cooldown
        if now - self._last_signal.get(symbol, 0) < COOLDOWN_SEC:
            return None

        if len(pw) < 2 or len(cw) < 2:
            return None

        # Основной импульс
        p_start = pw[0][1]
        p_end = pw[-1][1]
        move = (p_end - p_start) / p_start

        if abs(move) < params['thresh']:
            return None

        # Continuation filter — последние 3 секунды
        c_start = cw[0][1]
        c_end = cw[-1][1]
        move_3s = (c_end - c_start) / c_start

        if move > 0 and move_3s < CONTINUATION_THRESH:
            return None
        if move < 0 and move_3s > -CONTINUATION_THRESH:
            return None

        direction = 'long' if move > 0 else 'short'
        # Dynamic sizing: strength = move/thresh, capped at 2x
        strength = min(abs(move) / params['thresh'], 2.0)
        confidence = strength / 2.0  # 0.5–1.0 для логов

        self._last_signal[symbol] = now

        logger.info(
            "TickMomentum signal: %s %s move=%.3f%% cont=%.3f%% conf=%.2f",
            symbol, direction, move*100, move_3s*100, confidence
        )

        return TickMomentumSignal(
            symbol=symbol,
            direction=direction,
            timestamp=now,
            move_pct=abs(move),
            entry_price=price,
            sl_pct=params['sl'],
            tp_mult=params['tp'],
            confidence=confidence,
            size_mult=min(strength, 2.0),
        )
