# models/signals.py
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Direction(Enum):
    LONG = "long"
    SHORT = "short"


class SignalSource(Enum):
    VECTOR = "vector"
    AVERAGES = "averages"
    DEPTH_SHOT = "depth_shot"
    TELEGRAM = "telegram"
    MANUAL = "manual"


class LatencyLevel(Enum):
    NORMAL = "normal"        # < 300ms
    WARNING = "warning"      # 300-500ms
    CRITICAL = "critical"    # 500-1000ms
    EMERGENCY = "emergency"  # > 1000ms


@dataclass
class Signal:
    source: SignalSource
    direction: Direction
    symbol: str
    confidence: float
    entry_price: float
    suggested_tp: float
    suggested_sl: float
    timestamp: float
    metadata: dict = field(default_factory=dict)
    
    def __post_init__(self):
        self.confidence = max(0.0, min(1.0, self.confidence))


@dataclass
class TradeData:
    """Один трейд из WebSocket"""
    symbol: str
    price: float
    qty: float
    quote_volume: float
    side: str          # Buy / Sell
    timestamp: float   # seconds
    trade_id: str = ""
    
    def __post_init__(self):
        if self.quote_volume == 0 and self.price > 0:
            self.quote_volume = self.price * self.qty


@dataclass
class OrderBookLevel:
    price: float
    qty: float
    
    @property
    def quote_volume(self) -> float:
        return self.price * self.qty


@dataclass
class OrderBookUpdate:
    """Обновление стакана"""
    symbol: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    timestamp: float
    update_id: int = 0
    is_snapshot: bool = False


@dataclass
class OrderRTT:
    """Round-trip time для ордера"""
    order_id: str
    symbol: str
    sent_at: float
    filled_at: float = 0.0
    acknowledged_at: float = 0.0
    
    @property
    def rtt_ms(self) -> float:
        if self.acknowledged_at > 0:
            return (self.acknowledged_at - self.sent_at) * 1000
        return 0.0
    
    @property
    def fill_time_ms(self) -> float:
        if self.filled_at > 0:
            return (self.filled_at - self.sent_at) * 1000
        return 0.0
