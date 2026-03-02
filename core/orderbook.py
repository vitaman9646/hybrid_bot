# core/orderbook.py
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional
from collections import OrderedDict

from models.signals import OrderBookLevel, OrderBookUpdate

logger = logging.getLogger(__name__)


class LocalOrderBook:
    """
    Локальная копия стакана с инкрементальными обновлениями.
    Поддерживает snapshot + delta от Bybit.
    """
    
    def __init__(self, symbol: str, max_depth: int = 200):
        self.symbol = symbol
        self.max_depth = max_depth
        
        # Сортированные словари: price → qty
        # bids: высокая цена → низкая (descending)
        # asks: низкая цена → высокая (ascending)
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        
        self._last_update_id: int = 0
        self._last_update_time: float = 0.0
        self._initialized: bool = False
        
        # Статистика
        self._update_count: int = 0
        self._snapshot_count: int = 0
    
    @property
    def is_initialized(self) -> bool:
        return self._initialized
    
    @property
    def best_bid(self) -> Optional[float]:
        if not self._bids:
            return None
        return max(self._bids.keys())
    
    @property
    def best_ask(self) -> Optional[float]:
        if not self._asks:
            return None
        return min(self._asks.keys())
    
    @property
    def spread(self) -> Optional[float]:
        bid = self.best_bid
        ask = self.best_ask
        if bid is None or ask is None:
            return None
        return ask - bid
    
    @property
    def spread_pct(self) -> Optional[float]:
        bid = self.best_bid
        spread = self.spread
        if bid is None or spread is None or bid == 0:
            return None
        return (spread / bid) * 100
    
    @property
    def mid_price(self) -> Optional[float]:
        bid = self.best_bid
        ask = self.best_ask
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2
    
    def apply_snapshot(self, update: OrderBookUpdate):
        """Применить snapshot (полная замена)"""
        self._bids.clear()
        self._asks.clear()
        
        for level in update.bids:
            if level.qty > 0:
                self._bids[level.price] = level.qty
        
        for level in update.asks:
            if level.qty > 0:
                self._asks[level.price] = level.qty
        
        self._last_update_id = update.update_id
        self._last_update_time = update.timestamp
        self._initialized = True
        self._snapshot_count += 1
        
        self._trim()
        
        logger.debug(
            f"OrderBook snapshot {self.symbol}: "
            f"{len(self._bids)} bids, {len(self._asks)} asks"
        )
    
    def apply_delta(self, update: OrderBookUpdate):
        """Применить delta (инкрементальное обновление)"""
        if not self._initialized:
            logger.warning(
                f"Delta before snapshot for {self.symbol}, ignoring"
            )
            return
        
        for level in update.bids:
            if level.qty == 0:
                self._bids.pop(level.price, None)
            else:
                self._bids[level.price] = level.qty
        
        for level in update.asks:
            if level.qty == 0:
                self._asks.pop(level.price, None)
            else:
                self._asks[level.price] = level.qty
        
        self._last_update_id = update.update_id
        self._last_update_time = update.timestamp
        self._update_count += 1
        
        self._trim()
    
    def _trim(self):
        """Ограничить глубину стакана"""
        if len(self._bids) > self.max_depth:
            sorted_bids = sorted(
                self._bids.keys(), reverse=True
            )
            for price in sorted_bids[self.max_depth:]:
                del self._bids[price]
        
        if len(self._asks) > self.max_depth:
            sorted_asks = sorted(self._asks.keys())
            for price in sorted_asks[self.max_depth:]:
                del self._asks[price]
    
    def get_bids(
        self, depth: int = 20
    ) -> list[OrderBookLevel]:
        """Получить N лучших bid уровней"""
        sorted_prices = sorted(
            self._bids.keys(), reverse=True
        )[:depth]
        return [
            OrderBookLevel(price=p, qty=self._bids[p])
            for p in sorted_prices
        ]
    
    def get_asks(
        self, depth: int = 20
    ) -> list[OrderBookLevel]:
        """Получить N лучших ask уровней"""
        sorted_prices = sorted(
            self._asks.keys()
        )[:depth]
        return [
            OrderBookLevel(price=p, qty=self._asks[p])
            for p in sorted_prices
        ]
    
    def get_volume_at_distance(
        self,
        side: str,
        distance_pct: float,
    ) -> float:
        """
        Суммарный объём в стакане до определённого расстояния
        от best bid/ask. Для Depth Shot.
        """
        if side == 'bid':
            base = self.best_bid
            if base is None:
                return 0.0
            threshold = base * (1 - distance_pct / 100)
            return sum(
                price * qty
                for price, qty in self._bids.items()
                if price >= threshold
            )
        else:
            base = self.best_ask
            if base is None:
                return 0.0
            threshold = base * (1 + distance_pct / 100)
            return sum(
                price * qty
                for price, qty in self._asks.items()
                if price <= threshold
            )
    
    def find_volume_level(
        self,
        side: str,
        target_volume_usdt: float,
        min_distance_pct: float = 0.1,
        max_distance_pct: float = 5.0,
    ) -> Optional[tuple[float, float]]:
        """
        Найти уровень где суммарный объём >= target.
        Возвращает (price, accumulated_volume) или None.
        Для Depth Shot алгоритма.
        """
        base = self.best_bid if side == 'bid' else self.best_ask
        if base is None or base == 0:
            return None
        
        if side == 'bid':
            sorted_prices = sorted(
                self._bids.keys(), reverse=True
            )
        else:
            sorted_prices = sorted(self._asks.keys())
        
        accumulated = 0.0
        
        for price in sorted_prices:
            distance_pct_actual = (
                abs(price - base) / base * 100
            )
            
            if distance_pct_actual < min_distance_pct:
                accumulated += price * self._bids.get(
                    price, self._asks.get(price, 0)
                )
                continue
            
            if distance_pct_actual > max_distance_pct:
                break
            
            qty = self._bids.get(
                price, self._asks.get(price, 0)
            )
            accumulated += price * qty
            
            if accumulated >= target_volume_usdt:
                return (price, accumulated)
        
        return None
    
    def get_stats(self) -> dict:
        return {
            'symbol': self.symbol,
            'initialized': self._initialized,
            'best_bid': self.best_bid,
            'best_ask': self.best_ask,
            'spread': self.spread,
            'spread_pct': (
                round(self.spread_pct, 6)
                if self.spread_pct else None
            ),
            'mid_price': self.mid_price,
            'bid_levels': len(self._bids),
            'ask_levels': len(self._asks),
            'updates': self._update_count,
            'snapshots': self._snapshot_count,
            'last_update': self._last_update_time,
        }


class OrderBookManager:
    """Управляет стаканами для нескольких символов"""
    
    def __init__(self, max_depth: int = 200):
        self.max_depth = max_depth
        self._books: dict[str, LocalOrderBook] = {}
    
    def get_book(self, symbol: str) -> LocalOrderBook:
        if symbol not in self._books:
            self._books[symbol] = LocalOrderBook(
                symbol, self.max_depth
            )
        return self._books[symbol]
    
    def process_message(self, message: dict):
        """
        Обработать raw message от Bybit WebSocket.
        Формат: {
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot" | "delta",
            "data": {"s": "BTCUSDT", "b": [...], "a": [...], "u": 123}
        }
        """
        topic = message.get('topic', '')
        msg_type = message.get('type', '')
        data = message.get('data', {})
        
        if 'orderbook' not in topic:
            return
        
        symbol = data.get('s', '')
        if not symbol:
            return
        
        timestamp = message.get('ts', time.time() * 1000) / 1000
        update_id = data.get('u', 0)
        
        bids = [
            OrderBookLevel(
                price=float(b[0]), qty=float(b[1])
            )
            for b in data.get('b', [])
        ]
        asks = [
            OrderBookLevel(
                price=float(a[0]), qty=float(a[1])
            )
            for a in data.get('a', [])
        ]
        
        update = OrderBookUpdate(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=timestamp,
            update_id=update_id,
            is_snapshot=(msg_type == 'snapshot'),
        )
        
        book = self.get_book(symbol)
        
        if msg_type == 'snapshot':
            book.apply_snapshot(update)
        elif msg_type == 'delta':
            book.apply_delta(update)
    
    def get_all_stats(self) -> dict[str, dict]:
        return {
            symbol: book.get_stats()
            for symbol, book in self._books.items()
              }
