# tests/test_orderbook.py
import pytest
from core.orderbook import LocalOrderBook, OrderBookManager
from models.signals import OrderBookLevel, OrderBookUpdate


class TestLocalOrderBook:
    def setup_method(self):
        self.book = LocalOrderBook("BTCUSDT", max_depth=100)
    
    def test_empty_book(self):
        assert not self.book.is_initialized
        assert self.book.best_bid is None
        assert self.book.best_ask is None
        assert self.book.spread is None
    
    def test_snapshot(self):
        update = OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[
                OrderBookLevel(60000, 1.0),
                OrderBookLevel(59999, 2.0),
                OrderBookLevel(59998, 3.0),
            ],
            asks=[
                OrderBookLevel(60001, 1.5),
                OrderBookLevel(60002, 2.5),
            ],
            timestamp=1234567.0,
            update_id=1,
            is_snapshot=True,
        )
        
        self.book.apply_snapshot(update)
        
        assert self.book.is_initialized
        assert self.book.best_bid == 60000
        assert self.book.best_ask == 60001
        assert self.book.spread == 1.0
        assert self.book.mid_price == 60000.5
    
    def test_delta_update(self):
        # Сначала snapshot
        self.book.apply_snapshot(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[OrderBookLevel(60000, 1.0)],
            asks=[OrderBookLevel(60001, 1.0)],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))
        
        # Потом delta
        self.book.apply_delta(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[
                OrderBookLevel(60000, 2.0),   # Обновлённый
                OrderBookLevel(59999, 1.5),    # Новый
            ],
            asks=[
                OrderBookLevel(60001, 0),     # Удалён
                OrderBookLevel(60002, 3.0),   # Новый
            ],
            timestamp=2.0,
            update_id=2,
            is_snapshot=False,
        ))
        
        assert self.book.best_bid == 60000
        assert self.book.best_ask == 60002
        
        bids = self.book.get_bids(10)
        assert len(bids) == 2
        assert bids[0].qty == 2.0
    
    def test_find_volume_level(self):
        self.book.apply_snapshot(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[
                OrderBookLevel(60000, 1.0),    # 60000 USDT
                OrderBookLevel(59500, 5.0),    # 297500 USDT
                OrderBookLevel(59000, 10.0),   # 590000 USDT
            ],
            asks=[],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))
        
        # Ищем уровень с объёмом >= 500000
        result = self.book.find_volume_level(
            side='bid',
            target_volume_usdt=500000,
            min_distance_pct=0.0,
            max_distance_pct=5.0,
        )
        
        assert result is not None
        price, volume = result
        assert volume >= 500000
    
    def test_spread_pct(self):
        self.book.apply_snapshot(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[OrderBookLevel(60000, 1.0)],
            asks=[OrderBookLevel(60060, 1.0)],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))
        
        assert self.book.spread_pct == pytest.approx(0.1, rel=0.01)


class TestOrderBookManager:
    def test_process_message(self):
        manager = OrderBookManager(max_depth=100)
        
        message = {
            'topic': 'orderbook.50.BTCUSDT',
            'type': 'snapshot',
            'ts': 1234567890123,
            'data': {
                's': 'BTCUSDT',
                'b': [['60000', '1.0'], ['59999', '2.0']],
                'a': [['60001', '1.5']],
                'u': 1,
            },
        }
        
        manager.process_message(message)
        
        book = manager.get_book('BTCUSDT')
        assert book.is_initialized
        assert book.best_bid == 60000
