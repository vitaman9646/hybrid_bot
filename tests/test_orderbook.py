# tests/test_orderbook.py
import pytest
import time

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
        assert self.book.mid_price is None
        assert self.book.spread_pct is None

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

    def test_delta_add_and_remove(self):
        self.book.apply_snapshot(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[OrderBookLevel(60000, 1.0)],
            asks=[OrderBookLevel(60001, 1.0)],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))

        self.book.apply_delta(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[
                OrderBookLevel(60000, 2.0),
                OrderBookLevel(59999, 1.5),
            ],
            asks=[
                OrderBookLevel(60001, 0),
                OrderBookLevel(60002, 3.0),
            ],
            timestamp=2.0,
            update_id=2,
            is_snapshot=False,
        ))

        assert self.book.best_bid == 60000
        assert self.book.best_ask == 60002

        bids = self.book.get_bids(10)
        assert len(bids) == 2
        assert bids[0].price == 60000
        assert bids[0].qty == 2.0
        assert bids[1].price == 59999

    def test_delta_before_snapshot_ignored(self):
        self.book.apply_delta(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[OrderBookLevel(60000, 1.0)],
            asks=[],
            timestamp=1.0,
            update_id=1,
            is_snapshot=False,
        ))

        assert not self.book.is_initialized

    def test_get_bids_asks_depth(self):
        self.book.apply_snapshot(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[
                OrderBookLevel(60000, 1.0),
                OrderBookLevel(59999, 2.0),
                OrderBookLevel(59998, 3.0),
                OrderBookLevel(59997, 4.0),
                OrderBookLevel(59996, 5.0),
            ],
            asks=[
                OrderBookLevel(60001, 1.0),
                OrderBookLevel(60002, 2.0),
            ],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))

        bids = self.book.get_bids(3)
        assert len(bids) == 3
        assert bids[0].price == 60000
        assert bids[2].price == 59998

        asks = self.book.get_asks(2)
        assert len(asks) == 2
        assert asks[0].price == 60001

    def test_find_volume_level(self):
        self.book.apply_snapshot(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[
                OrderBookLevel(60000, 1.0),
                OrderBookLevel(59500, 5.0),
                OrderBookLevel(59000, 10.0),
            ],
            asks=[],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))

        result = self.book.find_volume_level(
            side='bid',
            target_volume_usdt=500000,
            min_distance_pct=0.0,
            max_distance_pct=5.0,
        )

        assert result is not None
        price, volume = result
        assert volume >= 500000

    def test_find_volume_level_not_found(self):
        self.book.apply_snapshot(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[OrderBookLevel(60000, 0.001)],
            asks=[],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))

        result = self.book.find_volume_level(
            side='bid',
            target_volume_usdt=999999999,
        )
        assert result is None

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

    def test_trim_depth(self):
        book = LocalOrderBook("TEST", max_depth=3)
        book.apply_snapshot(OrderBookUpdate(
            symbol="TEST",
            bids=[
                OrderBookLevel(100, 1),
                OrderBookLevel(99, 1),
                OrderBookLevel(98, 1),
                OrderBookLevel(97, 1),
                OrderBookLevel(96, 1),
            ],
            asks=[],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))

        bids = book.get_bids(10)
        assert len(bids) == 3
        assert bids[0].price == 100
        assert bids[2].price == 98

    def test_volume_at_distance(self):
        self.book.apply_snapshot(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[
                OrderBookLevel(60000, 1.0),
                OrderBookLevel(59700, 2.0),
                OrderBookLevel(59400, 3.0),
            ],
            asks=[],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))

        vol = self.book.get_volume_at_distance('bid', 0.5)
        assert vol >= 60000

    def test_stats(self):
        self.book.apply_snapshot(OrderBookUpdate(
            symbol="BTCUSDT",
            bids=[OrderBookLevel(60000, 1.0)],
            asks=[OrderBookLevel(60001, 1.0)],
            timestamp=1.0,
            update_id=1,
            is_snapshot=True,
        ))

        stats = self.book.get_stats()
        assert stats['symbol'] == 'BTCUSDT'
        assert stats['initialized'] is True
        assert stats['best_bid'] == 60000
        assert stats['best_ask'] == 60001
        assert stats['snapshots'] == 1


class TestOrderBookManager:
    def test_process_snapshot(self, sample_orderbook_snapshot):
        manager = OrderBookManager(max_depth=100)
        manager.process_message(sample_orderbook_snapshot)

        book = manager.get_book('BTCUSDT')
        assert book.is_initialized
        assert book.best_bid == 60000.0
        assert book.best_ask == 60000.5

    def test_process_delta_after_snapshot(
        self, sample_orderbook_snapshot, sample_orderbook_delta
    ):
        manager = OrderBookManager(max_depth=100)

        manager.process_message(sample_orderbook_snapshot)
        manager.process_message(sample_orderbook_delta)

        book = manager.get_book('BTCUSDT')
        assert book.best_bid == 60000.0
        # 60000.50 удалён, следующий ask = 60001.00
        assert book.best_ask == 60001.0

    def test_multiple_symbols(self):
        manager = OrderBookManager(max_depth=100)

        msg1 = {
            'topic': 'orderbook.50.BTCUSDT',
            'type': 'snapshot',
            'ts': int(time.time() * 1000),
            'data': {
                's': 'BTCUSDT',
                'b': [['60000', '1']],
                'a': [['60001', '1']],
                'u': 1,
            },
        }
        msg2 = {
            'topic': 'orderbook.50.ETHUSDT',
            'type': 'snapshot',
            'ts': int(time.time() * 1000),
            'data': {
                's': 'ETHUSDT',
                'b': [['3000', '5']],
                'a': [['3001', '5']],
                'u': 1,
            },
        }

        manager.process_message(msg1)
        manager.process_message(msg2)

        stats = manager.get_all_stats()
        assert 'BTCUSDT' in stats
        assert 'ETHUSDT' in stats
        assert stats['BTCUSDT']['best_bid'] == 60000
        assert stats['ETHUSDT']['best_bid'] == 3000

    def test_ignore_non_orderbook(self):
        manager = OrderBookManager()
        manager.process_message({
            'topic': 'publicTrade.BTCUSDT',
            'data': {},
        })
        assert len(manager.get_all_stats()) == 0
