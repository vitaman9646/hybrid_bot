import pytest
import time
from core.orderbook import OrderBookManager
from models.signals import OrderBookLevel, OrderBookUpdate, Direction
from analyzers.depth_shot_analyzer import (
    DepthShotAnalyzer, TakeProfitType
)


def make_orderbook_manager(
    symbol: str,
    bids: list,
    asks: list,
) -> OrderBookManager:
    manager = OrderBookManager(max_depth=200)
    ts = time.time()
    update_id = int(ts * 1000)
    message = {
        'topic': f'orderbook.50.{symbol}',
        'type': 'snapshot',
        'ts': int(ts * 1000),
        'data': {
            's': symbol,
            'b': [[str(p), str(q)] for p, q in bids],
            'a': [[str(p), str(q)] for p, q in asks],
            'u': update_id,
        },
    }
    manager.process_message(message)
    return manager


def make_config(**kwargs):
    base = dict(
        min_volume_usdt=100_000,
        min_distance_pct=0.3,
        max_distance_pct=3.0,
        buffer_pct=0.2,
        stop_if_outside=True,
        tp_type='classic',
        tp_pct=0.3,
        wall_tracker={'min_age_s': 0.0, 'max_drop_pct': 0.99},  # отключаем в тестах
    )
    base.update(kwargs)
    return base


class TestDepthShotAnalyzer:

    def _make_analyzer(self, symbol, bids, asks, **kwargs):
        manager = make_orderbook_manager(symbol, bids, asks)
        return DepthShotAnalyzer(make_config(**kwargs), manager), manager

    def test_init(self):
        manager = OrderBookManager()
        a = DepthShotAnalyzer(make_config(), manager)
        assert a.min_volume_usdt == 100_000
        assert a.tp_type == TakeProfitType.CLASSIC

    def test_no_signal_ob_not_initialized(self):
        manager = OrderBookManager()
        a = DepthShotAnalyzer(make_config(), manager)
        result = a.scan("BTCUSDT", Direction.LONG, 60000)
        assert result is None

    def test_signal_long_finds_bid_wall(self):
        # Стена на биде: 10 BTC на цене 59700 = 597,000 USDT
        bids = [
            (59900, 0.1),
            (59800, 0.2),
            (59700, 10.0),   # стена
            (59600, 0.1),
        ]
        asks = [(60100, 1.0), (60200, 1.0)]

        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=500_000)
        signal = a.scan("BTCUSDT", Direction.LONG, 60000)

        assert signal is not None
        assert signal.direction == Direction.LONG
        assert signal.entry_price == pytest.approx(59700, rel=0.01)
        assert signal.volume_at_level >= 500_000
        assert signal.symbol == "BTCUSDT"

    def test_signal_short_finds_ask_wall(self):
        # Стена на аске: 10 BTC на 60300 = 603,000 USDT
        bids = [(59900, 1.0), (59800, 1.0)]
        asks = [
            (60100, 0.1),
            (60200, 0.2),
            (60300, 10.0),   # стена
            (60400, 0.1),
        ]

        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=500_000)
        signal = a.scan("BTCUSDT", Direction.SHORT, 60000)

        assert signal is not None
        assert signal.direction == Direction.SHORT
        assert signal.entry_price == pytest.approx(60300, rel=0.01)

    def test_no_signal_volume_too_small(self):
        bids = [(59700, 1.0)]  # 59,700 USDT — меньше min
        asks = [(60100, 1.0)]

        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=500_000)
        signal = a.scan("BTCUSDT", Direction.LONG, 60000)
        assert signal is None

    def test_no_signal_distance_too_close(self):
        # Стена слишком близко (0.1% < min 0.5%)
        bids = [(59940, 20.0)]  # 0.1% от 60000
        asks = [(60100, 1.0)]

        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=100_000,
                                   min_distance_pct=0.5)
        signal = a.scan("BTCUSDT", Direction.LONG, 60000)
        assert signal is None

    def test_no_signal_distance_too_far(self):
        # Стена слишком далеко (5% > max 3%)
        bids = [(57000, 20.0)]  # 5% от 60000
        asks = [(60100, 1.0)]

        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=100_000,
                                   max_distance_pct=3.0)
        signal = a.scan("BTCUSDT", Direction.LONG, 60000)
        assert signal is None

    def test_tp_classic(self):
        bids = [(59700, 10.0)]
        asks = [(60100, 1.0)]
        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=500_000,
                                   tp_type='classic', tp_pct=0.5)
        signal = a.scan("BTCUSDT", Direction.LONG, 60000)
        assert signal is not None
        expected_tp = signal.entry_price * 1.005
        assert signal.tp_price == pytest.approx(expected_tp, rel=0.001)

    def test_tp_depth(self):
        # TP на следующей стене в аске
        bids = [(59700, 10.0)]
        asks = [(60200, 0.1), (60500, 10.0)]  # стена на 60500
        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=500_000,
                                   tp_type='depth')
        signal = a.scan("BTCUSDT", Direction.LONG, 60000)
        assert signal is not None
        # TP должен быть около стены на аске
        assert signal.tp_price > signal.entry_price

    def test_confidence_range(self):
        bids = [(59700, 10.0)]
        asks = [(60100, 1.0)]
        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=500_000)
        signal = a.scan("BTCUSDT", Direction.LONG, 60000)
        assert signal is not None
        assert 0.0 <= signal.confidence <= 1.0

    def test_signal_callback(self):
        bids = [(59700, 10.0)]
        asks = [(60100, 1.0)]
        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=500_000)
        received = []
        a.on_signal(received.append)
        a.scan("BTCUSDT", Direction.LONG, 60000)
        assert len(received) == 1

    def test_level_still_valid(self):
        bids = [(59700, 10.0)]
        asks = [(60100, 1.0)]
        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=500_000)
        assert a.is_level_still_valid("BTCUSDT", 59700, Direction.LONG)

    def test_level_not_valid_after_disappears(self):
        # Стакан без стены
        bids = [(59700, 0.1)]
        asks = [(60100, 1.0)]
        a, _ = self._make_analyzer("BTCUSDT", bids, asks,
                                   min_volume_usdt=500_000)
        assert not a.is_level_still_valid(
            "BTCUSDT", 59700, Direction.LONG
        )

    def test_orderbook_imbalance_bid_heavy(self):
        # Много объёма на бидах
        bids = [(59900 - i*10, 10.0) for i in range(10)]
        asks = [(60100 + i*10, 0.1) for i in range(10)]
        a, _ = self._make_analyzer("BTCUSDT", bids, asks)
        imbalance = a.get_orderbook_imbalance("BTCUSDT")
        assert imbalance > 0.55

    def test_orderbook_imbalance_ask_heavy(self):
        # Много объёма на асках
        bids = [(59900 - i*10, 0.1) for i in range(10)]
        asks = [(60100 + i*10, 10.0) for i in range(10)]
        a, _ = self._make_analyzer("BTCUSDT", bids, asks)
        imbalance = a.get_orderbook_imbalance("BTCUSDT")
        assert imbalance < 0.45

    def test_stats_keys(self):
        manager = OrderBookManager()
        a = DepthShotAnalyzer(make_config(), manager)
        stats = a.get_stats()
        assert 'total_signals' in stats
        assert 'config' in stats
        assert stats['config']['min_volume_usdt'] == 100_000
