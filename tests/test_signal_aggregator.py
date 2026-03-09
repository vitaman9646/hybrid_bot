import pytest
import time
from unittest.mock import MagicMock, patch
from analyzers.signal_aggregator import (
    SignalAggregator, ScenarioType, AggregationMode
)
from analyzers.vector_analyzer import VectorAnalyzer, MarketState, VectorSignal
from analyzers.averages_analyzer import AveragesAnalyzer, TrendState
from analyzers.depth_shot_analyzer import DepthShotAnalyzer, DepthShotSignal
from core.orderbook import OrderBookManager
from models.signals import Direction, TradeData


def make_ob_manager(symbol, bids, asks):
    manager = OrderBookManager()
    msg = {
        'topic': f'orderbook.50.{symbol}',
        'type': 'snapshot',
        'ts': int(time.time() * 1000),
        'data': {
            's': symbol,
            'b': [[str(p), str(q)] for p, q in bids],
            'a': [[str(p), str(q)] for p, q in asks],
            'u': 1,
        }
    }
    manager.process_message(msg)
    return manager


def make_vector_signal(symbol, direction, confidence=0.8,
                       market_state=MarketState.NORMAL):
    return VectorSignal(
        symbol=symbol,
        direction=direction,
        timestamp=time.time(),
        spread_pct=0.5,
        upper_border=60050.0,
        lower_border=59950.0,
        frame_count=5,
        avg_volume_per_frame=10_000,
        confidence=confidence,
        market_state=market_state,
    )


def make_aggregator(symbol="BTCUSDT", bids=None, asks=None,
                    imbalance_filter=False, cooldown=0.0,
                    s2_threshold=0.50, **kwargs):
    if bids is None:
        bids = [(59700, 10.0), (59800, 0.5)]
    if asks is None:
        asks = [(60100, 0.5), (60300, 10.0)]

    manager = make_ob_manager(symbol, bids, asks)
    vector = VectorAnalyzer({'frame_size': 0.2, 'time_frame': 1.0,
                              'min_spread_size': 0.3})
    averages = AveragesAnalyzer({'short_period': 10.0, 'long_period': 60.0,
                                  'min_delta_pct': 0.1,
                                  'oversold_delta': -0.3,
                                  'overbought_delta': 0.3})
    depth = DepthShotAnalyzer({
        'min_volume_usdt': 500_000,
        'min_distance_pct': 0.1,
        'max_distance_pct': 5.0,
        'tp_type': 'classic',
        'tp_pct': 0.3,
        'wall_tracker': {'min_age_s': 0.0, 'max_drop_pct': 0.99},
    }, manager)

    config = {
        'mode': 'weighted',
        'weight_vector': 0.5,
        'weight_averages': 0.3,
        'weight_depth': 0.2,
        'entry_threshold': 0.3,
        'use_imbalance_filter': imbalance_filter,
        'thresholds': {
            'all_three':       0.30,
            'averages_vector': s2_threshold,
            'vector_depth':    0.30,
            'averages_depth':  0.30,
        },
        'cooldown': {
            'default':  cooldown,
            'after_tp': cooldown,
            'after_sl': cooldown,
        },
    }
    config.update(kwargs)

    agg = SignalAggregator(config, vector, averages, depth)
    return agg, vector, averages, depth


def feed_uptrend(averages, symbol, now=None):
    if now is None:
        now = time.time() - 70
    for i, price in enumerate([60000.0]*60 + [60200.0]*10):
        averages.on_trade(TradeData(symbol, price, 1.0, price, "Buy", now+i))


def feed_downtrend(averages, symbol, now=None):
    if now is None:
        now = time.time() - 70
    for i, price in enumerate([60000.0]*60 + [59800.0]*10):
        averages.on_trade(TradeData(symbol, price, 1.0, price, "Buy", now+i))


def feed_oversold(averages, symbol, now=None):
    if now is None:
        now = time.time() - 70
    for i, price in enumerate([60000.0]*60 + [58000.0]*10):
        averages.on_trade(TradeData(symbol, price, 1.0, price, "Buy", now+i))


class TestSignalAggregator:

    def test_init(self):
        agg, *_ = make_aggregator()
        assert agg.mode == AggregationMode.WEIGHTED
        assert agg.entry_threshold == 0.3

    def test_no_signal_dead_market(self):
        agg, vector, averages, depth = make_aggregator()
        # Мокаем dead market
        vector._market_state["BTCUSDT"] = MarketState.DEAD
        result = agg.evaluate("BTCUSDT", None, 60000)
        assert result is None

    def test_no_signal_chaos_market(self):
        agg, vector, *_ = make_aggregator()
        vector._market_state["BTCUSDT"] = MarketState.CHAOS
        result = agg.evaluate("BTCUSDT", None, 60000)
        assert result is None

    def test_scenario4_all_three_agree(self):
        agg, vector, averages, depth = make_aggregator()
        feed_uptrend(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.NORMAL

        vsig = make_vector_signal("BTCUSDT", Direction.LONG,
                                   market_state=MarketState.NORMAL)
        result = agg.evaluate("BTCUSDT", vsig, 60000)

        assert result is not None
        assert result.scenario == ScenarioType.SCENARIO4
        assert result.direction == Direction.LONG
        assert result.vector_confidence > 0
        assert result.depth_confidence > 0

    def test_scenario4_conflict_direction(self):
        """Vector говорит SHORT, тренд UP → конфликт → нет сигнала"""
        agg, vector, averages, _ = make_aggregator()
        feed_uptrend(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.NORMAL

        vsig = make_vector_signal("BTCUSDT", Direction.SHORT)
        result = agg.evaluate("BTCUSDT", vsig, 60000)
        assert result is None

    def test_scenario2_trend_plus_impulse(self):
        agg, vector, averages, _ = make_aggregator(s2_threshold=0.45)
        feed_uptrend(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.VOLATILE

        vsig = make_vector_signal("BTCUSDT", Direction.LONG,
                                   market_state=MarketState.VOLATILE)
        result = agg.evaluate("BTCUSDT", vsig, 60000)

        assert result is not None
        assert result.scenario == ScenarioType.SCENARIO2
        assert result.direction == Direction.LONG
        assert result.averages_confidence > 0
        assert result.depth_confidence == 0.0

    def test_scenario2_conflict_falls_back_to_scenario1(self):
        """Тренд DOWN, Vector говорит LONG → S2 конфликт → падает на S1"""
        agg, vector, averages, _ = make_aggregator()
        feed_downtrend(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.VOLATILE

        vsig = make_vector_signal("BTCUSDT", Direction.LONG,
                                   market_state=MarketState.VOLATILE)
        result = agg.evaluate("BTCUSDT", vsig, 60000)
        # S2 заблокирован конфликтом, но S1 (Vector+Depth) проходит
        assert result is not None
        assert result.scenario == ScenarioType.SCENARIO1
        assert agg._conflicts_logged > 0

    def test_scenario1_vector_depth(self):
        agg, vector, averages, _ = make_aggregator()
        # Нет тренда — flat
        averages._trend_state["BTCUSDT"] = TrendState.FLAT
        vector._market_state["BTCUSDT"] = MarketState.VOLATILE

        vsig = make_vector_signal("BTCUSDT", Direction.LONG,
                                   market_state=MarketState.VOLATILE)
        result = agg.evaluate("BTCUSDT", vsig, 60000)

        assert result is not None
        assert result.scenario == ScenarioType.SCENARIO1
        assert result.vector_confidence > 0
        assert result.depth_confidence > 0

    def test_scenario3_oversold_depth(self):
        agg, vector, averages, _ = make_aggregator(
            bids=[(59950, 10.0)],  # стена близко — 0.08% от 60000
            asks=[(60100, 0.5)],
        )
        feed_oversold(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.NORMAL
        averages._trend_state["BTCUSDT"] = TrendState.FLAT

        result = agg.evaluate("BTCUSDT", None, 60000)

        assert result is not None
        assert result.scenario == ScenarioType.SCENARIO3
        assert result.direction == Direction.LONG

    def test_cooldown_blocks_second_signal(self):
        agg, vector, averages, _ = make_aggregator(cooldown=60.0)
        feed_uptrend(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.NORMAL

        vsig = make_vector_signal("BTCUSDT", Direction.LONG)
        result1 = agg.evaluate("BTCUSDT", vsig, 60000)
        result2 = agg.evaluate("BTCUSDT", vsig, 60000)

        assert result1 is not None
        assert result2 is None  # заблокирован cooldown

    def test_signal_callback_called(self):
        agg, vector, averages, _ = make_aggregator()
        feed_uptrend(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.NORMAL

        received = []
        agg.on_signal(received.append)

        vsig = make_vector_signal("BTCUSDT", Direction.LONG)
        agg.evaluate("BTCUSDT", vsig, 60000)

        assert len(received) == 1
        assert received[0].direction == Direction.LONG

    def test_signal_has_required_fields(self):
        agg, vector, averages, _ = make_aggregator()
        feed_uptrend(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.NORMAL

        vsig = make_vector_signal("BTCUSDT", Direction.LONG)
        result = agg.evaluate("BTCUSDT", vsig, 60000)

        assert result is not None
        assert result.symbol == "BTCUSDT"
        assert result.entry_price > 0
        assert result.tp_price > 0
        assert 0 < result.confidence <= 1.0
        assert result.timestamp > 0
        assert isinstance(result.market_state, MarketState)

    def test_stats_keys(self):
        agg, *_ = make_aggregator()
        stats = agg.get_stats()
        for key in ['mode', 'total_evaluated', 'total_signals',
                    'conflicts_logged', 'by_scenario', 'weights']:
            assert key in stats

    def test_stats_count_signals(self):
        agg, vector, averages, _ = make_aggregator()
        feed_uptrend(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.NORMAL

        vsig = make_vector_signal("BTCUSDT", Direction.LONG)
        agg.evaluate("BTCUSDT", vsig, 60000)

        stats = agg.get_stats()
        assert stats['total_signals'] == 1
        assert stats['total_evaluated'] >= 1

    def test_conflicts_counted(self):
        agg, vector, averages, _ = make_aggregator()
        feed_uptrend(averages, "BTCUSDT")
        vector._market_state["BTCUSDT"] = MarketState.NORMAL

        # Конфликт: тренд UP, vector SHORT
        vsig = make_vector_signal("BTCUSDT", Direction.SHORT)
        agg.evaluate("BTCUSDT", vsig, 60000)

        assert agg._conflicts_logged > 0
