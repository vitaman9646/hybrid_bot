"""
Тесты для VectorAnalyzer.
"""
import pytest
import time

from analyzers.vector_analyzer import VectorAnalyzer, MarketState, Frame
from models.signals import TradeData, Direction


def make_trade(
    symbol: str,
    price: float,
    qty: float = 0.1,
    side: str = "Buy",
    ts: float = None,
) -> TradeData:
    if ts is None:
        ts = time.time()
    return TradeData(
        symbol=symbol,
        price=price,
        qty=qty,
        quote_volume=price * qty,
        side=side,
        timestamp=ts,
    )


def make_config(**kwargs) -> dict:
    base = {
        'frame_size': 0.2,
        'time_frame': 1.0,
        'min_spread_size': 0.5,
        'min_trades_per_frame': 2,
        'min_quote_volume': 1_000,
        'dead_threshold': 0.1,
        'chaos_threshold': 5.0,
    }
    base.update(kwargs)
    return base


class TestFrame:
    def test_empty_frame(self):
        f = Frame(start_time=0.0, end_time=0.2)
        assert f.spread == 0.0
        assert f.spread_pct == 0.0
        assert f.trade_count == 0

    def test_add_trades(self):
        f = Frame(start_time=0.0, end_time=0.2)
        f.add_trade(100.0, 1.0, 100.0)
        f.add_trade(101.0, 1.0, 101.0)
        assert f.min_price == 100.0
        assert f.max_price == 101.0
        assert f.spread == pytest.approx(1.0)
        assert f.spread_pct == pytest.approx(1.0, rel=0.01)
        assert f.trade_count == 2
        assert f.quote_volume == pytest.approx(201.0)

    def test_is_complete(self):
        past = time.time() - 1.0
        f = Frame(start_time=past - 0.2, end_time=past)
        assert f.is_complete is True

        future = time.time() + 10.0
        f2 = Frame(start_time=time.time(), end_time=future)
        assert f2.is_complete is False


class TestVectorAnalyzer:

    def _fill_frames(
        self,
        analyzer: VectorAnalyzer,
        symbol: str,
        base_price: float,
        spread_pct: float,
        frames_count: int,
        trades_per_frame: int = 3,
        volume_per_trade: float = 5_000,
        direction: str = 'up',
    ):
        """
        Заполнить N фреймов трейдами.
        direction: 'up' | 'down' | 'flat'
        """
        now = time.time()
        price = base_price

        for frame_idx in range(frames_count):
            t_start = now + frame_idx * analyzer.frame_size

            spread = price * spread_pct / 100

            if direction == 'up':
                low = price
                high = price + spread
                price += spread * 0.3  # движение вверх
            elif direction == 'down':
                high = price
                low = price - spread
                price -= spread * 0.3
            else:
                low = price - spread / 2
                high = price + spread / 2

            for t_idx in range(trades_per_frame):
                t = t_start + t_idx * (analyzer.frame_size / trades_per_frame)
                # чередуем low/high внутри фрейма
                p = low if t_idx % 2 == 0 else high
                qty = volume_per_trade / p
                trade = make_trade(symbol, p, qty, ts=t)
                analyzer.on_trade(trade)

    def test_init(self):
        analyzer = VectorAnalyzer(make_config())
        assert analyzer.frame_size == 0.2
        assert analyzer.time_frame == 1.0
        assert analyzer._frames_needed == 5

    def test_no_signal_not_enough_frames(self):
        analyzer = VectorAnalyzer(make_config())
        signal = analyzer.on_trade(make_trade("BTCUSDT", 60000))
        assert signal is None

    def test_no_signal_low_spread(self):
        analyzer = VectorAnalyzer(make_config(min_spread_size=1.0))
        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.1,
            frames_count=6, direction='up'
        )
        stats = analyzer.get_stats("BTCUSDT")
        # Спред 0.1% < min 1.0% → не должно быть сигнала
        assert stats['signals_generated'] == 0

    def test_signal_uptrend(self):
        analyzer = VectorAnalyzer(make_config(
            min_spread_size=0.3,
            min_trades_per_frame=2,
            min_quote_volume=1_000,
        ))

        signals = []
        analyzer.on_signal(lambda s: signals.append(s))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.5,
            frames_count=8, direction='up',
            volume_per_trade=5_000,
        )

        # Должен быть хотя бы один сигнал
        assert len(signals) > 0
        # Направление — вверх
        assert signals[0].direction == Direction.LONG
        assert signals[0].symbol == "BTCUSDT"
        assert 0 < signals[0].confidence <= 1.0

    def test_signal_downtrend(self):
        analyzer = VectorAnalyzer(make_config(
            min_spread_size=0.3,
            min_trades_per_frame=2,
            min_quote_volume=1_000,
        ))

        signals = []
        analyzer.on_signal(lambda s: signals.append(s))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.5,
            frames_count=8, direction='down',
            volume_per_trade=5_000,
        )

        assert len(signals) > 0
        assert signals[0].direction == Direction.SHORT

    def test_market_state_dead(self):
        analyzer = VectorAnalyzer(make_config(
            dead_threshold=0.3,
            min_spread_size=0.05,
        ))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.01,
            frames_count=6, direction='flat',
            volume_per_trade=5_000,
        )

        state = analyzer.get_market_state("BTCUSDT")
        assert state == MarketState.DEAD

    def test_market_state_chaos(self):
        analyzer = VectorAnalyzer(make_config(
            chaos_threshold=3.0,
            min_spread_size=0.3,
        ))

        signals = []
        analyzer.on_signal(lambda s: signals.append(s))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=5.0,
            frames_count=8, direction='up',
            volume_per_trade=5_000,
        )

        state = analyzer.get_market_state("BTCUSDT")
        assert state == MarketState.CHAOS
        # В хаосе сигналов нет
        assert len(signals) == 0

    def test_no_signal_low_volume(self):
        analyzer = VectorAnalyzer(make_config(
            min_quote_volume=100_000,
            min_spread_size=0.3,
        ))

        signals = []
        analyzer.on_signal(lambda s: signals.append(s))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.5,
            frames_count=8, direction='up',
            volume_per_trade=100,  # мало объёма
        )

        assert len(signals) == 0

    def test_no_signal_low_trades(self):
        analyzer = VectorAnalyzer(make_config(
            min_trades_per_frame=10,
            min_spread_size=0.3,
        ))

        signals = []
        analyzer.on_signal(lambda s: signals.append(s))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.5,
            frames_count=8, direction='up',
            trades_per_frame=2,  # меньше min_trades_per_frame=10
            volume_per_trade=5_000,
        )

        assert len(signals) == 0

    def test_multiple_symbols_independent(self):
        analyzer = VectorAnalyzer(make_config(
            min_spread_size=0.3,
            min_trades_per_frame=2,
            min_quote_volume=1_000,
        ))

        btc_signals = []
        eth_signals = []

        def on_signal(s):
            if s.symbol == "BTCUSDT":
                btc_signals.append(s)
            else:
                eth_signals.append(s)

        analyzer.on_signal(on_signal)

        # BTC — растёт
        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.5,
            frames_count=8, direction='up', volume_per_trade=5_000,
        )
        # ETH — падает
        self._fill_frames(
            analyzer, "ETHUSDT", 3000, spread_pct=0.5,
            frames_count=8, direction='down', volume_per_trade=5_000,
        )

        assert len(btc_signals) > 0
        assert len(eth_signals) > 0
        assert btc_signals[0].direction == Direction.LONG
        assert eth_signals[0].direction == Direction.SHORT

    def test_confidence_range(self):
        analyzer = VectorAnalyzer(make_config(
            min_spread_size=0.3,
            min_trades_per_frame=2,
            min_quote_volume=1_000,
        ))

        signals = []
        analyzer.on_signal(lambda s: signals.append(s))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.8,
            frames_count=8, direction='up', volume_per_trade=10_000,
        )

        for sig in signals:
            assert 0.0 <= sig.confidence <= 1.0

    def test_signal_has_required_fields(self):
        analyzer = VectorAnalyzer(make_config(
            min_spread_size=0.3,
            min_trades_per_frame=2,
            min_quote_volume=1_000,
        ))

        signals = []
        analyzer.on_signal(lambda s: signals.append(s))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.5,
            frames_count=8, direction='up', volume_per_trade=5_000,
        )

        assert len(signals) > 0
        sig = signals[0]

        assert sig.symbol == "BTCUSDT"
        assert sig.direction in (Direction.LONG, Direction.SHORT)
        assert sig.timestamp > 0
        assert sig.spread_pct > 0
        assert sig.upper_border > sig.lower_border
        assert sig.frame_count > 0
        assert sig.avg_volume_per_frame > 0
        assert isinstance(sig.market_state, MarketState)
        assert isinstance(sig.is_shot, bool)

    def test_stats(self):
        analyzer = VectorAnalyzer(make_config())
        stats = analyzer.get_stats()

        assert 'total_signals' in stats
        assert 'config' in stats
        assert stats['config']['frame_size'] == 0.2

    def test_stats_per_symbol(self):
        analyzer = VectorAnalyzer(make_config(
            min_spread_size=0.3,
            min_trades_per_frame=2,
            min_quote_volume=1_000,
        ))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.5,
            frames_count=8, direction='up', volume_per_trade=5_000,
        )

        stats = analyzer.get_stats("BTCUSDT")
        assert stats['symbol'] == "BTCUSDT"
        assert 'market_state' in stats
        assert 'frames_collected' in stats
        assert 'signals_generated' in stats

    def test_detect_shot_mode(self):
        analyzer = VectorAnalyzer(make_config(
            use_detect_shot=True,
            shot_direction='up',
            shot_retracement=80.0,
            min_spread_size=0.3,
            min_trades_per_frame=2,
            min_quote_volume=1_000,
        ))

        signals = []
        analyzer.on_signal(lambda s: signals.append(s))

        self._fill_frames(
            analyzer, "BTCUSDT", 60000, spread_pct=0.5,
            frames_count=8, direction='up', volume_per_trade=5_000,
        )

        # В режиме shot сигналы должны иметь is_shot=True
        for sig in signals:
            assert sig.is_shot is True
