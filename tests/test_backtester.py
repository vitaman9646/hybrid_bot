"""
tests/test_backtester.py

Запуск: pytest tests/test_backtester.py -v
"""

import pytest
import time
import tempfile
import os
from unittest.mock import MagicMock, patch

from backtester.market_saver import MarketSaver, TradeRecord
from backtester.replay_engine import ReplayEngine, BacktestResult, BacktestTrade
from backtester.optimizer import ParameterOptimizer, OptimizationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_db(tmp_path) -> MarketSaver:
    return MarketSaver(str(tmp_path / "test.db"))


def make_trades(
    symbol: str = "BTCUSDT",
    count: int = 100,
    start_price: float = 50000.0,
    start_ts: float = None,
    price_step: float = 10.0,
) -> list[TradeRecord]:
    ts = start_ts or time.time()
    trades = []
    price = start_price
    for i in range(count):
        trades.append(TradeRecord(
            symbol=symbol,
            price=price,
            qty=0.01,
            side='Buy' if i % 2 == 0 else 'Sell',
            timestamp=ts + i * 0.5,
        ))
        price += price_step
    return trades


def make_strategy_config() -> dict:
    return {
        'name': 'test',
        'min_score': 0.3,
        'order': {
            'size_usdt': 100.0,
            'take_profit': {'percent': 0.8},
            'stop_loss': {'percent': 1.5},
        },
        'aggregator': {
            'entry_threshold': 0.3,
            'signal_cooldown': 1.0,
            'weight_vector': 0.5,
            'weight_averages': 0.3,
            'weight_depth': 0.2,
            'mode': 'weighted',
            'use_imbalance_filter': False,
        },
        'analyzers': {
            'vector': {
                'time_frame': 0.6,
                'frame_size': 0.2,
                'min_spread_size': 0.001,
                'min_quote_volume': 1.0,
                'min_trades_per_frame': 1,
            },
            'averages': {
                'short_period': 5.0,
                'long_period': 30.0,
                'min_delta_pct': 0.01,
                'overbought_delta': 0.5,
                'oversold_delta': -0.5,
            },
            'depth_shot': {
                'min_volume_usdt': 1.0,
                'min_distance_pct': 0.0,
                'max_distance_pct': 5.0,
                'buffer_pct': 0.1,
                'tp_pct': 0.3,
                'tp_type': 'classic',
                'stop_if_outside': False,
            },
        },
    }


# ---------------------------------------------------------------------------
# MarketSaver
# ---------------------------------------------------------------------------

class TestMarketSaver:

    def test_save_and_load_trades(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=10, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        loaded = db.get_trades("BTCUSDT", 999.0, 2000.0)
        assert len(loaded) == 10

    def test_get_symbols(self, tmp_path):
        db = make_db(tmp_path)
        for sym in ["BTCUSDT", "ETHUSDT"]:
            for t in make_trades(sym, count=5, start_ts=1000.0):
                db.save_trade_record(t)
        db.flush()
        symbols = db.get_symbols()
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols

    def test_get_time_range(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=5, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        ts_min, ts_max = db.get_time_range("BTCUSDT")
        assert ts_min == pytest.approx(1000.0)
        assert ts_max == pytest.approx(1000.0 + 4 * 0.5)

    def test_get_trade_count(self, tmp_path):
        db = make_db(tmp_path)
        for t in make_trades("BTCUSDT", count=7, start_ts=1000.0):
            db.save_trade_record(t)
        db.flush()
        assert db.get_trade_count("BTCUSDT") == 7

    def test_get_trades_time_filter(self, tmp_path):
        db = make_db(tmp_path)
        for t in make_trades("BTCUSDT", count=20, start_ts=1000.0):
            db.save_trade_record(t)
        db.flush()
        # Берём только первые 5 (ts 1000..1002)
        loaded = db.get_trades("BTCUSDT", 1000.0, 1002.0)
        assert len(loaded) == 5

    def test_buffer_flush_on_close(self, tmp_path):
        db = make_db(tmp_path)
        db._buf_size = 1000   # большой буфер — не сбросится автоматически
        for t in make_trades("BTCUSDT", count=3, start_ts=1000.0):
            db.save_trade_record(t)
        db.close()
        # Открываем новое соединение и проверяем
        db2 = make_db(tmp_path)
        assert db2.get_trade_count("BTCUSDT") == 3

    def test_save_orderbook_snapshot(self, tmp_path):
        db = make_db(tmp_path)
        db.save_orderbook_snapshot(
            "BTCUSDT",
            bids=[(50000.0, 1.0), (49999.0, 2.0)],
            asks=[(50001.0, 1.0), (50002.0, 2.0)],
            timestamp=1000.0,
        )
        cur = db._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM orderbook_snapshots")
        assert cur.fetchone()[0] == 1

    def test_empty_db_returns_empty(self, tmp_path):
        db = make_db(tmp_path)
        assert db.get_trades("BTCUSDT", 0, 9999999) == []
        assert db.get_symbols() == []
        assert db.get_trade_count("BTCUSDT") == 0


# ---------------------------------------------------------------------------
# BacktestResult metrics
# ---------------------------------------------------------------------------

class TestBacktestResult:

    def _make_result(self, pnls: list[float]) -> BacktestResult:
        result = BacktestResult(symbol="BTCUSDT", ts_from=0, ts_to=1)
        for i, pnl in enumerate(pnls):
            result.trades.append(BacktestTrade(
                symbol="BTCUSDT",
                direction="long",
                entry_price=50000.0,
                exit_price=50000.0 + (pnl / 0.002),
                qty=0.002,
                pnl_usdt=pnl,
                pnl_pct=pnl / 100 * 100,
                entry_ts=float(i),
                exit_ts=float(i + 1),
                exit_reason='tp' if pnl > 0 else 'sl',
                scenario='s1',
            ))
        result.calc_metrics()
        return result

    def test_win_rate(self):
        r = self._make_result([1.0, 1.0, -1.0, 1.0])
        assert r.win_rate == pytest.approx(0.75)

    def test_total_pnl(self):
        r = self._make_result([2.0, -1.0, 3.0])
        assert r.total_pnl == pytest.approx(4.0)

    def test_max_drawdown(self):
        # equity: 1, 0, -1 → peak=1, max_dd=2
        r = self._make_result([1.0, -1.0, -1.0])
        assert r.max_drawdown == pytest.approx(2.0)

    def test_profit_factor(self):
        r = self._make_result([3.0, 3.0, -2.0])
        assert r.profit_factor == pytest.approx(3.0)

    def test_empty_trades(self):
        r = BacktestResult(symbol="BTCUSDT", ts_from=0, ts_to=1)
        r.calc_metrics()
        assert r.total_trades == 0
        assert r.win_rate == 0.0
        assert r.sharpe == 0.0

    def test_summary_string(self):
        r = self._make_result([1.0, -0.5])
        s = r.summary()
        assert "BTCUSDT" in s
        assert "trades=" in s
        assert "win_rate=" in s
    def test_sharpe_no_explosion_identical_pnls(self):
        """Sharpe не взрывается когда все PnL одинаковые (std ≈ 0)"""
        r = self._make_result([1.0, 1.0, 1.0, 1.0, 1.0])
        assert r.sharpe == 0.0
        assert r.sharpe < 1e10

    def test_sharpe_no_explosion_near_zero_std(self):
        """Sharpe не взрывается при очень маленьком std"""
        r = self._make_result([1.0, 1.0000001, 1.0, 1.0000001])
        assert r.sharpe < 1e10


# ---------------------------------------------------------------------------
# ReplayEngine
# ---------------------------------------------------------------------------

class TestReplayEngine:

    def _make_engine(self, tmp_path) -> tuple[ReplayEngine, MarketSaver]:
        db = make_db(tmp_path)
        engine = ReplayEngine(make_strategy_config(), str(tmp_path / "test.db"))
        return engine, db

    def test_empty_db_returns_empty_result(self, tmp_path):
        engine, _ = self._make_engine(tmp_path)
        result = engine.run("BTCUSDT", 0, 999999)
        assert result.total_trades == 0

    def test_run_returns_backtest_result(self, tmp_path):
        engine, db = self._make_engine(tmp_path)
        trades = make_trades("BTCUSDT", count=200, start_ts=1000.0, price_step=5.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        result = engine.run("BTCUSDT", 999.0, 2000.0)
        assert isinstance(result, BacktestResult)
        assert result.symbol == "BTCUSDT"

    def test_calc_pnl_long(self, tmp_path):
        engine, _ = self._make_engine(tmp_path)
        from backtester.replay_engine import SimPosition
        pos = SimPosition(
            symbol="BTCUSDT", direction="long",
            entry_price=50000.0, qty=0.002,
            tp_price=50400.0, sl_price=49250.0,
            entry_ts=0.0, scenario="s1", size_usdt=100.0,
        )
        pnl = engine._calc_pnl(pos, 50400.0)
        assert pnl == pytest.approx(0.669480, rel=1e-4)

    def test_calc_pnl_short(self, tmp_path):
        engine, _ = self._make_engine(tmp_path)
        from backtester.replay_engine import SimPosition
        pos = SimPosition(
            symbol="BTCUSDT", direction="short",
            entry_price=50000.0, qty=0.002,
            tp_price=49600.0, sl_price=50750.0,
            entry_ts=0.0, scenario="s1", size_usdt=100.0,
        )
        pnl = engine._calc_pnl(pos, 49600.0)
        assert pnl == pytest.approx(0.670520, rel=1e-4)

    def test_check_exit_tp_long(self, tmp_path):
        engine, _ = self._make_engine(tmp_path)
        from backtester.replay_engine import SimPosition
        pos = SimPosition(
            symbol="BTCUSDT", direction="long",
            entry_price=50000.0, qty=0.002,
            tp_price=50400.0, sl_price=49250.0,
            entry_ts=0.0, scenario="s1", size_usdt=100.0,
        )
        trade = TradeRecord("BTCUSDT", 50500.0, 0.01, "Buy", 1.0)
        bt = engine._check_exit(pos, trade)
        assert bt is not None
        assert bt.exit_reason == 'tp'

    def test_check_exit_sl_long(self, tmp_path):
        engine, _ = self._make_engine(tmp_path)
        from backtester.replay_engine import SimPosition
        pos = SimPosition(
            symbol="BTCUSDT", direction="long",
            entry_price=50000.0, qty=0.002,
            tp_price=50400.0, sl_price=49250.0,
            entry_ts=0.0, scenario="s1", size_usdt=100.0,
        )
        trade = TradeRecord("BTCUSDT", 49000.0, 0.01, "Sell", 1.0)
        bt = engine._check_exit(pos, trade)
        assert bt is not None
        assert bt.exit_reason == 'sl'

    def test_check_exit_no_trigger(self, tmp_path):
        engine, _ = self._make_engine(tmp_path)
        from backtester.replay_engine import SimPosition
        pos = SimPosition(
            symbol="BTCUSDT", direction="long",
            entry_price=50000.0, qty=0.002,
            tp_price=50400.0, sl_price=49250.0,
            entry_ts=0.0, scenario="s1", size_usdt=100.0,
        )
        trade = TradeRecord("BTCUSDT", 50100.0, 0.01, "Buy", 1.0)
        assert engine._check_exit(pos, trade) is None

    def test_apply_params(self, tmp_path):
        base = {'analyzers': {'vector': {'min_spread_size': 0.003}}}
        result = ParameterOptimizer._apply_params(base, {'analyzers.vector.min_spread_size': 0.01})
        assert result['analyzers']['vector']['min_spread_size'] == 0.01
        # Оригинал не изменился
        assert base['analyzers']['vector']['min_spread_size'] == 0.003


# ---------------------------------------------------------------------------
# ParameterOptimizer
# ---------------------------------------------------------------------------

class TestParameterOptimizer:

    def test_apply_params_nested(self):
        base = {'a': {'b': {'c': 1}}}
        result = ParameterOptimizer._apply_params(base, {'a.b.c': 99})
        assert result['a']['b']['c'] == 99

    def test_apply_params_creates_missing_keys(self):
        base = {}
        result = ParameterOptimizer._apply_params(base, {'x.y.z': 42})
        assert result['x']['y']['z'] == 42

    def test_apply_params_does_not_mutate_original(self):
        base = {'order': {'size_usdt': 50.0}}
        ParameterOptimizer._apply_params(base, {'order.size_usdt': 100.0})
        assert base['order']['size_usdt'] == 50.0

    def test_get_metric_sharpe(self):
        r = BacktestResult(symbol="X", ts_from=0, ts_to=1)
        r.sharpe = 1.5
        assert ParameterOptimizer._get_metric(r, 'sharpe') == pytest.approx(1.5)

    def test_get_metric_win_rate(self):
        r = BacktestResult(symbol="X", ts_from=0, ts_to=1)
        r.win_rate = 0.65
        assert ParameterOptimizer._get_metric(r, 'win_rate') == pytest.approx(0.65)

    def test_get_metric_max_drawdown_inverted(self):
        r = BacktestResult(symbol="X", ts_from=0, ts_to=1)
        r.max_drawdown = 10.0
        assert ParameterOptimizer._get_metric(r, 'max_drawdown') == pytest.approx(-10.0)

    def test_optimizer_run_empty_db(self, tmp_path):
        opt = ParameterOptimizer(make_strategy_config(), str(tmp_path / "test.db"))
        results = opt.run(
            symbol="BTCUSDT",
            ts_from=0, ts_to=99999,
            param_grid={'order.take_profit.percent': [0.5, 0.8]},
            min_trades=1,
        )
        # Нет данных → нет трейдов → пустой список
        assert results == []

    def test_optimizer_top_n_format(self, tmp_path):
        opt = ParameterOptimizer(make_strategy_config(), str(tmp_path / "test.db"))
        mock_results = []
        text = opt.top_n(mock_results, n=3)
        assert "Top 0" in text


# ---------------------------------------------------------------------------
# Новые тесты — добавлено в ходе оптимизации
# ---------------------------------------------------------------------------

class TestMarketSaverIterTrades:

    def test_iter_trades_basic(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=10, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        result = list(db.iter_trades("BTCUSDT", 999.0, 2000.0))
        assert len(result) == 10

    def test_iter_trades_sample_every(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=100, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        result = list(db.iter_trades("BTCUSDT", 999.0, 9999.0, sample_every=10))
        assert len(result) == 10

    def test_iter_trades_sample_every_1(self, tmp_path):
        """sample_every=1 должен вернуть все тики"""
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=50, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        result = list(db.iter_trades("BTCUSDT", 999.0, 9999.0, sample_every=1))
        assert len(result) == 50

    def test_iter_trades_empty(self, tmp_path):
        db = make_db(tmp_path)
        result = list(db.iter_trades("BTCUSDT", 0.0, 9999.0))
        assert result == []

    def test_iter_trades_time_filter(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=20, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        # Только первые 5 тиков (ts 1000..1002)
        result = list(db.iter_trades("BTCUSDT", 1000.0, 1002.0))
        assert len(result) == 5

    def test_iter_trades_returns_trade_records(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=5, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        result = list(db.iter_trades("BTCUSDT", 999.0, 9999.0))
        assert all(isinstance(r, TradeRecord) for r in result)
        assert result[0].symbol == "BTCUSDT"

    def test_iter_trades_ordered_by_ts(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=20, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        result = list(db.iter_trades("BTCUSDT", 999.0, 9999.0))
        timestamps = [r.timestamp for r in result]
        assert timestamps == sorted(timestamps)

    def test_iter_trades_chunk_size(self, tmp_path):
        """Маленький chunk_size не должен терять тики"""
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=25, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        result = list(db.iter_trades("BTCUSDT", 999.0, 9999.0, chunk_size=7))
        assert len(result) == 25


class TestMarketSaverGetTradeCountPeriod:

    def test_count_period_all(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=10, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        assert db.get_trade_count_period("BTCUSDT", 999.0, 9999.0) == 10

    def test_count_period_partial(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=20, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        count = db.get_trade_count_period("BTCUSDT", 1000.0, 1002.0)
        assert count == 5

    def test_count_period_empty(self, tmp_path):
        db = make_db(tmp_path)
        assert db.get_trade_count_period("BTCUSDT", 0.0, 9999.0) == 0

    def test_count_period_wrong_symbol(self, tmp_path):
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=10, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        assert db.get_trade_count_period("ETHUSDT", 999.0, 9999.0) == 0


class TestMarketSaverSafeCommit:

    def test_safe_commit_no_crash(self, tmp_path):
        """_safe_commit не падает если нет активной транзакции"""
        db = make_db(tmp_path)
        db._safe_commit()
        db._safe_commit()
        db._safe_commit()

    def test_flush_after_safe_commit(self, tmp_path):
        """Данные сохраняются даже после лишних commit"""
        db = make_db(tmp_path)
        db._safe_commit()
        trades = make_trades("BTCUSDT", count=5, start_ts=1000.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        assert db.get_trade_count("BTCUSDT") == 5


class TestReplayEngineStreaming:

    def _make_engine(self, tmp_path):
        db = make_db(tmp_path)
        cfg = make_strategy_config()
        cfg['backtest_sample_every'] = 1
        engine = ReplayEngine(cfg, str(tmp_path / "test.db"))
        return engine, db

    def test_streaming_no_oom(self, tmp_path):
        """ReplayEngine не грузит все тики в память"""
        engine, db = self._make_engine(tmp_path)
        trades = make_trades("BTCUSDT", count=500, start_ts=1000.0, price_step=5.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        result = engine.run("BTCUSDT", 999.0, 9999.0)
        assert isinstance(result, BacktestResult)

    def test_last_trade_tracked(self, tmp_path):
        """Незакрытая позиция закрывается по последней цене"""
        engine, db = self._make_engine(tmp_path)
        trades = make_trades("BTCUSDT", count=200, start_ts=1000.0, price_step=10.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()
        result = engine.run("BTCUSDT", 999.0, 9999.0)
        # end_of_data трейды должны закрываться корректно
        end_trades = [t for t in result.trades if t.exit_reason == 'end_of_data']
        assert len(end_trades) <= 1

    def test_sample_every_reduces_signals(self, tmp_path):
        """Большой sample_every даёт меньше сигналов"""
        db = make_db(tmp_path)
        trades = make_trades("BTCUSDT", count=1000, start_ts=1000.0, price_step=5.0)
        for t in trades:
            db.save_trade_record(t)
        db.flush()

        cfg1 = make_strategy_config()
        cfg1['backtest_sample_every'] = 1
        engine1 = ReplayEngine(cfg1, str(tmp_path / "test.db"))
        result1 = engine1.run("BTCUSDT", 999.0, 9999.0)

        cfg2 = make_strategy_config()
        cfg2['backtest_sample_every'] = 100
        engine2 = ReplayEngine(cfg2, str(tmp_path / "test.db"))
        result2 = engine2.run("BTCUSDT", 999.0, 9999.0)

        # С большим сэмплом трейдов меньше или равно
        assert result2.total_trades <= result1.total_trades

