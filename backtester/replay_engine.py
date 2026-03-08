"""
backtester/replay_engine.py — воспроизведение истории через анализаторы

Использование:
    engine = ReplayEngine(strategy_config, db_path="data/market.db")
    result = engine.run(symbol="BTCUSDT", ts_from=..., ts_to=...)
    print(result.sharpe, result.win_rate)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from backtester.market_saver import MarketSaver, TradeRecord
from models.signals import TradeData, Direction
from analyzers.vector_analyzer import VectorAnalyzer
from analyzers.averages_analyzer import AveragesAnalyzer
from analyzers.depth_shot_analyzer import DepthShotAnalyzer
from analyzers.signal_aggregator import SignalAggregator, AggregatedSignal
from core.volatility_tracker import VolatilityTracker
from core.orderbook import OrderBookManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Результаты бэктеста
# ---------------------------------------------------------------------------

@dataclass
class BacktestTrade:
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    qty: float
    pnl_usdt: float
    pnl_pct: float
    entry_ts: float
    exit_ts: float
    exit_reason: str    # 'tp' / 'sl' / 'end_of_data'
    scenario: str


@dataclass
class BacktestResult:
    symbol: str
    ts_from: float
    ts_to: float
    trades: list[BacktestTrade] = field(default_factory=list)

    # Метрики (заполняются после run)
    total_trades: int = 0
    win_trades: int = 0
    loss_trades: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    sharpe: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0

    def calc_metrics(self):
        """Рассчитывает все метрики из списка trades."""
        if not self.trades:
            return

        self.total_trades = len(self.trades)
        wins = [t for t in self.trades if t.pnl_usdt > 0]
        losses = [t for t in self.trades if t.pnl_usdt <= 0]

        self.win_trades = len(wins)
        self.loss_trades = len(losses)
        self.win_rate = self.win_trades / self.total_trades if self.total_trades else 0.0
        self.total_pnl = sum(t.pnl_usdt for t in self.trades)

        self.avg_win = sum(t.pnl_usdt for t in wins) / len(wins) if wins else 0.0
        self.avg_loss = sum(t.pnl_usdt for t in losses) / len(losses) if losses else 0.0

        gross_profit = sum(t.pnl_usdt for t in wins)
        gross_loss = abs(sum(t.pnl_usdt for t in losses))
        self.profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        # Max drawdown
        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in self.trades:
            equity += t.pnl_usdt
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd
        self.max_drawdown = max_dd

        # Sharpe (упрощённый, без risk-free rate)
        import math
        pnls = [t.pnl_usdt for t in self.trades]
        if len(pnls) >= 2:
            mean = sum(pnls) / len(pnls)
            variance = sum((p - mean) ** 2 for p in pnls) / len(pnls)
            std = math.sqrt(variance)
            # Защита от деления на ~0 (все PnL одинаковые)
            if std < 1e-8:
                self.sharpe = 0.0
            else:
                self.sharpe = mean / std
        else:
            self.sharpe = 0.0

    def summary(self) -> str:
        return (
            f"BacktestResult [{self.symbol}] "
            f"trades={self.total_trades} "
            f"win_rate={self.win_rate:.1%} "
            f"pnl={self.total_pnl:+.2f} USDT "
            f"sharpe={self.sharpe:.3f} "
            f"max_dd={self.max_drawdown:.2f} USDT "
            f"pf={self.profit_factor:.2f}"
        )


# ---------------------------------------------------------------------------
# Симулированная позиция
# ---------------------------------------------------------------------------

@dataclass
class SimPosition:
    symbol: str
    direction: str
    entry_price: float
    qty: float
    tp_price: float
    sl_price: float
    entry_ts: float
    scenario: str
    size_usdt: float


# ---------------------------------------------------------------------------
# ReplayEngine
# ---------------------------------------------------------------------------

class ReplayEngine:
    """
    Воспроизводит исторические тики через анализаторы.
    Симулирует открытие/закрытие позиций без реальных ордеров.
    """

    def __init__(self, strategy_config: dict, db_path: str = "data/market.db"):
        self._cfg = strategy_config
        self._db = MarketSaver(db_path)

        order_cfg = strategy_config.get('order', {})
        self._size_usdt: float = order_cfg.get('size_usdt', 50.0)
        self._tp_pct: float = order_cfg.get('take_profit', {}).get('percent', 0.8)
        self._sl_pct: float = order_cfg.get('stop_loss', {}).get('percent', 1.5)

        # Минимальная уверенность для входа
        self._min_confidence: float = strategy_config.get('min_score', 0.55)

    def run(
        self,
        symbol: str,
        ts_from: float,
        ts_to: float,
    ) -> BacktestResult:
        """
        Прогоняет историю symbol за период [ts_from, ts_to].
        Возвращает BacktestResult с метриками.
        """
        result = BacktestResult(symbol=symbol, ts_from=ts_from, ts_to=ts_to)

        # Проверяем наличие данных за период
        trade_count = self._db.get_trade_count_period(symbol, ts_from, ts_to)
        if trade_count == 0:
            logger.warning("No trades found for %s in period", symbol)
            return result

        logger.info(
            "ReplayEngine: %s trades=%d period=%.0fh (streaming)",
            symbol, trade_count, (ts_to - ts_from) / 3600,
        )

        # Инициализируем анализаторы (свежие для каждого прогона)
        analyzers = self._make_analyzers()
        vector = analyzers['vector']
        averages = analyzers['averages']
        depth = analyzers['depth']
        aggregator = analyzers['aggregator']
        volatility = analyzers['volatility']

        # Собранные сигналы
        signals: list[AggregatedSignal] = []
        aggregator.on_signal(lambda s: signals.append(s))

        # Открытая симулированная позиция
        open_pos: Optional[SimPosition] = None
        last_signal_ts: float = 0.0
        cooldown = self._cfg.get('aggregator', {}).get('signal_cooldown', 10.0)

        sample = self._cfg.get('backtest_sample_every', 500)
        last_trade = None
        for trade in self._db.iter_trades(symbol, ts_from, ts_to, sample_every=sample):
            trade_data = self._to_trade_data(trade)

            # Обновляем анализаторы
            volatility.update(
                symbol=trade.symbol,
                price=trade.price,
                timestamp=trade.timestamp,
                volume=trade.qty,
            )
            averages.on_trade(trade_data)
            vector_signal = vector.on_trade(trade_data)

            # Проверяем открытую позицию
            if open_pos:
                bt_trade = self._check_exit(open_pos, trade)
                if bt_trade:
                    result.trades.append(bt_trade)
                    open_pos = None
                continue

            # Ищем сигнал
            last_trade = trade
            if trade.timestamp - last_signal_ts < cooldown:
                continue

            signals.clear()
            aggregator.evaluate(
                symbol=trade.symbol,
                vector_signal=vector_signal,
                current_price=trade.price,
            )

            if signals:
                sig = signals[-1]
                if sig.confidence >= self._min_confidence:
                    open_pos = self._open_sim_position(sig, trade)
                    last_signal_ts = trade.timestamp

        # Закрываем незакрытую позицию по последней цене
        if open_pos and last_trade:
            last = last_trade
            pnl = self._calc_pnl(open_pos, last.price)
            result.trades.append(BacktestTrade(
                symbol=open_pos.symbol,
                direction=open_pos.direction,
                entry_price=open_pos.entry_price,
                exit_price=last.price,
                qty=open_pos.qty,
                pnl_usdt=pnl,
                pnl_pct=pnl / open_pos.size_usdt * 100,
                entry_ts=open_pos.entry_ts,
                exit_ts=last.timestamp,
                exit_reason='end_of_data',
                scenario=open_pos.scenario,
            ))

        result.calc_metrics()
        logger.info(result.summary())
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _make_analyzers(self) -> dict:
        strategy_cfg = self._cfg.get('analyzers', {})
        volatility = VolatilityTracker()
        orderbook = OrderBookManager()

        vector = VectorAnalyzer(strategy_cfg.get('vector', {}))
        averages = AveragesAnalyzer(strategy_cfg.get('averages', {}))
        depth = DepthShotAnalyzer(strategy_cfg.get('depth_shot', {}), orderbook)
        aggregator = SignalAggregator(
            self._cfg.get('aggregator', {}),
            vector, averages, depth,
        )
        return dict(
            vector=vector, averages=averages,
            depth=depth, aggregator=aggregator,
            volatility=volatility,
        )

    @staticmethod
    def _to_trade_data(record: TradeRecord) -> TradeData:
        return TradeData(
            symbol=record.symbol,
            price=record.price,
            qty=record.qty,
            quote_volume=record.price * record.qty,
            trade_id="",
            side=record.side,
            timestamp=record.timestamp,
        )

    def _open_sim_position(
        self, sig: AggregatedSignal, trade: TradeRecord
    ) -> SimPosition:
        direction = sig.direction.value
        entry = trade.price
        qty = self._size_usdt / entry

        if direction == 'long':
            tp = entry * (1 + self._tp_pct / 100)
            sl = entry * (1 - self._sl_pct / 100)
        else:
            tp = entry * (1 - self._tp_pct / 100)
            sl = entry * (1 + self._sl_pct / 100)

        return SimPosition(
            symbol=trade.symbol,
            direction=direction,
            entry_price=entry,
            qty=qty,
            tp_price=tp,
            sl_price=sl,
            entry_ts=trade.timestamp,
            scenario=sig.scenario.value,
            size_usdt=self._size_usdt,
        )

    def _check_exit(
        self, pos: SimPosition, trade: TradeRecord
    ) -> Optional[BacktestTrade]:
        price = trade.price
        hit_tp = hit_sl = False

        if pos.direction == 'long':
            hit_tp = price >= pos.tp_price
            hit_sl = price <= pos.sl_price
        else:
            hit_tp = price <= pos.tp_price
            hit_sl = price >= pos.sl_price

        if not (hit_tp or hit_sl):
            return None

        exit_price = pos.tp_price if hit_tp else pos.sl_price
        pnl = self._calc_pnl(pos, exit_price)

        return BacktestTrade(
            symbol=pos.symbol,
            direction=pos.direction,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            qty=pos.qty,
            pnl_usdt=pnl,
            pnl_pct=pnl / pos.size_usdt * 100,
            entry_ts=pos.entry_ts,
            exit_ts=trade.timestamp,
            exit_reason='tp' if hit_tp else 'sl',
            scenario=pos.scenario,
        )

    @staticmethod
    def _calc_pnl(pos: SimPosition, exit_price: float) -> float:
        TAKER_FEE = 0.00055  # Bybit taker 0.055%
        if pos.direction == 'long':
            gross = (exit_price - pos.entry_price) * pos.qty
        else:
            gross = (pos.entry_price - exit_price) * pos.qty
        entry_fee = pos.entry_price * pos.qty * TAKER_FEE
        exit_fee = exit_price * pos.qty * TAKER_FEE
        return gross - entry_fee - exit_fee
