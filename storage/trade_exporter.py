"""
storage/trade_exporter.py — экспорт сделок в CSV для анализа
"""
from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.position_manager import Position

logger = logging.getLogger(__name__)


class TradeExporter:

    DEFAULT_PATH = "data/trades.csv"

    HEADERS = [
        "timestamp", "date", "symbol", "direction",
        "entry_price", "exit_price", "tp_price", "sl_price",
        "qty", "size_usdt", "pnl_usdt", "pnl_pct",
        "exit_reason", "duration_sec", "scenario",
    ]

    def __init__(self, path: str = DEFAULT_PATH):
        self._path = Path(path)
        self._ensure_file()

    def _ensure_file(self):
        if not self._path.exists():
            with open(self._path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.HEADERS)
            logger.info("TradeExporter: created %s", self._path)

    def export(self, pos: "Position"):
        """Записывает закрытую позицию в CSV."""
        try:
            close_ts = time.time()
            duration = int(close_ts - pos.timestamp)
            date_str = datetime.fromtimestamp(pos.timestamp).strftime("%Y-%m-%d %H:%M:%S")

            # PnL %
            pnl_pct = 0.0
            if pos.entry_price and pos.size_usdt:
                pnl_pct = round(pos.realized_pnl / pos.size_usdt * 100, 4)

            row = [
                round(pos.timestamp, 3),
                date_str,
                pos.symbol,
                pos.direction,
                pos.entry_price,
                getattr(pos, 'exit_price', ''),
                pos.tp_price,
                pos.sl_price,
                pos.qty,
                pos.size_usdt,
                round(pos.realized_pnl, 6),
                pnl_pct,
                getattr(pos, 'close_reason', ''),
                duration,
                getattr(pos, 'scenario', ''),
            ]

            with open(self._path, 'a', newline='') as f:
                csv.writer(f).writerow(row)

            logger.debug("TradeExporter: exported %s pnl=%+.4f", pos.symbol, pos.realized_pnl)

        except Exception as e:
            logger.error("TradeExporter error: %s", e)

    def get_summary(self) -> dict:
        """Быстрая статистика из CSV."""
        try:
            rows = []
            with open(self._path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            if not rows:
                return {}

            pnls = [float(r['pnl_usdt']) for r in rows if r['pnl_usdt']]
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]

            return {
                'total_trades': len(rows),
                'total_pnl': round(sum(pnls), 4),
                'win_rate': round(len(wins) / len(pnls), 4) if pnls else 0,
                'avg_win': round(sum(wins) / len(wins), 4) if wins else 0,
                'avg_loss': round(sum(losses) / len(losses), 4) if losses else 0,
                'profit_factor': round(
                    sum(wins) / abs(sum(losses)), 4
                ) if losses and sum(losses) != 0 else 0,
                'best_trade': round(max(pnls), 4) if pnls else 0,
                'worst_trade': round(min(pnls), 4) if pnls else 0,
            }
        except Exception as e:
            logger.error("TradeExporter summary error: %s", e)
            return {}
