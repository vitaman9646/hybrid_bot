"""
backtester/market_saver.py — запись рыночных данных в SQLite

Использование:
    saver = MarketSaver("data/market.db")
    saver.save_trade(trade)
    saver.save_orderbook_snapshot(symbol, bids, asks, ts)
"""

from __future__ import annotations

import sqlite3
import logging
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    symbol: str
    price: float
    qty: float
    side: str           # 'Buy' / 'Sell'
    timestamp: float    # unix seconds


@dataclass
class OrderBookSnapshot:
    symbol: str
    timestamp: float
    bids: list[tuple[float, float]]   # [(price, qty), ...]
    asks: list[tuple[float, float]]


class MarketSaver:
    """
    Сохраняет тики и снапшоты стакана в SQLite.
    Потокобезопасен — использует check_same_thread=False.
    """

    def __init__(self, db_path: str = "data/market.db"):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._db_path),
            check_same_thread=False,
            timeout=30,
            isolation_level=None,
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()
        self._buf_trades: list[tuple] = []
        self._buf_size = 500    # батч-запись
        logger.info("MarketSaver initialized: %s", self._db_path)

    def _create_tables(self):
        cur = self._conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT    NOT NULL,
                price     REAL    NOT NULL,
                qty       REAL    NOT NULL,
                side      TEXT    NOT NULL,
                ts        REAL    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts
                ON trades(symbol, ts);

            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT NOT NULL,
                ts        REAL NOT NULL,
                bids      TEXT NOT NULL,
                asks      TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ob_symbol_ts
                ON orderbook_snapshots(symbol, ts);
        """)
        self._safe_commit()

    # ------------------------------------------------------------------
    # Запись
    # ------------------------------------------------------------------

    def save_trade(self, trade) -> None:
        """Принимает TradeData из data_feed."""
        self._buf_trades.append((
            trade.symbol,
            trade.price,
            trade.qty,
            trade.side,
            trade.timestamp,
        ))
        if len(self._buf_trades) >= self._buf_size:
            self._flush_trades()

    def save_trade_record(self, record: TradeRecord) -> None:
        self._buf_trades.append((
            record.symbol,
            record.price,
            record.qty,
            record.side,
            record.timestamp,
        ))
        if len(self._buf_trades) >= self._buf_size:
            self._flush_trades()

    def save_orderbook_snapshot(
        self,
        symbol: str,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        timestamp: Optional[float] = None,
    ) -> None:
        import json
        ts = timestamp or time.time()
        cur = self._conn.cursor()
        cur.execute(
            "INSERT INTO orderbook_snapshots (symbol, ts, bids, asks) VALUES (?,?,?,?)",
            (symbol, ts, json.dumps(bids[:20]), json.dumps(asks[:20])),
        )
        self._safe_commit()

    def flush(self) -> None:
        """Принудительный сброс буфера."""
        self._flush_trades()

    def _safe_commit(self):
        try:
            self._conn.execute("COMMIT")
        except sqlite3.OperationalError:
            pass  # no active transaction

    def _flush_trades(self):
        if not self._buf_trades:
            return
        cur = self._conn.cursor()
        try:
            self._conn.execute("BEGIN")
            cur.executemany(
                "INSERT INTO trades (symbol, price, qty, side, ts) VALUES (?,?,?,?,?)",
                self._buf_trades,
            )
            self._safe_commit()
            self._buf_trades.clear()
        except sqlite3.OperationalError as e:
            logger.warning("flush_trades error: %s", e)
            try:
                self._conn.execute("ROLLBACK")
            except:
                pass

    # ------------------------------------------------------------------
    # Чтение
    # ------------------------------------------------------------------

    def get_trades(
        self,
        symbol: str,
        ts_from: float,
        ts_to: float,
    ) -> list[TradeRecord]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT symbol, price, qty, side, ts FROM trades "
            "WHERE symbol=? AND ts>=? AND ts<=? ORDER BY ts ASC",
            (symbol, ts_from, ts_to),
        )
        return [
            TradeRecord(symbol=r[0], price=r[1], qty=r[2], side=r[3], timestamp=r[4])
            for r in cur.fetchall()
        ]

    def get_symbols(self) -> list[str]:
        cur = self._conn.cursor()
        cur.execute("SELECT DISTINCT symbol FROM trades ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]

    def get_time_range(self, symbol: str) -> tuple[float, float]:
        """Возвращает (ts_min, ts_max) для символа."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT MIN(ts), MAX(ts) FROM trades WHERE symbol=?",
            (symbol,),
        )
        row = cur.fetchone()
        return (row[0] or 0.0, row[1] or 0.0)

    def get_trade_count(self, symbol: str) -> int:
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trades WHERE symbol=?", (symbol,))
        return cur.fetchone()[0]

    def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """Удаляет тики старше days_to_keep дней. Возвращает кол-во удалённых."""
        import time
        cutoff = time.time() - days_to_keep * 86400
        cur = self._conn.cursor()
        cur.execute("DELETE FROM trades WHERE ts < ?", (cutoff,))
        deleted = cur.rowcount
        cur.execute("DELETE FROM orderbook_snapshots WHERE ts < ?", (cutoff,))
        self._safe_commit()
        if deleted > 0:
            self._conn.execute("VACUUM")
            logger.info("MarketSaver cleanup: removed %d records older than %d days", deleted, days_to_keep)
        return deleted

    def close(self):
        self._flush_trades()
        self._conn.close()

    def iter_trades(
        self,
        symbol: str,
        ts_from: float,
        ts_to: float,
        chunk_size: int = 50000,
        sample_every: int = 1,
    ):
        """Итератор тиков чанками — не грузит всё в память.
        sample_every=10 берёт каждый 10-й тик — ускоряет в 10x.
        """
        cur = self._conn.cursor()
        cur.execute(
            "SELECT symbol, price, qty, side, ts FROM trades "
            "WHERE symbol=? AND ts>=? AND ts<=? ORDER BY ts ASC",
            (symbol, ts_from, ts_to),
        )
        i = 0
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            for r in rows:
                if i % sample_every == 0:
                    yield TradeRecord(
                        symbol=r[0], price=r[1], qty=r[2],
                        side=r[3], timestamp=r[4],
                    )
                i += 1

    def get_trade_count_period(self, symbol: str, ts_from: float, ts_to: float) -> int:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM trades WHERE symbol=? AND ts>=? AND ts<=?",
            (symbol, ts_from, ts_to),
        )
        return cur.fetchone()[0]
