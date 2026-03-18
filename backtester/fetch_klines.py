"""
backtester/fetch_klines.py v2 — скачка свечей с Bybit (от старых к новым)
"""
import argparse
import sqlite3
import time
import logging
from datetime import datetime
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
BASE_URL = "https://api.bybit.com/v5/market/kline"

def fetch_klines(symbol, interval, start_ms, end_ms):
    params = {"category": "linear", "symbol": symbol, "interval": interval,
              "start": start_ms, "end": end_ms, "limit": 200}
    r = requests.get(BASE_URL, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    if data.get("retCode") != 0:
        raise ValueError(f"Bybit error: {data}")
    return data["result"]["list"]

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS klines (
        symbol TEXT, interval TEXT, ts INTEGER,
        open REAL, high REAL, low REAL, close REAL, volume REAL, turnover REAL,
        PRIMARY KEY (symbol, interval, ts))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_ts ON klines(symbol, interval, ts)")
    conn.commit()
    return conn

def get_last_ts(conn, symbol, interval):
    row = conn.execute("SELECT MAX(ts) FROM klines WHERE symbol=? AND interval=?",
                       (symbol, interval)).fetchone()
    return row[0] if row[0] else 0

def fetch_symbol(conn, symbol, interval, days):
    now_ms = int(time.time() * 1000)
    last_ts = get_last_ts(conn, symbol, interval)
    start_ms = last_ts + 1 if last_ts > 0 else now_ms - days * 24 * 3600 * 1000

    logger.info(f"{symbol}: from {datetime.fromtimestamp(start_ms/1000)} ({days}d)")
    interval_ms = int(interval) * 60 * 1000
    total = 0

    while start_ms < now_ms - interval_ms:
        end_ms = min(start_ms + 200 * interval_ms, now_ms)
        try:
            candles = fetch_klines(symbol, interval, start_ms, end_ms)
        except Exception as e:
            logger.error(f"{symbol}: {e}, retry 5s")
            time.sleep(5)
            continue

        if not candles:
            start_ms = end_ms
            continue

        # Bybit возвращает от новых к старым — разворачиваем
        candles = sorted(candles, key=lambda c: int(c[0]))

        rows = [(symbol, interval, int(c[0]), float(c[1]), float(c[2]),
                 float(c[3]), float(c[4]), float(c[5]), float(c[6])) for c in candles]
        conn.executemany("INSERT OR IGNORE INTO klines VALUES (?,?,?,?,?,?,?,?,?)", rows)
        conn.commit()
        total += len(rows)

        # Двигаемся вперёд
        start_ms = int(candles[-1][0]) + interval_ms
        logger.info(f"{symbol}: {datetime.fromtimestamp(start_ms/1000)} +{len(rows)} (total={total:,})")
        time.sleep(0.1)

    logger.info(f"{symbol}: DONE {total:,} candles")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT"])
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--interval", default="1")
    parser.add_argument("--db", default="data/klines.db")
    args = parser.parse_args()
    conn = init_db(args.db)
    for symbol in args.symbols:
        fetch_symbol(conn, symbol, args.interval, args.days)
    conn.close()
    logger.info("All done!")

if __name__ == "__main__":
    main()
