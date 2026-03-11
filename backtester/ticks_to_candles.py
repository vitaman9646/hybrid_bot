"""
Агрегирует тиковые данные из trades таблицы в OHLCV свечи.
Сохраняет в отдельную DB для candle_backtest.py

Использование:
    python3 backtester/ticks_to_candles.py --src ~/history_btc_eth.db --dst ~/klines_btc_eth.db --interval 60
"""
import sqlite3
import argparse
import time
from datetime import datetime, timezone

def create_klines_table(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS klines (
        symbol TEXT NOT NULL,
        ts INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        turnover REAL NOT NULL,
        PRIMARY KEY (symbol, ts)
    )''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_klines_sym_ts ON klines(symbol, ts)')
    conn.commit()

def aggregate(src_path: str, dst_path: str, interval_s: int, symbols: list):
    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(dst_path)
    create_klines_table(dst)

    for symbol in symbols:
        print(f"Агрегируем {symbol}...")
        t0 = time.time()

        # Получаем диапазон
        row = src.execute(
            'SELECT MIN(ts), MAX(ts), COUNT(*) FROM trades WHERE symbol=?', (symbol,)
        ).fetchone()
        if not row or not row[0]:
            print(f"  {symbol}: нет данных")
            continue

        ts_min, ts_max, total = row
        print(f"  {symbol}: {total:,} тиков, {datetime.fromtimestamp(ts_min, tz=timezone.utc).strftime('%Y-%m-%d')} → {datetime.fromtimestamp(ts_max, tz=timezone.utc).strftime('%Y-%m-%d')}")

        # Агрегируем батчами по дням
        candles = []
        cur_ts = (int(ts_min) // interval_s) * interval_s
        end_ts = int(ts_max) + interval_s

        # Используем SQL агрегацию — быстро
        rows = src.execute('''
            SELECT
                (CAST(ts AS INTEGER) / ?) * ? AS bucket,
                MIN(price) as low,
                MAX(price) as high,
                SUM(qty) as volume,
                SUM(qty * price) as turnover,
                COUNT(*) as cnt
            FROM trades
            WHERE symbol = ?
            GROUP BY bucket
            ORDER BY bucket
        ''', (interval_s, interval_s, symbol)).fetchall()

        # Для open/close нужны первый/последний тик в каждом bucket
        # Делаем отдельный запрос
        oc_rows = src.execute('''
            SELECT bucket, price, rn FROM (
                SELECT
                    (CAST(ts AS INTEGER) / ?) * ? AS bucket,
                    price,
                    ROW_NUMBER() OVER (PARTITION BY (CAST(ts AS INTEGER) / ?) ORDER BY ts ASC) as rn
                FROM trades WHERE symbol = ?
            ) WHERE rn = 1
            UNION ALL
            SELECT bucket, price, -1 as rn FROM (
                SELECT
                    (CAST(ts AS INTEGER) / ?) * ? AS bucket,
                    price,
                    ROW_NUMBER() OVER (PARTITION BY (CAST(ts AS INTEGER) / ?) ORDER BY ts DESC) as rn
                FROM trades WHERE symbol = ?
            ) WHERE rn = 1
        ''', (interval_s, interval_s, interval_s, symbol,
              interval_s, interval_s, interval_s, symbol)).fetchall()

        open_prices = {r[0]: r[1] for r in oc_rows if r[2] == 1}
        close_prices = {r[0]: r[1] for r in oc_rows if r[2] == -1}

        insert_rows = []
        for bucket, low, high, volume, turnover, cnt in rows:
            open_p = open_prices.get(bucket, low)
            close_p = close_prices.get(bucket, high)
            insert_rows.append((symbol, int(bucket), open_p, high, low, close_p, volume, turnover))

        dst.executemany(
            'INSERT OR REPLACE INTO klines (symbol,ts,open,high,low,close,volume,turnover) VALUES (?,?,?,?,?,?,?,?)',
            insert_rows
        )
        dst.commit()
        elapsed = time.time() - t0
        print(f"  {symbol}: {len(insert_rows):,} свечей за {elapsed:.1f}s")

    src.close()
    dst.close()
    print("Готово!")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', default='/home/vitaman/history_btc_eth.db')
    parser.add_argument('--dst', default='/home/vitaman/klines_btc_eth.db')
    parser.add_argument('--interval', type=int, default=60, help='Интервал свечи в секундах')
    parser.add_argument('--symbols', nargs='+', default=['BTCUSDT', 'ETHUSDT'])
    args = parser.parse_args()

    print(f"Агрегация тиков → свечи {args.interval}s")
    aggregate(args.src, args.dst, args.interval, args.symbols)

if __name__ == '__main__':
    main()
