"""
Скачивает исторические тики с public.bybit.com и сохраняет в SQLite
Запуск: python3 backtester/download_bybit_history.py --symbols BTCUSDT ETHUSDT --days 30 --db data/history_btc_eth.db
"""
import sqlite3, gzip, csv, io, urllib.request, argparse, time
from datetime import datetime, timedelta, timezone

BASE_URL = "https://public.bybit.com/trading/{symbol}/{symbol}{date}.csv.gz"

def get_dates(days: int) -> list[str]:
    dates = []
    for i in range(1, days + 1):
        d = datetime.now(timezone.utc) - timedelta(days=i)
        dates.append(d.strftime('%Y-%m-%d'))
    return dates

def download_day(symbol: str, date: str, db_path: str) -> int:
    url = BASE_URL.format(symbol=symbol, date=date)
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = resp.read()
    except Exception as e:
        print(f"  SKIP {date}: {e}")
        return 0

    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL, price REAL NOT NULL,
        qty REAL NOT NULL, side TEXT NOT NULL, ts REAL NOT NULL)''')

    with gzip.open(io.BytesIO(data), 'rt') as f:
        reader = csv.DictReader(f)
        rows = [(row['symbol'], float(row['price']), float(row['size']),
                 row['side'], float(row['timestamp'])) for row in reader]

    conn.executemany('INSERT INTO trades (symbol,price,qty,side,ts) VALUES (?,?,?,?,?)', rows)
    conn.commit()
    conn.close()
    return len(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbols', nargs='+', default=['BTCUSDT', 'ETHUSDT'])
    parser.add_argument('--days', type=int, default=30)
    parser.add_argument('--db', default='data/history_btc_eth.db')
    args = parser.parse_args()

    dates = get_dates(args.days)
    for symbol in args.symbols:
        total = 0
        for date in dates:
            n = download_day(symbol, date, args.db)
            total += n
            print(f"{symbol} {date}: {n:,} тиков (total={total:,})")
        print(f"=== {symbol}: {total:,} total ===")

if __name__ == '__main__':
    main()
