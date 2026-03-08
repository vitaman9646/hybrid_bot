#!/bin/bash
# merge_db.sh — объединяет history_extra.db в history.db
# Запуск: bash merge_db.sh

cd /root/hybrid_bot
source venv/bin/activate

echo "Merging history_extra.db -> history.db..."

python3 - << 'PYEOF'
import sqlite3, time

src = sqlite3.connect("data/history_extra.db")
dst = sqlite3.connect("data/history.db")

symbols = src.execute("SELECT DISTINCT symbol FROM trades").fetchall()
print(f"Symbols in extra db: {[s[0] for s in symbols]}")

total = 0
for (symbol,) in symbols:
    # Проверяем что уже есть в dst
    existing = dst.execute(
        "SELECT COUNT(*) FROM trades WHERE symbol=?", (symbol,)
    ).fetchone()[0]
    print(f"{symbol}: existing={existing:,}")

    # Копируем только новые записи
    rows = src.execute(
        "SELECT symbol, price, qty, side, ts FROM trades WHERE symbol=?",
        (symbol,)
    ).fetchall()

    dst.executemany(
        "INSERT OR IGNORE INTO trades (symbol, price, qty, side, ts) VALUES (?,?,?,?,?)",
        rows
    )
    dst.commit()
    inserted = dst.execute(
        "SELECT COUNT(*) FROM trades WHERE symbol=?", (symbol,)
    ).fetchone()[0] - existing
    print(f"{symbol}: inserted={inserted:,}")
    total += inserted

print(f"\nTotal inserted: {total:,}")

# Итог
rows = dst.execute("""
    SELECT symbol, COUNT(*) as cnt FROM trades
    GROUP BY symbol ORDER BY cnt DESC
""").fetchall()
print(f"\nFinal DB:")
print(f"  {'Symbol':<12} {'Trades':>12}")
for sym, cnt in rows:
    print(f"  {sym:<12} {cnt:>12,}")

src.close()
dst.close()
PYEOF

echo "Done!"
