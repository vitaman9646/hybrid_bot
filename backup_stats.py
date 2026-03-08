import sqlite3, json, sys
backup_dir = sys.argv[1] if len(sys.argv) > 1 else "data/backups"
try:
    db = sqlite3.connect("data/history.db")
    rows = db.execute("""
        SELECT symbol, COUNT(*) as cnt, MIN(ts) as ts_min, MAX(ts) as ts_max
        FROM trades GROUP BY symbol
    """).fetchall()
    stats = {r[0]: {"trades": r[1], "from": r[2], "to": r[3]} for r in rows}
    with open(f"{backup_dir}/db_stats.json", "w") as f:
        json.dump(stats, f, indent=2)
    db.close()
    print(f"DB stats saved: {len(stats)} symbols")
except Exception as e:
    print(f"DB stats error: {e}")
