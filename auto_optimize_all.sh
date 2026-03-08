#!/bin/bash
# auto_optimize_all.sh — ждёт докачки, сливает БД, оптимизирует все символы
# Запуск: nohup bash auto_optimize_all.sh > data/auto_optimize.log 2>&1 &
# Мониторинг: tail -f data/auto_optimize.log

cd /root/hybrid_bot
source venv/bin/activate

SYMBOLS=(BTCUSDT ETHUSDT SOLUSDT BNBUSDT XRPUSDT DOGEUSDT ADAUSDT AVAXUSDT DOTUSDT)
FETCH_LOG="data/fetch_retry2.log"
DB="data/history.db"
EXTRA_DB="data/history_extra.db"
DAYS=7
METRIC="sharpe"

send_telegram() {
    local msg="$1"
    python3 - << PYEOF
import yaml, requests
try:
    cfg = yaml.safe_load(open("config/settings.yaml"))
    tg = cfg.get("monitoring", {}).get("telegram", {})
    token = tg.get("bot_token", "")
    chat_id = str(tg.get("chat_id", ""))
    if token and chat_id:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": """$msg""", "parse_mode": "HTML"},
            timeout=10,
        )
except Exception as e:
    print(f"TG error: {e}")
PYEOF
}

echo "=========================================="
echo "auto_optimize_all.sh started: $(date)"
echo "=========================================="

# ------------------------------------------------------------------
# 1. Ждём завершения докачки
# ------------------------------------------------------------------
echo "[$(date '+%H:%M:%S')] Waiting for fetch to complete..."

while true; do
    if ! pgrep -f "fetch_history" > /dev/null 2>&1; then
        echo "[$(date '+%H:%M:%S')] Fetch process done."
        break
    fi
    LAST=$(tail -1 "$FETCH_LOG" 2>/dev/null || echo "...")
    echo "[$(date '+%H:%M:%S')] Still fetching: $LAST"
    sleep 60
done

# ------------------------------------------------------------------
# 2. Сливаем БД
# ------------------------------------------------------------------
echo ""
echo "[$(date '+%H:%M:%S')] Merging databases..."
bash merge_db.sh

echo "[$(date '+%H:%M:%S')] DB after merge:"
python3 - << 'PYEOF'
import sqlite3, time
db = sqlite3.connect("data/history.db")
rows = db.execute("""
    SELECT symbol, COUNT(*) as cnt FROM trades
    GROUP BY symbol ORDER BY cnt DESC
""").fetchall()
print(f"  {'Symbol':<12} {'Trades':>12}")
for sym, cnt in rows:
    print(f"  {sym:<12} {cnt:>12,}")
print(f"\n  Total: {sum(r[1] for r in rows):,}")
db.close()
PYEOF

send_telegram "✅ <b>Data merge complete</b>
Starting optimization for ${#SYMBOLS[@]} symbols..."

# ------------------------------------------------------------------
# 3. Оптимизация всех символов последовательно
# ------------------------------------------------------------------
echo ""
echo "[$(date '+%H:%M:%S')] Starting optimization..."
mkdir -p data/opt_logs

FAILED=()
for SYMBOL in "${SYMBOLS[@]}"; do
    # Пропускаем BTC — уже оптимизирован
    if [ "$SYMBOL" = "BTCUSDT" ] && [ -f "data/best_BTCUSDT_sharpe.yaml" ]; then
        echo "[$(date '+%H:%M:%S')] $SYMBOL — skipping (already done)"
        continue
    fi

    echo "[$(date '+%H:%M:%S')] Optimizing $SYMBOL..."
    LOG="data/opt_logs/${SYMBOL}_auto.log"

    if python3 -m backtester.run_optimization \
        --symbol "$SYMBOL" \
        --days "$DAYS" \
        --metric "$METRIC" \
        --min-trades 5 \
        --fast \
        --db "$DB" > "$LOG" 2>&1; then
        echo "[$(date '+%H:%M:%S')] $SYMBOL — OK"
    else
        echo "[$(date '+%H:%M:%S')] $SYMBOL — FAILED"
        FAILED+=("$SYMBOL")
    fi
done

# ------------------------------------------------------------------
# 4. Итоговая таблица
# ------------------------------------------------------------------
echo ""
echo "=========================================="
echo "[$(date '+%H:%M:%S')] Results:"
echo "=========================================="

REPORT=$(python3 - << 'PYEOF'
import yaml, glob

results = []
for f in glob.glob("data/best_*_sharpe.yaml"):
    try:
        d = yaml.safe_load(open(f))
        results.append(d)
    except:
        pass

results.sort(key=lambda x: x.get("metric_value", 0), reverse=True)

lines = ["?? <b>Optimization Results</b>\n"]
lines.append(f"{'Symbol':<12} {'Sharpe':>7}  {'Win%':>6}  {'PnL':>8}  {'PF':>5}")
lines.append("-" * 45)

for r in results:
    s = r.get("stats", {})
    sharpe = r.get("metric_value", 0)
    icon = "??" if sharpe > 1.0 else "??" if sharpe > 0.5 else "??"
    lines.append(
        f"{icon} {r.get('symbol','?'):<10} "
        f"{sharpe:>7.3f}  "
        f"{s.get('win_rate',0):>6.1%}  "
        f"{s.get('total_pnl',0):>+8.2f}  "
        f"{s.get('profit_factor',0):>5.2f}"
    )

# Лучший символ
if results:
    best = results[0]
    lines.append(f"\n?? Best: {best.get('symbol')} sharpe={best.get('metric_value'):.3f}")
    lines.append(f"Applied params: BTCUSDT (primary)")
    lines.append(f"\nReady: bash mainnet_checklist.sh")

print("\n".join(lines))
PYEOF
)

echo "$REPORT"
send_telegram "$REPORT"

# ------------------------------------------------------------------
# 4b. Применяем лучшие параметры
# ------------------------------------------------------------------
echo "Applying best params..."
cd /root/hybrid_bot
python3 apply_params.py && send_telegram "✅ Best params applied" || send_telegram "⚠️ apply_params.py failed"

# ------------------------------------------------------------------
# 5. Финал
# ------------------------------------------------------------------
echo ""
echo "=========================================="
echo "DONE: $(date)"
[ ${#FAILED[@]} -gt 0 ] && echo "Failed: ${FAILED[*]}"
echo "=========================================="

send_telegram "?? <b>auto_optimize_all.sh complete</b>
$([ ${#FAILED[@]} -gt 0 ] && echo "Failed: ${FAILED[*]}" || echo "All symbols OK")

Next: bash mainnet_checklist.sh"
