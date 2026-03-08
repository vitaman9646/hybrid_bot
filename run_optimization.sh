#!/bin/bash
# run_optimization.sh — ждёт загрузки, оптимизирует все символы параллельно, применяет параметры
# Запуск: nohup bash run_optimization.sh > data/optimization.log 2>&1 &
# Мониторинг: tail -f data/optimization.log

cd /root/hybrid_bot
source venv/bin/activate


# ------------------------------------------------------------------
# Telegram уведомление
# ------------------------------------------------------------------
send_telegram() {
    local msg="$1"
    python3 - << INNER
import yaml, requests
try:
    cfg = yaml.safe_load(open("config/settings.yaml"))
    tg = cfg.get("monitoring", {}).get("telegram", {})
    token = tg.get("bot_token", "")
    chat_id = tg.get("chat_id", "")
    if token and chat_id:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": """$msg""", "parse_mode": "HTML"},
            timeout=10,
        )
except Exception as e:
    print(f"TG notify error: {e}")
INNER
}

DB="data/history.db"
DAYS=7
METRIC="sharpe"
MIN_TRADES=20
FETCH_LOG="data/fetch_all.log"
PRIMARY="BTCUSDT"
SYMBOLS=(BTCUSDT ETHUSDT SOLUSDT BNBUSDT XRPUSDT DOGEUSDT ADAUSDT AVAXUSDT DOTUSDT MATICUSDT)

echo "=========================================="
echo "run_optimization.sh started: $(date)"
echo "Symbols: ${SYMBOLS[*]}"
echo "Metric:  $METRIC | min_trades=$MIN_TRADES"
echo "=========================================="

# 1. Ждём завершения загрузки
echo ""
echo "[$(date '+%H:%M:%S')] Step 1: Waiting for data fetch..."
send_telegram "⏳ <b>Optimization started</b>
Symbols: ${SYMBOLS[*]}
Metric: $METRIC | min_trades=$MIN_TRADES"
while true; do
    if grep -q "fetch_all.sh finished\|All symbols OK" "$FETCH_LOG" 2>/dev/null; then
        echo "[$(date '+%H:%M:%S')] Fetch complete!"
        break
    fi
    if ! pgrep -f "fetch_history\|fetch_all" > /dev/null 2>&1; then
        echo "[$(date '+%H:%M:%S')] No fetch process running — continuing."
        break
    fi
    LAST=$(tail -1 "$FETCH_LOG" 2>/dev/null || echo "...")
    echo "[$(date '+%H:%M:%S')] Still fetching: $LAST"
    sleep 30
done

# Статус БД
echo ""
echo "[$(date '+%H:%M:%S')] DB contents:"
python3 - << 'PYEOF'
import sqlite3, time
try:
    db = sqlite3.connect("data/history.db")
    rows = db.execute("""
        SELECT symbol, COUNT(*) as cnt, MIN(timestamp), MAX(timestamp)
        FROM trades GROUP BY symbol ORDER BY cnt DESC
    """).fetchall()
    print(f"  {'Symbol':<12} {'Trades':>12}  {'From':<12}  {'To':<12}")
    print(f"  {'-'*52}")
    for sym, cnt, ts_min, ts_max in rows:
        print(f"  {sym:<12} {cnt:>12,}  {time.strftime('%Y-%m-%d', time.gmtime(ts_min))}  {time.strftime('%Y-%m-%d', time.gmtime(ts_max))}")
    print(f"\n  Total: {len(rows)} symbols, {sum(r[1] for r in rows):,} trades")
    db.close()
except Exception as e:
    print(f"  DB error: {e}")
PYEOF

# 2. Параллельная оптимизация
echo ""
echo "[$(date '+%H:%M:%S')] Step 2: Running optimization (parallel)..."
echo "------------------------------------------"
mkdir -p data/opt_logs

PIDS=()
for SYMBOL in "${SYMBOLS[@]}"; do
    LOG="data/opt_logs/${SYMBOL}.log"
    echo "[$(date '+%H:%M:%S')] Starting $SYMBOL..."
    python3 -m backtester.run_optimization \
        --symbol "$SYMBOL" \
        --days "$DAYS" \
        --metric "$METRIC" \
        --min-trades "$MIN_TRADES" \
        --fast \
        --db "$DB" > "$LOG" 2>&1 &
    PIDS+=($!)
done

echo "[$(date '+%H:%M:%S')] All ${#SYMBOLS[@]} running (PIDs: ${PIDS[*]})"
echo "[$(date '+%H:%M:%S')] Waiting..."

FAILED_OPT=()
for i in "${!PIDS[@]}"; do
    PID=${PIDS[$i]}
    SYM=${SYMBOLS[$i]}
    if wait "$PID"; then
        echo "[$(date '+%H:%M:%S')] $SYM — OK"
    else
        echo "[$(date '+%H:%M:%S')] $SYM — FAILED"
        FAILED_OPT+=("$SYM")
    fi
done

# 3. Итоги
echo ""
echo "=========================================="
echo "[$(date '+%H:%M:%S')] Step 3: Results"
echo "=========================================="
python3 - << 'PYEOF'
import yaml, glob
results = []
for f in glob.glob("data/best_*_sharpe.yaml"):
    try:
        d = yaml.safe_load(open(f))
        results.append(d)
    except:
        pass
if not results:
    print("  No results found!")
else:
    results.sort(key=lambda x: x.get('metric_value', 0), reverse=True)
    print(f"  {'Symbol':<12} {'Sharpe':>8}  {'Trades':>7}  {'Win%':>6}  {'PnL':>10}  {'PF':>6}")
    print(f"  {'-'*58}")
    for r in results:
        s = r.get('stats', {})
        print(
            f"  {r.get('symbol','?'):<12} "
            f"{r.get('metric_value', 0):>8.3f}  "
            f"{s.get('trades', 0):>7}  "
            f"{s.get('win_rate', 0):>6.1%}  "
            f"{s.get('total_pnl', 0):>+10.2f}  "
            f"{s.get('profit_factor', 0):>6.2f}"
        )
    print(f"\n  Best: {results[0].get('symbol')} sharpe={results[0].get('metric_value'):.3f}")
PYEOF

# Отправляем результаты в Telegram
BEST_SHARPE=$(python3 -c "
import yaml, glob
results = []
for f in glob.glob('data/best_*_sharpe.yaml'):
    try:
        d = yaml.safe_load(open(f))
        results.append(d)
    except: pass
results.sort(key=lambda x: x.get('metric_value', 0), reverse=True)
if results:
    r = results[0]
    s = r.get('stats', {})
    print(f\"✅ <b>Optimization complete!</b>\\n\\nBest: {r.get('symbol')} sharpe={r.get('metric_value'):.3f}\\nTrades: {s.get('trades')} | Win: {s.get('win_rate'):.1%}\\nPnL: {s.get('total_pnl'):+.2f} USDT\\n\\nTop results:\\n\" + '\\n'.join(
        f\"  {rr.get('symbol')}: sharpe={rr.get('metric_value'):.3f} trades={rr.get('stats',{}).get('trades',0)}\"
        for rr in results[:5]
    ) + f\"\\n\\nReady: bash mainnet_checklist.sh\")
else:
    print('⚠️ No optimization results')
" 2>/dev/null || echo "⚠️ Could not read results")
send_telegram "$BEST_SHARPE"

# 4. Применяем параметры PRIMARY символа
echo ""
echo "[$(date '+%H:%M:%S')] Step 4: Applying best params for $PRIMARY..."
BEST_FILE="data/best_${PRIMARY}_${METRIC}.yaml"
if [ -f "$BEST_FILE" ]; then
    echo "--- Dry run ---"
    python3 -m backtester.apply_params --file "$BEST_FILE" --dry-run
    echo "--- Applying ---"
    python3 -m backtester.apply_params --file "$BEST_FILE"
    echo "[$(date '+%H:%M:%S')] Done. Backup: config/strategies/hybrid.yaml.bak"
else
    echo "[$(date '+%H:%M:%S')] WARNING: $BEST_FILE not found — skipping"
fi

# 5. Итог
echo ""
echo "=========================================="
echo "DONE: $(date)"
[ ${#FAILED_OPT[@]} -gt 0 ] && echo "Failed: ${FAILED_OPT[*]}"
echo ""
send_telegram "?? <b>run_optimization.sh finished</b>
Applied params: $PRIMARY
Ready for mainnet: bash mainnet_checklist.sh"

echo "Next steps:"
echo "  1. bash mainnet_checklist.sh"
echo "  2. nano config/settings.yaml   # testnet: false + mainnet keys"
echo "  3. systemctl restart hybrid-bot"
echo "  4. journalctl -u hybrid-bot -f"
echo "=========================================="
