#!/bin/bash
set -e
cd /root/hybrid_bot
source venv/bin/activate

DB="data/history.db"
DAYS=7
LOG="data/fetch.log"

SYMBOLS=(ETHUSDT SOLUSDT BNBUSDT XRPUSDT DOGEUSDT ADAUSDT AVAXUSDT DOTUSDT MATICUSDT)

echo "=========================================="
echo "fetch_all.sh started: $(date)"
echo "Symbols to fetch: ${SYMBOLS[*]}"
echo "=========================================="

echo "[$(date '+%H:%M:%S')] Waiting for current BTC fetch to finish..."
while true; do
    if ! pgrep -f "fetch_history" > /dev/null 2>&1; then
        echo "[$(date '+%H:%M:%S')] BTC fetch done."
        break
    fi
    LAST_LINE=$(tail -1 "$LOG" 2>/dev/null || echo "")
    echo "[$(date '+%H:%M:%S')] BTC still loading... $LAST_LINE"
    sleep 30
done

FAILED=()
for SYMBOL in "${SYMBOLS[@]}"; do
    echo "=========================================="
    echo "[$(date '+%H:%M:%S')] Fetching $SYMBOL..."
    if python3 -m backtester.fetch_history --symbols "$SYMBOL" --days "$DAYS" --db "$DB"; then
        echo "[$(date '+%H:%M:%S')] $SYMBOL — OK"
    else
        echo "[$(date '+%H:%M:%S')] $SYMBOL — FAILED"
        FAILED+=("$SYMBOL")
    fi
    sleep 2
done

echo "=========================================="
echo "fetch_all.sh finished: $(date)"
if [ ${#FAILED[@]} -gt 0 ]; then
    echo "FAILED: ${FAILED[*]}"
else
    echo "All symbols OK!"
fi
