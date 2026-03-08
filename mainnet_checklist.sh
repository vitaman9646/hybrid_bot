#!/bin/bash
# mainnet_checklist.sh — проверки перед переходом на mainnet
# Запуск: bash mainnet_checklist.sh

cd /root/hybrid_bot
source venv/bin/activate

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
PASS=0; WARN=0; FAIL=0

ok()   { echo -e "  ${GREEN}✓${NC} $1"; ((PASS++)); }
warn() { echo -e "  ${YELLOW}⚠${NC}  $1"; ((WARN++)); }
fail() { echo -e "  ${RED}✗${NC} $1"; ((FAIL++)); }

echo "=========================================="
echo "  MAINNET CHECKLIST — $(date '+%Y-%m-%d %H:%M')"
echo "=========================================="

echo ""
echo "[ 1. Конфигурация ]"

TESTNET=$(python3 -c "import yaml; c=yaml.safe_load(open('config/settings.yaml')); print(c['exchange']['testnet'])" 2>/dev/null)
if [ "$TESTNET" = "False" ] || [ "$TESTNET" = "false" ]; then
    ok "testnet: false"
else
    fail "testnet: $TESTNET — поменяйте на false в config/settings.yaml"
fi

API_KEY=$(python3 -c "import yaml; c=yaml.safe_load(open('config/settings.yaml')); print(c['exchange']['api_key'])" 2>/dev/null)
API_LEN=${#API_KEY}
if [ $API_LEN -gt 15 ]; then
    ok "API key присутствует (длина $API_LEN)"
else
    fail "API key подозрительно короткий — проверьте mainnet ключ"
fi

SIZE=$(python3 -c "import yaml; c=yaml.safe_load(open('config/strategies/hybrid.yaml')); print(c.get('order',{}).get('size_usdt',0))" 2>/dev/null)
if (( $(echo "$SIZE <= 100" | bc -l) )); then ok "order.size_usdt: $SIZE USDT"
elif (( $(echo "$SIZE <= 500" | bc -l) )); then warn "order.size_usdt: $SIZE USDT — для старта рекомендуем 50-100"
else fail "order.size_usdt: $SIZE USDT — слишком большой для старта"; fi

SL=$(python3 -c "import yaml; c=yaml.safe_load(open('config/strategies/hybrid.yaml')); print(c.get('order',{}).get('stop_loss',{}).get('percent','not set'))" 2>/dev/null)
[ "$SL" != "not set" ] && ok "stop_loss.percent: $SL%" || fail "stop_loss не задан — критично"

DAILY=$(python3 -c "import yaml; c=yaml.safe_load(open('config/strategies/hybrid.yaml')); print(c.get('risk',{}).get('daily_loss_limit_usdt','not set'))" 2>/dev/null)
[ "$DAILY" != "not set" ] && ok "daily_loss_limit_usdt: $DAILY USDT" || warn "daily_loss_limit_usdt не задан"

echo ""
echo "[ 2. Данные и оптимизация ]"

if [ -f "data/history.db" ]; then
    SZ=$(du -sh data/history.db | cut -f1)
    SC=$(python3 -c "import sqlite3; db=sqlite3.connect('data/history.db'); print(db.execute('SELECT COUNT(DISTINCT symbol) FROM trades').fetchone()[0])" 2>/dev/null || echo "?")
    ok "history.db: $SZ, $SC символов"
else
    fail "data/history.db не найден"
fi

BEST_COUNT=$(ls data/best_*_sharpe.yaml 2>/dev/null | wc -l)
[ $BEST_COUNT -gt 0 ] \
    && ok "Результаты оптимизации: $BEST_COUNT файлов" \
    || warn "Оптимизация не проведена — запустите run_optimization.sh"

[ -f "config/strategies/hybrid.yaml.bak" ] \
    && ok "Бэкап конфига: hybrid.yaml.bak" \
    || warn "Бэкапа конфига нет — apply_params создаст автоматически"

echo ""
echo "[ 3. Сервис ]"

systemctl list-unit-files | grep -q "hybrid-bot" \
    && ok "systemd unit hybrid-bot существует" \
    || fail "systemd unit не найден"

STATUS=$(systemctl is-active hybrid-bot 2>/dev/null)
[ "$STATUS" = "active" ] && ok "hybrid-bot: active" || warn "hybrid-bot: $STATUS"

ERROR_COUNT=$(journalctl -u hybrid-bot -n 100 --no-pager 2>/dev/null | grep -c "CRITICAL\|ERROR" 2>/dev/null || echo "0")
ERROR_COUNT=$(echo $ERROR_COUNT | tr -dc '0-9')
ERROR_COUNT=${ERROR_COUNT:-0}
if [ "$ERROR_COUNT" = "0" ]; then ok "Нет CRITICAL/ERROR в последних 100 строках"
elif [ "$ERROR_COUNT" -lt 5 ]; then warn "$ERROR_COUNT ошибок — проверьте: journalctl -u hybrid-bot -n 100"
else fail "$ERROR_COUNT ошибок в логе — исправьте перед mainnet"; fi

echo ""
echo "[ 4. Безопасность ]"

PERMS=$(stat -c "%a" config/settings.yaml 2>/dev/null)
[ "$PERMS" = "600" ] || [ "$PERMS" = "640" ] \
    && ok "config/settings.yaml permissions: $PERMS" \
    || warn "Рекомендуем: chmod 600 config/settings.yaml"

MAX_SIZE=$(python3 -c "import yaml; c=yaml.safe_load(open('config/strategies/hybrid.yaml')); print(c.get('risk',{}).get('max_size_usdt','not set'))" 2>/dev/null)
[ "$MAX_SIZE" != "not set" ] && ok "risk.max_size_usdt: $MAX_SIZE USDT" || warn "risk.max_size_usdt не задан"

echo ""
echo "=========================================="
echo -e "  ИТОГ: ${GREEN}✓ $PASS${NC}  ${YELLOW}⚠ $WARN${NC}  ${RED}✗ $FAIL${NC}"
echo "=========================================="

if [ $FAIL -gt 0 ]; then
    echo -e "  ${RED}НЕ ГОТОВ — исправьте ✗ пункты${NC}"
elif [ $WARN -gt 0 ]; then
    echo -e "  ${YELLOW}ПОЧТИ ГОТОВ — разберитесь с ⚠ пунктами${NC}"
else
    echo -e "  ${GREEN}ГОТОВ к mainnet!${NC}"
fi

echo ""
echo "Команды перехода:"
echo "  nano config/settings.yaml        # testnet: false + mainnet ключи"
echo "  bash mainnet_checklist.sh        # перепроверить"
echo "  systemctl restart hybrid-bot"
echo "  journalctl -u hybrid-bot -f"
echo "  /status в Telegram"
echo "=========================================="
