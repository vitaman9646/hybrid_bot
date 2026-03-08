#!/bin/bash
# backup.sh — ежедневный backup конфига и статистики
# Cron: 0 3 * * * cd /root/hybrid_bot && bash backup.sh

cd /root/hybrid_bot
BACKUP_DIR="data/backups/$(date +%Y-%m-%d)"
mkdir -p "$BACKUP_DIR"

# Конфиги
cp config/settings.yaml "$BACKUP_DIR/settings.yaml"
cp config/strategies/hybrid.yaml "$BACKUP_DIR/hybrid.yaml"

# Результаты оптимизации
cp data/best_*.yaml "$BACKUP_DIR/" 2>/dev/null

# CSV сделок
[ -f data/trades.csv ] && cp data/trades.csv "$BACKUP_DIR/trades.csv"

# Статистика БД
python3 backup_stats.py "$BACKUP_DIR" 2>/dev/null

# Удаляем бэкапы старше 30 дней
find data/backups -type d -mtime +30 -exec rm -rf {} + 2>/dev/null

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backup done: $BACKUP_DIR"
ls -lh "$BACKUP_DIR"
