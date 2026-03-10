#!/bin/bash
cd /root/hybrid_bot
source venv/bin/activate
while true; do
    echo "$(date) Starting bot..." >> /tmp/bot_restarts.log
    python3 main.py >> /tmp/bot.log 2>&1
    echo "$(date) Bot exited, restarting in 10s..." >> /tmp/bot_restarts.log
    sleep 10
done
