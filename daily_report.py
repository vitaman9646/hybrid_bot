#!/usr/bin/env python3
"""
daily_report.py — ежедневный отчёт в Telegram
Запуск: python3 daily_report.py
Cron:  0 9 * * * cd /root/hybrid_bot && venv/bin/python3 daily_report.py
"""
import re
import yaml
import time
import requests
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

LOG_FILE = "logs/bot.log"
CONFIG   = "config/settings.yaml"

def send_telegram(token, chat_id, text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        print(f"TG error: {e}")

def parse_day(log_path, hours=24):
    since = time.time() - hours * 3600
    lines = Path(log_path).read_text(errors="ignore").splitlines()

    trades = []
    signals = 0
    filtered = defaultdict(int)
    errors = 0
    sl_failed = 0

    for line in lines:
        # Парсим время
        try:
            ts = datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S").timestamp()
        except:
            continue
        if ts < since:
            continue

        if "Position CLOSED" in line or "closed on exchange" in line:
            pnl = 0.0
            m = re.search(r"pnl=([+-]?\d+\.\d+)", line)
            if m:
                pnl = float(m.group(1))
            sym = "?"
            # Символ идёт после уровня лога: [INFO] core.xxx: [BTCUSDT]
            m2 = re.search(r"position_manager: \[(\w+)\]", line)
            if m2:
                sym = m2.group(1)
            trades.append({"symbol": sym, "pnl": pnl})

        elif "SIGNAL FILTERED" in line:
            m = re.search(r"FILTERED \[(\w+)\]: (\w+)", line)
            if m:
                filtered[m.group(2)] += 1

        elif "AGGREGATED SIGNAL" in line or "SIGNAL [" in line:
            signals += 1

        elif "[ERROR]" in line:
            errors += 1

        elif "SL placement failed" in line:
            sl_failed += 1

    return dict(trades=trades, signals=signals, filtered=filtered,
                errors=errors, sl_failed=sl_failed)

def format_report(data, balance=None, weekly=False):
    trades = data["trades"]
    total  = len(trades)
    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    pnl    = sum(t["pnl"] for t in trades)
    wr     = len(wins) / total * 100 if total else 0

    gross_p = sum(t["pnl"] for t in wins)
    gross_l = abs(sum(t["pnl"] for t in losses))
    pf      = gross_p / gross_l if gross_l > 0 else float("inf")

    # Топ символы по кол-ву сделок
    sym_count = defaultdict(int)
    sym_pnl   = defaultdict(float)
    for t in trades:
        sym_count[t["symbol"]] += 1
        sym_pnl[t["symbol"]]   += t["pnl"]
    top_syms = sorted(sym_count, key=lambda s: sym_count[s], reverse=True)[:5]

    # Топ причины фильтрации
    top_filters = sorted(data["filtered"].items(), key=lambda x: x[1], reverse=True)[:3]

    hours_back = 168 if weekly else 24
    date_str = (datetime.now() - timedelta(hours=hours_back)).strftime("%Y-%m-%d")
    bal_str  = f"{balance:.2f} USDT" if balance else "N/A"

    title = "?? Weekly Report" if weekly else "?? Daily Report"
    lines = [
        f"{title} — {date_str}",
        f"Balance: <b>{bal_str}</b>",
        "",
        f"<b>Trades:</b> {total} (✅ {len(wins)} / ❌ {len(losses)})",
        f"Win rate:      {wr:.1f}%",
        f"Total PnL:     {pnl:+.2f} USDT",
        f"Profit factor: {pf:.2f}" if pf != float("inf") else "Profit factor: ∞",
        "",
        f"<b>Signals:</b> {data['signals']} total",
        f"Filtered:      {sum(data['filtered'].values())}",
    ]

    if top_filters:
        lines.append("Top filters: " + ", ".join(f"{k}({v})" for k, v in top_filters))

    if top_syms:
        lines.append("")
        lines.append("<b>Top symbols:</b>")
        for s in top_syms:
            lines.append(f"  {s:<12} {sym_count[s]} trades  {sym_pnl[s]:+.2f} USDT")

    if data["errors"] > 0 or data["sl_failed"] > 0:
        lines.append("")
        lines.append(f"⚠️ Errors: {data['errors']} | SL failed: {data['sl_failed']}")

    return "\n".join(lines)

def main():
    cfg    = yaml.safe_load(open(CONFIG))
    tg     = cfg.get("monitoring", {}).get("telegram", {})
    token  = tg.get("bot_token", "")
    chat_id = str(tg.get("chat_id", ""))

    if not token or not chat_id:
        print("Telegram not configured")
        return

    data   = parse_day(LOG_FILE, hours=24)

    # Читаем последний баланс из лога
    balance = None
    try:
        lines = Path(LOG_FILE).read_text(errors="ignore").splitlines()
        for line in reversed(lines):
            m = re.search(r"balance[=: ]+(\d+\.\d+)", line, re.IGNORECASE)
            if m:
                balance = float(m.group(1))
                break
    except:
        pass

    report = format_report(data, balance=balance)

    print(report)
    send_telegram(token, chat_id, report)
    print("Sent!")

if __name__ == "__main__":
    main()
