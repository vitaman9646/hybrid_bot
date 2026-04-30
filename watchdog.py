#!/usr/bin/env python3
"""
Watchdog для Hybrid Trend Impulse Bot
Мониторит состояние бота и автоматически исправляет проблемы
"""
import subprocess, sqlite3, re, time, requests, yaml
from datetime import datetime
from pathlib import Path

# ─── Конфиг ───────────────────────────────────────────────────────────────────
BOT_SERVICE    = "hybrid-bot.service"
LOG_FILE       = "/root/hybrid_bot/logs/bot.log"
DB_FILE        = "/root/hybrid_bot/data/signal_features.db"
SETTINGS_FILE  = "/root/hybrid_bot/config/settings.yaml"
DUMP_DIR       = Path("/root/hybrid_bot/logs/watchdog_dumps")
TG_TOKEN       = "8741859170:AAExdEaMLBYA5FotU2RFIgw9rMxRucYNb_I"
TG_CHAT        = "1066756284"

NO_TRADE_HOURS  = 2     # алерт если нет сделок N часов
DEAD_PCT_LIMIT  = 0.80  # алерт если >80% переходов → dead
CHECK_INTERVAL  = 60    # проверка каждые 60 сек
MAX_RESTARTS    = 3     # максимум перезапусков подряд

# Автофикс: если DEAD — снижаем dead_threshold в два раза (минимум 0.0005)
AUTOFIX_DEAD_THRESHOLD_MIN = 0.0005
# ──────────────────────────────────────────────────────────────────────────────

DUMP_DIR.mkdir(parents=True, exist_ok=True)
restart_count  = 0
last_restart   = 0
autofix_done   = False   # чтобы не фиксить бесконечно

def tg(msg: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": f"?? Watchdog: {msg}", "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"[TG ERROR] {e}")

def ts():
    return datetime.now().strftime("%H:%M:%S")

def dump_logs(reason: str):
    fname = DUMP_DIR / f"dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    try:
        result = subprocess.run(["tail", "-200", LOG_FILE], capture_output=True, text=True)
        fname.write_text(f"# Причина: {reason}\n\n{result.stdout}")
        print(f"[{ts()}] Дамп сохранён: {fname}")
    except Exception as e:
        print(f"[{ts()}] Ошибка дампа: {e}")

def restart_bot(reason: str) -> bool:
    global restart_count, last_restart
    now = time.time()
    if now - last_restart > 3600:
        restart_count = 0
    if restart_count >= MAX_RESTARTS:
        tg(f"?? <b>СТОП:</b> {MAX_RESTARTS} перезапусков подряд!\n"
           f"Причина: {reason}\n"
           f"Нужно вмешательство вручную!")
        return False
    restart_count += 1
    last_restart = now
    dump_logs(reason)
    subprocess.run(["systemctl", "restart", BOT_SERVICE])
    time.sleep(5)
    tg(f"?? <b>Перезапуск #{restart_count}</b>\nПричина: {reason}")
    print(f"[{ts()}] Перезапуск #{restart_count}: {reason}")
    return True

def autofix_dead_threshold():
    """Снижает dead_threshold в settings.yaml если рынок постоянно DEAD"""
    global autofix_done
    if autofix_done:
        return
    try:
        with open(SETTINGS_FILE, "r") as f:
            content = f.read()

        # Ищем текущее значение
        m = re.search(r"dead_threshold:\s*([\d.]+)", content)
        if not m:
            tg("⚠️ Автофикс: dead_threshold не найден в settings.yaml")
            return

        current = float(m.group(1))
        new_val  = max(current / 2, AUTOFIX_DEAD_THRESHOLD_MIN)

        if new_val == current:
            tg("⚠️ Автофикс: dead_threshold уже на минимуме")
            return

        new_content = content.replace(
            f"dead_threshold: {m.group(1)}",
            f"dead_threshold: {new_val:.4f}"
        )
        with open(SETTINGS_FILE, "w") as f:
            f.write(new_content)

        autofix_done = True
        tg(f"?? <b>Автофикс:</b> dead_threshold {current} → {new_val:.4f}\n"
           f"Перезапускаю бота...")
        print(f"[{ts()}] Автофикс: dead_threshold {current} → {new_val:.4f}")
        restart_bot(f"Автофикс dead_threshold: {current} → {new_val:.4f}")

    except Exception as e:
        tg(f"❌ Автофикс ошибка: {e}")
        print(f"[{ts()}] Автофикс ошибка: {e}")

def check_service_running() -> bool:
    r = subprocess.run(["systemctl", "is-active", BOT_SERVICE],
                       capture_output=True, text=True)
    return r.stdout.strip() == "active"

def check_websocket_alive() -> bool:
    """Лог обновлялся последние 2 минуты?"""
    try:
        result = subprocess.run(["tail", "-5", LOG_FILE], capture_output=True, text=True)
        lines  = result.stdout.strip().splitlines()
        if not lines:
            return False
        m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", lines[-1])
        if not m:
            return True
        log_time = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        age = (datetime.now() - log_time).total_seconds()
        return age < 120
    except:
        return True

def check_critical_errors() -> str | None:
    try:
        result = subprocess.run(["tail", "-50", LOG_FILE], capture_output=True, text=True)
        text   = result.stdout
        for pattern in [
            "ConnectionRefusedError",
            "WebSocket connection closed",
            "Traceback (most recent call last)",
            "OSError: [Errno",
        ]:
            if pattern in text:
                return pattern
    except:
        pass
    return None

def check_dead_ratio() -> float:
    """Доля переходов → dead среди всех переходов состояния в последних 500 строках"""
    try:
        result = subprocess.run(["tail", "-500", LOG_FILE], capture_output=True, text=True)
        text  = result.stdout
        dead  = text.count("→ dead")
        total = dead + text.count("→ normal") + text.count("→ volatile")
        return dead / total if total else 0.0
    except:
        return 0.0

def check_hours_without_trade() -> int:
    try:
        conn = sqlite3.connect(DB_FILE)
        cur  = conn.cursor()
        cur.execute("SELECT MAX(timestamp) FROM signal_features WHERE was_traded=1")
        row  = cur.fetchone()
        conn.close()
        if not row or not row[0]:
            return 999
        last = datetime.fromisoformat(row[0])
        return int((datetime.now() - last).total_seconds() / 3600)
    except:
        return 0

# ─── Главный цикл ─────────────────────────────────────────────────────────────
print(f"[{ts()}] Watchdog запущен. Интервал: {CHECK_INTERVAL}с")
tg("✅ Watchdog <b>запущен</b> — слежу за ботом")

alerted_dead    = False
alerted_notrade = False

while True:
    try:
        # 1. Сервис упал?
        if not check_service_running():
            restart_bot("Сервис не активен")
            alerted_dead = alerted_notrade = False
            time.sleep(CHECK_INTERVAL)
            continue

        # 2. WebSocket завис?
        if not check_websocket_alive():
            restart_bot("Лог завис >2 мин — WS мёртв?")
            time.sleep(CHECK_INTERVAL)
            continue

        # 3. Критические ошибки?
        err = check_critical_errors()
        if err:
            restart_bot(f"Критическая ошибка: {err}")
            time.sleep(CHECK_INTERVAL)
            continue

        # 4. Всё время DEAD?
        dead_pct = check_dead_ratio()
        if dead_pct > DEAD_PCT_LIMIT:
            if not alerted_dead:
                tg(f"⚠️ <b>DEAD рынок:</b> {dead_pct:.0%} сигналов заблокировано\n"
                   f"Запускаю автофикс dead_threshold...")
                alerted_dead = True
            autofix_dead_threshold()
        else:
            alerted_dead  = False
            autofix_done  = False  # сброс — можно фиксить снова если вернётся

        # 5. Нет сделок N часов?
        no_trade_h = check_hours_without_trade()
        if no_trade_h >= NO_TRADE_HOURS and not alerted_notrade:
            tg(f"⚠️ <b>Нет сделок</b> уже {no_trade_h}ч\n"
               f"DEAD рынков: {dead_pct:.0%}\n"
               f"Проверь фильтры!")
            alerted_notrade = True
        elif no_trade_h < NO_TRADE_HOURS:
            alerted_notrade = False

        print(f"[{ts()}] OK | DEAD={dead_pct:.0%} | no_trade={no_trade_h}h | restarts={restart_count}")

    except Exception as e:
        print(f"[{ts()}] Watchdog ошибка: {e}")

    time.sleep(CHECK_INTERVAL)
