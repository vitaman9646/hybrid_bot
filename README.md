# Hybrid Trading Bot

Гибридный алготрейдинг-бот для Bybit (USDT Perpetual Futures).

## Архитектура

Модульный конвейер: Market Data → Analyzers → Signal Aggregator → Filters → Position Manager → Executor

### Алгоритмы (Фаза 2)
- **Vector** — детектор аномальной волатильности в микрофреймах
- **Averages** — сравнение средних за два периода
- **Depth Shot** — торговля на уровнях крупной ликвидности в стакане

### Гибридные режимы
- `AND` — все анализаторы согласны
- `OR` — хотя бы один сигнал
- `WEIGHTED` — взвешенная оценка с dynamic weight adjustment

## Установка

```bash
git clone https://github.com/YOUR_USER/hybrid-trading-bot.git
cd hybrid-trading-bot
python -m venv venv
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
cp config/settings.example.yaml config/settings.yaml
# Отредактировать config/settings.yaml — вписать API ключи

---

## Roadmap

### Фаза 4 — Multi-profile оптимизация (следующий этап)

**Идея:** вместо одного общего конфига — 3 профиля под разные типы монет.

**Профили:**
- `high_cap` (BTC, ETH) — узкие спреды, консервативные параметры
- `mid_cap` (SOL, BNB, XRP) — средние пороги
- `volatile` (DOGE, ADA, AVAX) — широкие пороги, больший TP/SL

**План реализации:**
1. Дождаться накопления реальных данных на mainnet (2-4 недели)
2. Оптимизировать каждый символ отдельно
3. Кластеризовать по результатам (не по интуиции)
4. Добавить `profiles:` секцию в `hybrid.yaml`
5. Engine читает профиль символа при открытии позиции

**Фаза 5 — Auto-adaptive параметры**
- `min_spread_size` масштабируется на текущий ATR символа
- `VolatilityTracker` уже есть — использовать его данные
- Бот сам подстраивает пороги под рыночный режим (trending/ranging/volatile)

