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
