"""
Запуск тикового бэктеста на исторических данных.
Использование:
    python3 run_backtest.py --db /mnt/d/hybrid_bot/data/history_btc_eth.db --symbols BTCUSDT ETHUSDT --days 30
"""
import argparse
import logging
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('backtest')


def load_config():
    import yaml
    import os
    cfg_file = next((f for f in ['config/settings.yaml', 'config/settings_mainnet.yaml'] if os.path.exists(f)), None)
    if not cfg_file:
        raise FileNotFoundError('No config found')
    with open(cfg_file) as f:
        return yaml.safe_load(f)


def run(symbol: str, db_path: str, days: int, config: dict):
    from backtester.replay_engine import ReplayEngine

    ts_to = time.time()
    ts_from = ts_to - days * 86400

    logger.info("=" * 60)
    logger.info("Symbol: %s | Days: %d | DB: %s", symbol, days, db_path)
    logger.info("Period: %s → %s",
        datetime.fromtimestamp(ts_from, tz=timezone.utc).strftime('%Y-%m-%d'),
        datetime.fromtimestamp(ts_to, tz=timezone.utc).strftime('%Y-%m-%d'),
    )

    engine = ReplayEngine(config, db_path=db_path)
    t0 = time.time()
    result = engine.run(symbol=symbol, ts_from=ts_from, ts_to=ts_to)
    elapsed = time.time() - t0

    logger.info("-" * 60)
    logger.info("Results for %s:", symbol)
    logger.info("  Trades:        %d", result.total_trades)
    logger.info("  Win rate:      %.1f%%", result.win_rate * 100)
    logger.info("  Profit factor: %.2f", result.profit_factor)
    logger.info("  Sharpe:        %.3f", result.sharpe)
    logger.info("  Total PnL:     %.2f USDT", result.total_pnl)
    logger.info("  Max drawdown:  %.2f%%", result.max_drawdown)
    logger.info("  Elapsed:       %.1fs", elapsed)
    logger.info("=" * 60)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='/mnt/d/hybrid_bot/data/history_btc_eth.db')
    parser.add_argument('--symbols', nargs='+', default=['BTCUSDT', 'ETHUSDT'])
    parser.add_argument('--days', type=int, default=30)
    args = parser.parse_args()

    config = load_config()

    # Бэктест-оверрайды: снижаем пороги для тиковых данных (нет реального стакана)
    # ReplayEngine читает cfg.get('analyzers', {}) — пишем на верхний уровень
    config.setdefault('analyzers', {})
    config['analyzers'].setdefault('vector', {})['min_spread_size'] = 0.05
    config['analyzers'].setdefault('vector', {})['min_trades_per_frame'] = 1
    config['analyzers'].setdefault('vector', {})['dead_threshold'] = 0.001
    config['analyzers'].setdefault('vector', {})['chaos_threshold'] = 0.5
    config['analyzers'].setdefault('averages', {})['min_delta'] = 0.05
    config['backtest_sample_every'] = 50  # каждый 50-й тик вместо 500

    results = {}
    for symbol in args.symbols:
        try:
            results[symbol] = run(symbol, args.db, args.days, config)
        except Exception as e:
            logger.error("Failed %s: %s", symbol, e, exc_info=True)

    # Итоговая сводка
    logger.info("\n=== SUMMARY ===")
    for sym, r in results.items():
        logger.info(
            "%s: trades=%d win=%.1f%% pf=%.2f sharpe=%.3f pnl=%.2f",
            sym, r.total_trades, r.win_rate * 100,
            r.profit_factor, r.sharpe, r.total_pnl
        )


if __name__ == '__main__':
    main()
