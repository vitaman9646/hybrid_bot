"""
backtester/run_optimization.py — запуск оптимизации параметров

Использование:
    python3 -m backtester.run_optimization --symbol BTCUSDT --days 7
    python3 -m backtester.run_optimization --symbol BTCUSDT --days 7 --fast
    python3 -m backtester.run_optimization --symbol BTCUSDT --days 30 --workers 8
"""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import time
import yaml

from backtester.optimizer import ParameterOptimizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger(__name__)


# Полный grid — 486 комбинаций
PARAM_GRID = {
    'analyzers.vector.min_spread_size':   [0.003, 0.005, 0.01],
    'analyzers.vector.min_quote_volume':  [50, 200, 500],
    'analyzers.vector.time_frame':        [0.4, 0.6, 1.0],
    'analyzers.averages.short_period':    [30.0, 60.0, 120.0],
    'analyzers.averages.min_delta_pct':   [0.03, 0.05, 0.1],
    'order.take_profit.percent':          [0.5, 0.8, 1.2],
    'order.stop_loss.percent':            [1.0, 1.5, 2.0],
    'min_score':                          [0.5, 0.55, 0.65],
}

# Быстрый grid — 24 комбинации
PARAM_GRID_FAST = {
    'analyzers.vector.min_spread_size':   [0.003, 0.01],
    'analyzers.vector.min_quote_volume':  [50, 500],
    'order.take_profit.percent':          [0.5, 0.8, 1.2],
    'order.stop_loss.percent':            [1.0, 1.5, 2.0],
    'min_score':                          [0.5, 0.6],
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',         default='data/history.db')
    parser.add_argument('--symbol',     default='BTCUSDT')
    parser.add_argument('--days',       type=int, default=7)
    parser.add_argument('--metric',     default='sharpe',
                        choices=['sharpe', 'win_rate', 'total_pnl', 'profit_factor'])
    parser.add_argument('--min-trades', type=int, default=20)
    parser.add_argument('--fast',       action='store_true')
    parser.add_argument('--config',     default='config/strategies/hybrid.yaml')
    parser.add_argument('--top',        type=int, default=10)
    parser.add_argument('--workers',    type=int,
                        default=max(1, mp.cpu_count() - 1),
                        help=f'Параллельных процессов (default: CPU-1 = {max(1, mp.cpu_count()-1)})')
    args = parser.parse_args()

    cfg = yaml.safe_load(open(args.config))

    ts_to = time.time()
    ts_from = ts_to - args.days * 86400

    grid = PARAM_GRID_FAST if args.fast else PARAM_GRID
    combos = 1
    for v in grid.values():
        combos *= len(v)

    logger.info("=" * 60)
    logger.info("Symbol:  %s | %d days", args.symbol, args.days)
    logger.info("Metric:  %s | min_trades=%d", args.metric, args.min_trades)
    logger.info("Grid:    %d params, %d combinations", len(grid), combos)
    logger.info("Mode:    %s", "FAST" if args.fast else "FULL")
    logger.info("Workers: %d / %d CPUs", args.workers, mp.cpu_count())
    logger.info("=" * 60)

    t0 = time.time()
    opt = ParameterOptimizer(cfg, db_path=args.db)
    results = opt.run(
        symbol=args.symbol,
        ts_from=ts_from,
        ts_to=ts_to,
        param_grid=grid,
        metric=args.metric,
        min_trades=args.min_trades,
        workers=args.workers,
    )
    elapsed = time.time() - t0

    print("\n" + "=" * 60)
    if not results:
        print("No results. Try --min-trades 5 or --fast")
        return

    print(opt.top_n(results, n=args.top))
    print("=" * 60)
    print(f"\nCompleted in {elapsed:.0f}s ({combos} combinations, {args.workers} workers)")

    best = results[0]
    print(f"\n{'='*60}")
    print("BEST PARAMS:")
    for k, v in best.params.items():
        print(f"  {k}: {v}")

    print(f"\nSTATS:")
    r = best.result
    print(f"  {args.metric}: {best.metric_value:.4f}")
    print(f"  trades:        {r.total_trades}")
    print(f"  win_rate:      {r.win_rate:.1%}")
    print(f"  total_pnl:     {r.total_pnl:+.2f} USDT")
    print(f"  sharpe:        {r.sharpe:.3f}")
    print(f"  max_drawdown:  {r.max_drawdown:.2f} USDT")
    print(f"  profit_factor: {r.profit_factor:.2f}")

    import yaml as _yaml
    out = f"data/best_{args.symbol}_{args.metric}.yaml"
    with open(out, 'w') as f:
        _yaml.dump({
            'symbol': args.symbol,
            'metric': args.metric,
            'metric_value': round(best.metric_value, 4),
            'params': best.params,
            'stats': {
                'trades':        r.total_trades,
                'win_rate':      round(r.win_rate, 4),
                'total_pnl':     round(r.total_pnl, 4),
                'sharpe':        round(r.sharpe, 4),
                'max_drawdown':  round(r.max_drawdown, 4),
                'profit_factor': round(r.profit_factor, 4),
            },
        }, f, default_flow_style=False)
    print(f"\nSaved to {out}")


if __name__ == '__main__':
    main()
