"""
backtester/optimizer.py — grid search по параметрам стратегии (параллельная версия)
"""

from __future__ import annotations

import copy
import itertools
import logging
import multiprocessing as mp
from dataclasses import dataclass
from typing import Any

from backtester.replay_engine import ReplayEngine, BacktestResult

logger = logging.getLogger(__name__)


@dataclass
class OptimizationResult:
    params: dict
    result: BacktestResult
    metric_value: float

    def __repr__(self) -> str:
        return (
            f"OptResult(metric={self.metric_value:.4f} "
            f"trades={self.result.total_trades} "
            f"win_rate={self.result.win_rate:.1%} "
            f"pnl={self.result.total_pnl:+.2f} "
            f"params={self.params})"
        )


def _run_combo(args: tuple):
    """Top-level worker — обязательно вне класса для pickle."""
    base_config, db_path, params, symbol, ts_from, ts_to, min_trades, metric, idx, total = args

    cfg = _apply_params_static(base_config, params)
    engine = ReplayEngine(cfg, db_path)

    try:
        bt_result = engine.run(symbol, ts_from, ts_to)
    except Exception as e:
        logger.warning("Combo %d/%d failed: %s", idx + 1, total, e)
        return None

    if bt_result.total_trades < min_trades:
        return None

    metric_val = _get_metric_static(bt_result, metric)

    logger.info(
        "Combo %d/%d: %s → %s=%.4f trades=%d",
        idx + 1, total, params, metric, metric_val, bt_result.total_trades,
    )

    return {
        'params': params,
        'metric_value': metric_val,
        'result': bt_result,
    }


def _apply_params_static(base_config: dict, params: dict) -> dict:
    cfg = copy.deepcopy(base_config)
    for key, value in params.items():
        parts = key.split('.')
        node = cfg
        for part in parts[:-1]:
            if part not in node:
                node[part] = {}
            node = node[part]
        node[parts[-1]] = value
    return cfg


def _get_metric_static(result: BacktestResult, metric: str) -> float:
    mapping = {
        'sharpe':        result.sharpe,
        'win_rate':      result.win_rate,
        'total_pnl':     result.total_pnl,
        'profit_factor': result.profit_factor,
        'max_drawdown':  -result.max_drawdown,
    }
    return mapping.get(metric, result.sharpe)


class ParameterOptimizer:

    def __init__(self, base_config: dict, db_path: str = "data/market.db"):
        self._base_config = base_config
        self._db_path = db_path

    def run(
        self,
        symbol: str,
        ts_from: float,
        ts_to: float,
        param_grid: dict[str, list],
        metric: str = 'sharpe',
        min_trades: int = 10,
        workers: int = 1,
    ) -> list[OptimizationResult]:

        keys = list(param_grid.keys())
        values = list(param_grid.values())
        combinations = list(itertools.product(*values))
        total = len(combinations)

        logger.info(
            "ParameterOptimizer: symbol=%s combinations=%d metric=%s workers=%d",
            symbol, total, metric, workers,
        )

        tasks = [
            (
                self._base_config,
                self._db_path,
                dict(zip(keys, combo)),
                symbol, ts_from, ts_to,
                min_trades, metric,
                i, total,
            )
            for i, combo in enumerate(combinations)
        ]

        if workers > 1:
            ctx = mp.get_context('spawn')
            with ctx.Pool(processes=workers) as pool:
                raw_results = pool.map(_run_combo, tasks)
        else:
            raw_results = [_run_combo(t) for t in tasks]

        results: list[OptimizationResult] = []
        for r in raw_results:
            if r is None:
                continue
            results.append(OptimizationResult(
                params=r['params'],
                result=r['result'],
                metric_value=r['metric_value'],
            ))

        results.sort(key=lambda r: r.metric_value, reverse=True)

        if results:
            logger.info("Best result: %s", results[0])
        else:
            logger.warning("No results with min_trades >= %d", min_trades)

        return results

    def top_n(self, results: list[OptimizationResult], n: int = 5) -> str:
        lines = [f"Top {min(n, len(results))} results:"]
        for i, r in enumerate(results[:n]):
            lines.append(
                f"  #{i+1} metric={r.metric_value:.4f} "
                f"trades={r.result.total_trades} "
                f"win_rate={r.result.win_rate:.1%} "
                f"pnl={r.result.total_pnl:+.2f} USDT "
                f"sharpe={r.result.sharpe:.3f} | "
                f"{r.params}"
            )
        return "\n".join(lines)

    @staticmethod
    def _apply_params(base_config: dict, params: dict) -> dict:
        return _apply_params_static(base_config, params)

    @staticmethod
    def _get_metric(result: BacktestResult, metric: str) -> float:
        return _get_metric_static(result, metric)
