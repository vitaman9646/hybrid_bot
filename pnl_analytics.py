"""
PnL Analytics — анализ торговой статистики из trades.csv

Использование:
    python3 pnl_analytics.py
    python3 pnl_analytics.py --file data/trades.csv --days 7
"""
from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def load_trades(path: str, days: int = None) -> list[dict]:
    rows = []
    cutoff = None
    if days:
        cutoff = datetime.now().timestamp() - days * 86400

    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get('pnl_usdt'):
                continue
            try:
                row['pnl_usdt'] = float(row['pnl_usdt'])
                row['pnl_pct'] = float(row['pnl_pct'] or 0)
                row['entry_price'] = float(row['entry_price'] or 0)
                row['exit_price'] = float(row['exit_price'] or 0)
                row['size_usdt'] = float(row['size_usdt'] or 0)
                row['duration_sec'] = int(row['duration_sec'] or 0)
                row['timestamp'] = float(row['timestamp'] or 0)
                if cutoff and row['timestamp'] < cutoff:
                    continue
                rows.append(row)
            except (ValueError, KeyError):
                continue
    return rows


def sharpe(pnls: list[float], periods_per_year: float = 252) -> float:
    if len(pnls) < 2:
        return 0.0
    n = len(pnls)
    mean = sum(pnls) / n
    variance = sum((p - mean) ** 2 for p in pnls) / (n - 1)
    std = math.sqrt(variance)
    if std == 0:
        return 0.0
    return round(mean / std * math.sqrt(periods_per_year), 3)


def max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
    return round(max_dd, 4)


def profit_factor(pnls: list[float]) -> float:
    wins = sum(p for p in pnls if p > 0)
    losses = abs(sum(p for p in pnls if p < 0))
    if losses == 0:
        return float('inf') if wins > 0 else 0.0
    return round(wins / losses, 3)


def consecutive_stats(pnls: list[float]) -> tuple[int, int]:
    """Макс серия побед и поражений."""
    max_wins = max_losses = cur_wins = cur_losses = 0
    for p in pnls:
        if p > 0:
            cur_wins += 1
            cur_losses = 0
        else:
            cur_losses += 1
            cur_wins = 0
        max_wins = max(max_wins, cur_wins)
        max_losses = max(max_losses, cur_losses)
    return max_wins, max_losses


def print_section(title: str):
    print(f"\n{'═' * 50}")
    print(f"  {title}")
    print('═' * 50)


def print_stats(trades: list[dict], label: str = "ALL"):
    if not trades:
        print(f"  {label}: нет сделок")
        return

    pnls = [t['pnl_usdt'] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    durations = [t['duration_sec'] for t in trades]

    win_rate = len(wins) / len(pnls) * 100
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    pf = profit_factor(pnls)
    sh = sharpe(pnls)
    max_dd = max_drawdown(pnls)
    max_w, max_l = consecutive_stats(pnls)
    avg_dur = sum(durations) / len(durations) if durations else 0
    expectancy = sum(pnls) / len(pnls)

    print(f"\n  [{label}]  Trades: {len(trades)}")
    print(f"  Total PnL:      {sum(pnls):+.4f} USDT")
    print(f"  Win rate:       {win_rate:.1f}%  ({len(wins)}W / {len(losses)}L)")
    print(f"  Expectancy:     {expectancy:+.4f} USDT/trade")
    print(f"  Profit factor:  {pf:.3f}")
    print(f"  Sharpe:         {sh:.3f}")
    print(f"  Max drawdown:   {max_dd:.4f} USDT")
    print(f"  Avg win:        {avg_win:+.4f} USDT")
    print(f"  Avg loss:       {avg_loss:+.4f} USDT")
    print(f"  Win/Loss ratio: {abs(avg_win/avg_loss):.2f}" if avg_loss != 0 else "  Win/Loss ratio: ∞")
    print(f"  Best trade:     {max(pnls):+.4f} USDT")
    print(f"  Worst trade:    {min(pnls):+.4f} USDT")
    print(f"  Max consec W/L: {max_w} / {max_l}")
    print(f"  Avg duration:   {avg_dur:.0f}s ({avg_dur/60:.1f} min)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', default='data/trades.csv')
    parser.add_argument('--days', type=int, default=None)
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"File not found: {path}")
        return

    trades = load_trades(str(path), args.days)

    period = f"последние {args.days} дней" if args.days else "всё время"
    print(f"\n{'═' * 50}")
    print(f"  PnL Analytics — {period}")
    print(f"  File: {path}")
    print(f"{'═' * 50}")

    if not trades:
        print("\n  Нет сделок для анализа.")
        return

    # Общая статистика
    print_section("ОБЩАЯ СТАТИСТИКА")
    print_stats(trades, "ALL")

    # По символам
    print_section("ПО СИМВОЛАМ")
    by_symbol = defaultdict(list)
    for t in trades:
        by_symbol[t['symbol']].append(t)
    for sym, sym_trades in sorted(by_symbol.items()):
        print_stats(sym_trades, sym)

    # По направлению
    print_section("LONG vs SHORT")
    by_dir = defaultdict(list)
    for t in trades:
        by_dir[t['direction']].append(t)
    for d, d_trades in sorted(by_dir.items()):
        print_stats(d_trades, d.upper())

    # По сценарию
    print_section("ПО СЦЕНАРИЮ")
    by_scenario = defaultdict(list)
    for t in trades:
        by_scenario[t.get('scenario', 'unknown') or 'unknown'].append(t)
    for sc, sc_trades in sorted(by_scenario.items()):
        print_stats(sc_trades, sc)

    # По причине выхода
    print_section("ПО ПРИЧИНЕ ВЫХОДА")
    by_reason = defaultdict(list)
    for t in trades:
        by_reason[t.get('exit_reason', '?') or '?'].append(t)
    for reason, r_trades in sorted(by_reason.items()):
        pnls = [t['pnl_usdt'] for t in r_trades]
        print(f"  {reason:20s}: {len(r_trades):4d} сделок  PnL={sum(pnls):+.4f}  WR={len([p for p in pnls if p>0])/len(pnls)*100:.1f}%")

    # По часам (UTC)
    print_section("ПО ЧАСАМ (UTC)")
    by_hour = defaultdict(list)
    for t in trades:
        hour = datetime.fromtimestamp(t['timestamp'], tz=timezone.utc).hour
        by_hour[hour].append(t['pnl_usdt'])
    print(f"  {'Hour':>5}  {'Trades':>6}  {'PnL':>10}  {'WR':>7}")
    for h in sorted(by_hour.keys()):
        pnls = by_hour[h]
        wr = len([p for p in pnls if p > 0]) / len(pnls) * 100
        print(f"  {h:02d}:00  {len(pnls):>6}  {sum(pnls):>+10.4f}  {wr:>6.1f}%")

    # Equity curve (ASCII)
    print_section("EQUITY CURVE")
    equity = 0.0
    curve = []
    for t in sorted(trades, key=lambda x: x['timestamp']):
        equity += t['pnl_usdt']
        curve.append(equity)

    if curve:
        min_eq = min(curve)
        max_eq = max(curve)
        height = 8
        width = min(len(curve), 60)
        step = max(1, len(curve) // width)
        sampled = curve[::step][:width]

        rng = max_eq - min_eq
        print(f"\n  {min_eq:+.2f} → {max_eq:+.2f} USDT  (final: {curve[-1]:+.4f})\n")
        for row in range(height, -1, -1):
            threshold = min_eq + (rng * row / height) if rng > 0 else 0
            line = ""
            for val in sampled:
                line += "█" if val >= threshold else " "
            label = f"{threshold:+6.2f} |" if row % 2 == 0 else "       |"
            print(f"  {label}{line}")
        print(f"       +{'-' * len(sampled)}")


if __name__ == '__main__':
    main()
