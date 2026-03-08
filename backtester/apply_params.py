"""
backtester/apply_params.py — применение лучших параметров из оптимизации в конфиг

Использование:
    python3 -m backtester.apply_params --file data/best_BTCUSDT_sharpe.yaml
"""

from __future__ import annotations

import argparse
import logging
import yaml
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)


def set_nested(cfg: dict, key: str, value):
    """Устанавливает значение по dot-notation ключу."""
    parts = key.split('.')
    node = cfg
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file',   required=True, help='YAML с лучшими параметрами')
    parser.add_argument('--config', default='config/strategies/hybrid.yaml')
    parser.add_argument('--dry-run', action='store_true', help='Показать без записи')
    args = parser.parse_args()

    best = yaml.safe_load(open(args.file))
    params = best.get('params', {})
    stats = best.get('stats', {})

    print(f"\nApplying params from: {args.file}")
    print(f"Metric: {best.get('metric')} = {best.get('metric_value')}")
    print(f"Stats: trades={stats.get('trades')} win_rate={stats.get('win_rate'):.1%} "
          f"pnl={stats.get('total_pnl'):+.2f} sharpe={stats.get('sharpe'):.3f}")
    print()

    cfg = yaml.safe_load(open(args.config))

    for key, value in params.items():
        old_val = None
        parts = key.split('.')
        node = cfg
        for part in parts[:-1]:
            node = node.get(part, {})
        old_val = node.get(parts[-1], 'N/A')
        print(f"  {key}: {old_val} → {value}")
        if not args.dry_run:
            set_nested(cfg, key, value)

    if args.dry_run:
        print("\n[DRY RUN] No changes written.")
        return

    # Бэкап оригинала
    backup = args.config + '.bak'
    Path(backup).write_text(Path(args.config).read_text())
    print(f"\nBackup saved to {backup}")

    # Записываем
    with open(args.config, 'w') as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)
    print(f"Config updated: {args.config}")
    print("Restart bot to apply: systemctl restart hybrid-bot")


if __name__ == '__main__':
    main()
