"""
backtester/fetch_history.py — загрузка исторических тиков из Bybit REST API

Использование:
    python3 backtester/fetch_history.py --symbols BTCUSDT ETHUSDT --days 7
"""

from __future__ import annotations

import argparse
import logging
import time
import yaml
from pathlib import Path
from pybit.unified_trading import HTTP

from backtester.market_saver import MarketSaver, TradeRecord

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)


def fetch_trades(
    client: HTTP,
    symbol: str,
    ts_from: float,
    ts_to: float,
    saver: MarketSaver,
) -> int:
    """
    Загружает тики для symbol за период [ts_from, ts_to].
    Bybit отдаёт max 1000 записей за запрос — пагинируем.
    Возвращает кол-во загруженных тиков.
    """
    total = 0
    cursor = None
    ts_to_ms = int(ts_to * 1000)
    ts_from_ms = int(ts_from * 1000)

    logger.info(
        "Fetching %s from %s to %s",
        symbol,
        time.strftime('%Y-%m-%d', time.gmtime(ts_from)),
        time.strftime('%Y-%m-%d', time.gmtime(ts_to)),
    )

    while True:
        params = dict(
            category='linear',
            symbol=symbol,
            limit=1000,
            endTime=ts_to_ms,
        )
        if cursor:
            params['cursor'] = cursor

        try:
            resp = client.get_public_trade_history(**params)
        except Exception as e:
            logger.error("API error: %s", e)
            break

        if resp.get('retCode') != 0:
            logger.error("API retCode: %s", resp.get('retMsg'))
            break

        result = resp.get('result', {})
        trades_raw = result.get('list', [])

        if not trades_raw:
            break

        for t in trades_raw:
            ts_ms = int(t.get('T', 0) or t.get('time', 0))
            if ts_ms < ts_from_ms:
                saver.flush()
                logger.info("%s: loaded %d trades", symbol, total)
                return total

            record = TradeRecord(
                symbol=symbol,
                price=float(t.get('p', 0) or t.get('price', 0)),
                qty=float(t.get('v', 0) or t.get('size', 0)),
                side=t.get('S', 'Buy') or t.get('side', 'Buy'),
                timestamp=ts_ms / 1000.0,
            )
            saver.save_trade_record(record)
            total += 1

        # Обновляем ts_to для следующей страницы
        last_ts = int(trades_raw[-1].get('T', 0) or trades_raw[-1].get('time', 0))
        ts_to_ms = last_ts - 1

        cursor = result.get('nextPageCursor')

        logger.info("%s: fetched %d (total=%d)", symbol, len(trades_raw), total)

        if ts_to_ms < ts_from_ms:
            break

        # Rate limit
        time.sleep(0.01)

    saver.flush()
    logger.info("%s: done, total=%d trades", symbol, total)
    return total


def _fetch_one(args_tuple):
    symbol, ex_cfg, db_path, sym_ts_from, ts_to = args_tuple
    _client = HTTP(
        testnet=ex_cfg.get('testnet', True),
        api_key=ex_cfg.get('api_key', ''),
        api_secret=ex_cfg.get('api_secret', ''),
    )
    _saver = MarketSaver(db_path)
    n = fetch_trades(_client, symbol, sym_ts_from, ts_to, _saver)
    _saver.close()
    return symbol, n


def main():
    parser = argparse.ArgumentParser(description='Fetch historical trades from Bybit')
    parser.add_argument('--symbols', nargs='+', default=['BTCUSDT', 'ETHUSDT'],
                        help='Symbols to fetch')
    parser.add_argument('--days', type=int, default=7,
                        help='Number of days to fetch')
    parser.add_argument('--db', default='data/market.db',
                        help='SQLite database path')
    parser.add_argument('--config', default='config/settings.yaml',
                        help='Config file path')
    args = parser.parse_args()

    cfg = yaml.safe_load(open(args.config))
    ex_cfg = cfg.get('exchange', {})

    client = HTTP(
        testnet=ex_cfg.get('testnet', True),
        api_key=ex_cfg.get('api_key', ''),
        api_secret=ex_cfg.get('api_secret', ''),
    )

    saver = MarketSaver(args.db)

    ts_to = time.time()
    ts_from = ts_to - args.days * 86400

    # Инкрементальная скачка — начинаем с последней записи в БД
    import sqlite3 as _sqlite3
    try:
        _conn = _sqlite3.connect(args.db)
        _cur = _conn.cursor()
        for _sym in args.symbols:
            _cur.execute("SELECT MAX(ts) FROM trades WHERE symbol=?", (_sym,))
            _row = _cur.fetchone()
            if _row and _row[0]:
                _last_ts = float(_row[0])
                if _last_ts > ts_from:
                    logger.info("Incremental: %s last_ts=%s, skipping already fetched data",
                                _sym, time.strftime('%Y-%m-%d %H:%M', time.gmtime(_last_ts)))
        _conn.close()
    except Exception as _e:
        logger.warning("Incremental check failed: %s", _e)

    # Получаем последние ts per-symbol для инкрементальной скачки
    import sqlite3 as _sqlite3
    symbol_ts_from = {}
    try:
        _conn = _sqlite3.connect(args.db)
        _cur = _conn.cursor()
        for _sym in args.symbols:
            _cur.execute("SELECT MAX(ts) FROM trades WHERE symbol=?", (_sym,))
            _row = _cur.fetchone()
            if _row and _row[0]:
                _last_ts = float(_row[0])
                if _last_ts > ts_from:
                    symbol_ts_from[_sym] = _last_ts + 0.001
                    logger.info("Incremental %s: starting from %s",
                                _sym, time.strftime('%Y-%m-%d %H:%M', time.gmtime(_last_ts)))
        _conn.close()
    except Exception as _e:
        logger.warning("Incremental check failed: %s", _e)

    from multiprocessing import Pool

    fetch_args = [(sym, ex_cfg, args.db, symbol_ts_from.get(sym, ts_from), ts_to) for sym in args.symbols]

    workers = min(len(args.symbols), 4)
    logger.info("Fetching %d symbols with %d workers", len(args.symbols), workers)

    with Pool(workers) as pool:
        results = pool.map(_fetch_one, fetch_args)

    total_all = 0
    for symbol, n in results:
        total_all += n
        logger.info("=== %s: %d trades loaded ===", symbol, n)

    logger.info("=== DONE: %d total trades for %s ===", total_all, args.symbols)

    # Показываем что в БД
    saver2 = MarketSaver(args.db)
    for sym in saver2.get_symbols():
        ts_min, ts_max = saver2.get_time_range(sym)
        count = saver2.get_trade_count(sym)
        logger.info(
            "DB: %s — %d trades, %s → %s",
            sym, count,
            time.strftime('%Y-%m-%d', time.gmtime(ts_min)),
            time.strftime('%Y-%m-%d', time.gmtime(ts_max)),
        )


if __name__ == '__main__':
    main()
