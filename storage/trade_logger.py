# storage/trade_logger.py
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from models.signals import TradeData

logger = logging.getLogger(__name__)


class TradeLogger:
    """
    Логирование трейдов и сигналов.
    Фаза 1: файловый логгер (JSON lines)
    Фаза 4: TimescaleDB
    """
    
    def __init__(self, log_dir: str = "data/trades"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self._trade_count = 0
        self._signal_count = 0
        
        # Текущий файл (ротация по дням)
        self._current_date = ""
        self._trade_file = None
        self._signal_file = None
    
    def _get_file(self, prefix: str):
        """Получить файл для текущего дня"""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        
        if today != self._current_date:
            self._current_date = today
            self._close_files()
        
        filepath = self.log_dir / f"{prefix}_{today}.jsonl"
        return open(filepath, 'a')
    
    def _close_files(self):
        if self._trade_file:
            self._trade_file.close()
            self._trade_file = None
        if self._signal_file:
            self._signal_file.close()
            self._signal_file = None
    
    def log_trade(self, trade: dict):
        """Записать информацию о сделке"""
        trade['logged_at'] = time.time()
        
        f = self._get_file("trades")
        f.write(json.dumps(trade) + '\n')
        f.flush()
        
        self._trade_count += 1
    
    def log_signal(self, signal_data: dict):
        """Записать сигнал"""
        signal_data['logged_at'] = time.time()
        
        f = self._get_file("signals")
        f.write(json.dumps(signal_data) + '\n')
        f.flush()
        
        self._signal_count += 1
    
    def log_market_data(
        self, symbol: str, trades: list[TradeData]
    ):
        """
        Markets Saver: записать тиковые данные.
        Для будущего бэктеста.
        """
        f = self._get_file(f"ticks_{symbol}")
        for trade in trades:
            record = {
                'symbol': trade.symbol,
                'price': trade.price,
                'qty': trade.qty,
                'quote_volume': trade.quote_volume,
                'side': trade.side,
                'timestamp': trade.timestamp,
                'trade_id': trade.trade_id,
            }
            f.write(json.dumps(record) + '\n')
        f.flush()
    
    def get_stats(self) -> dict:
        return {
            'trades_logged': self._trade_count,
            'signals_logged': self._signal_count,
            'log_dir': str(self.log_dir),
        }
    
    def __del__(self):
        self._close_files()
