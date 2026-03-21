# core/feature_logger.py

"""
Расширенный логгер — записывает полный snapshot фич при каждом сигнале.
НЕ влияет на торговую логику, только собирает данные.
"""

import sqlite3
from datetime import datetime
from typing import Dict, Optional
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class FeatureLogger:
    """
    Записывает фичи сигналов в SQLite для последующего анализа.
    Thread-safe, async-compatible.
    """
    
    def __init__(self, db_path: str = "data/signal_features.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Создаёт таблицу если не существует."""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signal_features (
                    signal_id TEXT PRIMARY KEY,
                    timestamp INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    
                    -- Market context
                    session TEXT,
                    regime TEXT,
                    btc_bias TEXT,
                    
                    -- Analyzer scores (RAW, до агрегации)
                    vector_score REAL,
                    vector_delta REAL,
                    vector_cvd REAL,
                    vector_sweep_detected INTEGER,
                    
                    averages_score REAL,
                    averages_slope REAL,
                    averages_ema_cross INTEGER,
                    
                    depth_score REAL,
                    depth_imbalance REAL,
                    depth_wall_detected INTEGER,
                    
                    -- Aggregated signal
                    scenario TEXT,
                    final_strength REAL,
                    direction TEXT,
                    threshold_used REAL,
                    
                    -- TickMomentum (если применимо)
                    tick_momentum_active INTEGER,
                    tick_price_change_pct REAL,
                    tick_continuation_pct REAL,
                    tick_window_sec INTEGER,
                    
                    -- Market microstructure
                    spread_bps REAL,
                    mid_price REAL,
                    volume_ratio REAL,
                    price_volatility REAL,
                    
                    -- Risk context (на момент сигнала)
                    open_positions_count INTEGER,
                    session_pnl_pct REAL,
                    consecutive_losses INTEGER,
                    account_equity REAL,
                    risk_multiplier REAL,
                    
                    -- Time features
                    hour_utc INTEGER,
                    day_of_week INTEGER,
                    minutes_since_session_open INTEGER,
                    
                    -- OUTCOME (заполняется позже, при закрытии)
                    was_traded INTEGER DEFAULT NULL,
                    rejected_reason TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    pnl_pct REAL,
                    pnl_usd REAL,
                    exit_reason TEXT,
                    duration_sec INTEGER,
                    max_adverse_excursion REAL,
                    max_favorable_excursion REAL
                )
            """)
            
            # Индексы
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON signal_features(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON signal_features(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scenario ON signal_features(scenario)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_was_traded ON signal_features(was_traded)")
            
            conn.commit()
    
    @contextmanager
    def _get_conn(self):
        """Thread-safe context manager для SQLite."""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        try:
            yield conn
        finally:
            conn.close()
    
    def log_signal(
        self,
        signal_id: str,
        symbol: str,
        
        # Analyzer outputs
        vector_signal: Optional[object],
        averages_signal: Optional[object],
        depth_signal: Optional[object],
        
        # Aggregated signal
        final_signal: object,
        
        # Context
        session: str,
        regime: str,
        btc_bias: str,
        
        # TickMomentum
        tick_momentum_data: Optional[Dict],
        
        # Market data
        market_data: Dict,
        
        # Risk state
        risk_state: Dict,
    ):
        """
        Записывает полный snapshot фич сигнала.
        Вызывается из SignalAggregator ПЕРЕД отправкой в RiskManager.
        """
        
        now = datetime.utcnow()
        
        features = {
            'signal_id': signal_id,
            'timestamp': int(now.timestamp()),
            'symbol': symbol,
            
            # Context
            'session': session,
            'regime': regime,
            'btc_bias': btc_bias,
            
            # Vector
            'vector_score': vector_signal.score if vector_signal else None,
            'vector_delta': getattr(vector_signal, 'delta', None),
            'vector_cvd': getattr(vector_signal, 'cvd', None),
            'vector_sweep_detected': int(getattr(vector_signal, 'sweep_detected', False)),
            
            # Averages
            'averages_score': averages_signal.score if averages_signal else None,
            'averages_slope': getattr(averages_signal, 'slope', None),
            'averages_ema_cross': int(getattr(averages_signal, 'ema_cross', False)),
            
            # Depth
            'depth_score': depth_signal.score if depth_signal else None,
            'depth_imbalance': getattr(depth_signal, 'imbalance', None),
            'depth_wall_detected': int(getattr(depth_signal, 'wall_detected', False)),
            
            # Final signal
            'scenario': final_signal.scenario,
            'final_strength': final_signal.strength,
            'direction': final_signal.direction,
            'threshold_used': final_signal.threshold if hasattr(final_signal, 'threshold') else None,
            
            # TickMomentum
            'tick_momentum_active': int(tick_momentum_data is not None) if tick_momentum_data else 0,
            'tick_price_change_pct': tick_momentum_data.get('price_change_pct') if tick_momentum_data else None,
            'tick_continuation_pct': tick_momentum_data.get('continuation_pct') if tick_momentum_data else None,
            'tick_window_sec': tick_momentum_data.get('window_sec') if tick_momentum_data else None,
            
            # Market microstructure
            'spread_bps': market_data.get('spread_bps'),
            'mid_price': market_data.get('mid_price'),
            'volume_ratio': market_data.get('volume_ratio'),
            'price_volatility': market_data.get('volatility'),
            
            # Risk state
            'open_positions_count': risk_state.get('open_positions'),
            'session_pnl_pct': risk_state.get('session_pnl_pct'),
            'consecutive_losses': risk_state.get('consecutive_losses'),
            'account_equity': risk_state.get('equity'),
            'risk_multiplier': risk_state.get('risk_multiplier'),
            
            # Time
            'hour_utc': now.hour,
            'day_of_week': now.weekday(),
            'minutes_since_session_open': market_data.get('minutes_since_session_open', 0),
        }
        
        # Insert
        with self._get_conn() as conn:
            placeholders = ', '.join(['?'] * len(features))
            columns = ', '.join(features.keys())
            
            conn.execute(
                f"INSERT OR IGNORE INTO signal_features ({columns}) VALUES ({placeholders})",
                tuple(features.values())
            )
            conn.commit()
        
        logger.debug(f"Logged signal features: {signal_id}")
    
    def update_outcome(
        self,
        signal_id: str,
        was_traded: bool,
        rejected_reason: Optional[str] = None,
        trade_result: Optional[Dict] = None,
    ):
        """
        Обновляет результат сигнала после закрытия позиции.
        Вызывается из PositionManager при закрытии.
        """
        
        updates = {
            'was_traded': int(was_traded),
            'rejected_reason': rejected_reason,
        }
        
        if trade_result:
            updates.update({
                'entry_price': trade_result.get('entry_price'),
                'exit_price': trade_result.get('exit_price'),
                'pnl_pct': trade_result.get('pnl_pct'),
                'pnl_usd': trade_result.get('pnl_usd'),
                'exit_reason': trade_result.get('exit_reason'),
                'duration_sec': trade_result.get('duration_sec'),
                'max_adverse_excursion': trade_result.get('mae'),
                'max_favorable_excursion': trade_result.get('mfe'),
            })
        
        # Update
        set_clause = ', '.join([f"{k} = ?" for k in updates.keys()])
        
        with self._get_conn() as conn:
            conn.execute(
                f"UPDATE signal_features SET {set_clause} WHERE signal_id = ?",
                (*updates.values(), signal_id)
            )
            conn.commit()
        
        logger.debug(f"Updated outcome for signal: {signal_id}")
    
    def get_recent_signals(self, hours: int = 24, min_trades: int = 50) -> Optional[object]:
        """Возвращает DataFrame последних сигналов для анализа."""
        import pandas as pd
        
        cutoff = int((datetime.utcnow().timestamp() - hours * 3600))
        
        with self._get_conn() as conn:
            df = pd.read_sql_query(
                "SELECT * FROM signal_features WHERE timestamp > ? ORDER BY timestamp DESC",
                conn,
                params=(cutoff,)
            )
        
        if len(df) < min_trades:
            logger.warning(f"Only {len(df)} signals in last {hours}h, need {min_trades}")
            return None
        
        return df
