"""
core/risk_manager.py — управление рисками v3
"""

from __future__ import annotations

import logging
import datetime
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class RiskConfig:
    # v3: риск на сделку в % от equity (заменяет position_pct)
    risk_per_trade_pct: float = 0.75       # 0.75% equity риска на сделку
    position_pct: float = 10.0             # fallback если нет SL distance
    sl_pct_default: float = 1.0            # дефолтный SL для расчёта размера

    drawdown_tiers: list[tuple[float, float]] = field(default_factory=lambda: [
        (0.0,  1.00),
        (-1.0, 0.50),
        (-2.0, 0.25),
        (-3.0, 0.00),
    ])

    # v3: дневной лимит в % от equity вместо фикс USDT
    daily_loss_limit_pct: float = 2.0      # 2% от баланса
    daily_loss_limit_usdt: float = 20.0    # fallback если баланс не задан

    corr_block_enabled: bool = True
    min_size_usdt: float = 5.0
    max_size_usdt: float = 500.0
    max_trades_per_day: int = 50
    max_concurrent_positions: int = 4
    max_consecutive_losses: int = 3
    consecutive_loss_size_mult: float = 0.5

    # v3: score-weighted sizing
    score_sizing_enabled: bool = True
    score_size_min_mult: float = 0.6       # слабый сигнал → 60% размера
    score_size_max_mult: float = 1.4       # сильный сигнал → 140% размера

    # v3: adaptive sizing по дневному PnL
    daily_profit_boost: float = 1.2        # в плюсе за день → +20%
    daily_loss_reduce: float = 0.5         # в минусе за день → -50%
    daily_profit_threshold_pct: float = 1.0   # порог "в плюсе" (% от баланса)
    daily_loss_threshold_pct: float = -0.5    # порог "в минусе"


@dataclass
class RiskDecision:
    allowed: bool
    size_usdt: float
    reason: str = ""

    def __repr__(self) -> str:
        status = "ALLOW" if self.allowed else "BLOCK"
        return f"RiskDecision({status}, size={self.size_usdt:.2f}, reason={self.reason!r})"


class RiskManager:

    def __init__(self, config: RiskConfig):
        self.cfg = config
        self._balance_usdt: float = 0.0
        self._session_pnl: float = 0.0
        self._session_date: str = self._today()
        self._daily_loss_usdt: float = 0.0
        self._open_symbols: set[str] = set()
        self._trades_today: int = 0
        self._consecutive_losses: int = 0
        logger.info("RiskManager v3 initialized: %s", config)

    def set_balance(self, balance_usdt: float) -> None:
        self._balance_usdt = max(balance_usdt, 0.0)
        logger.debug("RiskManager balance updated: %.2f USDT", self._balance_usdt)

    def record_open(self, symbol: str) -> None:
        self._open_symbols.add(symbol.upper())
        self._trades_today += 1

    def record_close(self, symbol: str, pnl_usdt: float) -> None:
        self._open_symbols.discard(symbol.upper())
        self._check_day_rollover()
        self._session_pnl += pnl_usdt
        if pnl_usdt < 0:
            self._daily_loss_usdt += abs(pnl_usdt)
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0
        logger.info(
            "RiskManager recorded close %s pnl=%.2f | session_pnl=%.2f daily_loss=%.2f",
            symbol, pnl_usdt, self._session_pnl, self._daily_loss_usdt,
        )

    def check(
        self,
        symbol: str,
        score: float = 0.5,
        sl_distance_pct: float = 0.0,
        scenario_threshold: float = 0.4,
    ) -> RiskDecision:
        self._check_day_rollover()
        symbol = symbol.upper()

        # 1. Daily loss limit (% от equity или фикс USDT)
        daily_limit = self._daily_loss_limit()
        if self._daily_loss_usdt >= daily_limit:
            return RiskDecision(
                allowed=False, size_usdt=0.0,
                reason=f"daily_loss_limit reached ({self._daily_loss_usdt:.2f} >= {daily_limit:.2f} USDT)",
            )

        # 2. Max trades per day
        if self._trades_today >= self.cfg.max_trades_per_day:
            return RiskDecision(
                allowed=False, size_usdt=0.0,
                reason=f"max_trades_per_day ({self._trades_today}/{self.cfg.max_trades_per_day})",
            )

        # 3. Max concurrent positions
        if len(self._open_symbols) >= self.cfg.max_concurrent_positions:
            return RiskDecision(
                allowed=False, size_usdt=0.0,
                reason=f"max_concurrent_positions ({len(self._open_symbols)}/{self.cfg.max_concurrent_positions})",
            )

        # 4. Session drawdown tiers
        size_mult = self._drawdown_multiplier()
        if size_mult == 0.0:
            return RiskDecision(
                allowed=False, size_usdt=0.0,
                reason=f"session drawdown stop (pnl={self._session_pnl:.2f} USDT)",
            )

        # 5. Корреляционный блок
        if self.cfg.corr_block_enabled:
            base = self._base_asset(symbol)
            for open_sym in self._open_symbols:
                if self._base_asset(open_sym) == base and open_sym != symbol:
                    return RiskDecision(
                        allowed=False, size_usdt=0.0,
                        reason=f"correlation block: {symbol} vs {open_sym}",
                    )

        # ── Расчёт размера позиции ────────────────────────────────────────

        sl_dist = sl_distance_pct if sl_distance_pct > 0 else self.cfg.sl_pct_default
        if self._balance_usdt <= 0:
            size = self.cfg.min_size_usdt
        else:
            # v3: Risk-based sizing
            # size = (risk_per_trade_pct% от баланса) / sl_distance_pct
            risk_usdt = self._balance_usdt * (self.cfg.risk_per_trade_pct / 100.0)
            size = risk_usdt / (sl_dist / 100.0)

            # Ограничиваем: не более position_pct% от баланса
            max_by_pct = self._balance_usdt * (self.cfg.position_pct / 100.0)
            size = min(size, max_by_pct)

        # Drawdown tier multiplier
        size *= size_mult

        # Consecutive losses
        if self._consecutive_losses >= self.cfg.max_consecutive_losses:
            size *= self.cfg.consecutive_loss_size_mult
            logger.warning("RiskManager: %d consecutive losses → size x%.1f",
                           self._consecutive_losses, self.cfg.consecutive_loss_size_mult)

        # v3: Score-weighted sizing
        if self.cfg.score_sizing_enabled and score > 0:
            score_mult = self._score_multiplier(score, scenario_threshold)
            size *= score_mult
            logger.debug("RiskManager: score=%.2f threshold=%.2f → size_mult=%.2f",
                         score, scenario_threshold, score_mult)

        # v3: Adaptive sizing по дневному PnL
        daily_mult = self._daily_pnl_multiplier()
        size *= daily_mult
        if daily_mult != 1.0:
            logger.info("RiskManager: daily PnL mult=%.2f (pnl=%.2f)", daily_mult, self._session_pnl)

        size = max(size, self.cfg.min_size_usdt)
        size = min(size, self.cfg.max_size_usdt)

        logger.info(
            "RiskManager check %s: balance=%.2f risk=%.2f%% sl=%.2f%% "
            "score=%.2f size=%.2f USDT",
            symbol, self._balance_usdt, self.cfg.risk_per_trade_pct,
            sl_dist, score, size,
        )
        return RiskDecision(allowed=True, size_usdt=round(size, 2))

    def _daily_loss_limit(self) -> float:
        """Динамический дневной лимит потерь в USDT."""
        if self._balance_usdt > 0:
            return self._balance_usdt * (self.cfg.daily_loss_limit_pct / 100.0)
        return self.cfg.daily_loss_limit_usdt

    def _score_multiplier(self, score: float, threshold: float) -> float:
        """Score → размер позиции. От min_mult до max_mult."""
        if score <= threshold:
            return self.cfg.score_size_min_mult
        # Линейная интерполяция от threshold до 1.0
        t = min((score - threshold) / max(1.0 - threshold, 0.01), 1.0)
        return self.cfg.score_size_min_mult + t * (self.cfg.score_size_max_mult - self.cfg.score_size_min_mult)

    def _daily_pnl_multiplier(self) -> float:
        """Адаптивный множитель по дневному PnL."""
        if self._balance_usdt <= 0:
            return 1.0
        pnl_pct = (self._session_pnl / self._balance_usdt) * 100.0
        if pnl_pct >= self.cfg.daily_profit_threshold_pct:
            return self.cfg.daily_profit_boost   # в плюсе → +20%
        elif pnl_pct <= self.cfg.daily_loss_threshold_pct:
            return self.cfg.daily_loss_reduce     # в минусе → -50%
        return 1.0

    @property
    def session_pnl(self) -> float:
        return self._session_pnl

    @property
    def daily_loss_usdt(self) -> float:
        return self._daily_loss_usdt

    @property
    def is_trading_halted(self) -> bool:
        self._check_day_rollover()
        return (
            self._daily_loss_usdt >= self._daily_loss_limit()
            or self._drawdown_multiplier() == 0.0
        )

    def status_str(self) -> str:
        return (
            f"RiskManager | balance={self._balance_usdt:.2f} USDT "
            f"session_pnl={self._session_pnl:+.2f} USDT "
            f"daily_loss={self._daily_loss_usdt:.2f}/{self._daily_loss_limit():.2f} USDT "
            f"drawdown_mult={self._drawdown_multiplier():.2f} "
            f"open_symbols={self._open_symbols}"
        )

    def _drawdown_multiplier(self) -> float:
        if self._balance_usdt <= 0:
            return 1.0
        pnl_pct = (self._session_pnl / self._balance_usdt) * 100.0
        sorted_tiers = sorted(self.cfg.drawdown_tiers, key=lambda t: t[0])
        matched = [(thr, mult) for thr, mult in sorted_tiers if pnl_pct >= thr]
        if not matched:
            return 0.0
        return matched[-1][1]

    @staticmethod
    def _base_asset(symbol: str) -> str:
        for quote in ("USDT", "BUSD", "USDC", "PERP", "USD"):
            if symbol.endswith(quote):
                return symbol[: -len(quote)]
        return symbol

    def _check_day_rollover(self) -> None:
        today = self._today()
        if today != self._session_date:
            logger.info("RiskManager: new day %s (prev pnl=%.2f)", today, self._session_pnl)
            self._session_date = today
            self._session_pnl = 0.0
            self._daily_loss_usdt = 0.0
            self._trades_today = 0

    @staticmethod
    def _today() -> str:
        return datetime.date.today().isoformat()
