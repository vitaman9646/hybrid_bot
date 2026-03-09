"""
core/risk_manager.py — управление рисками
"""

from __future__ import annotations

import logging
import datetime
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class RiskConfig:
    position_pct: float = 2.0
    drawdown_tiers: list[tuple[float, float]] = field(default_factory=lambda: [
        (0.0,  1.00),
        (-1.0, 0.50),
        (-2.0, 0.25),
        (-3.0, 0.00),
    ])
    daily_loss_limit_usdt: float = 50.0
    corr_block_enabled: bool = True
    min_size_usdt: float = 5.0
    max_size_usdt: float = 500.0
    max_trades_per_day: int = 50
    max_concurrent_positions: int = 4
    max_consecutive_losses: int = 3
    consecutive_loss_size_mult: float = 0.5


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
        logger.info("RiskManager initialized: %s", config)

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

    def check(self, symbol: str) -> RiskDecision:
        self._check_day_rollover()
        symbol = symbol.upper()

        # 1. Daily loss limit
        if self._daily_loss_usdt >= self.cfg.daily_loss_limit_usdt:
            return RiskDecision(
                allowed=False,
                size_usdt=0.0,
                reason=(
                    f"daily_loss_limit reached "
                    f"({self._daily_loss_usdt:.2f} >= "
                    f"{self.cfg.daily_loss_limit_usdt:.2f} USDT)"
                ),
            )

        # 1b. Max trades per day
        if self._trades_today >= self.cfg.max_trades_per_day:
            return RiskDecision(
                allowed=False,
                size_usdt=0.0,
                reason=f"max_trades_per_day reached ({self._trades_today}/{self.cfg.max_trades_per_day})",
            )

        # 1c. Max concurrent positions
        if len(self._open_symbols) >= self.cfg.max_concurrent_positions:
            return RiskDecision(
                allowed=False,
                size_usdt=0.0,
                reason=f"max_concurrent_positions reached ({len(self._open_symbols)}/{self.cfg.max_concurrent_positions})",
            )

        # 2. Session drawdown
        size_mult = self._drawdown_multiplier()
        if size_mult == 0.0:
            return RiskDecision(
                allowed=False,
                size_usdt=0.0,
                reason=f"session drawdown stop (pnl={self._session_pnl:.2f} USDT)",
            )

        # 3. Корреляционный блок
        if self.cfg.corr_block_enabled:
            base = self._base_asset(symbol)
            for open_sym in self._open_symbols:
                if self._base_asset(open_sym) == base and open_sym != symbol:
                    return RiskDecision(
                        allowed=False,
                        size_usdt=0.0,
                        reason=f"correlation block: {symbol} conflicts with open {open_sym}",
                    )

        # 4. Размер позиции
        if self._balance_usdt <= 0:
            base_size = self.cfg.min_size_usdt
            logger.warning("RiskManager: balance not set, using min_size_usdt=%.2f", base_size)
        else:
            base_size = self._balance_usdt * (self.cfg.position_pct / 100.0)

        size = base_size * size_mult

        # Consecutive losses → size reduction
        if self._consecutive_losses >= self.cfg.max_consecutive_losses:
            size *= self.cfg.consecutive_loss_size_mult
            logger.warning(
                "RiskManager: %d consecutive losses → size x%.1f",
                self._consecutive_losses, self.cfg.consecutive_loss_size_mult
            )

        size = max(size, self.cfg.min_size_usdt)
        size = min(size, self.cfg.max_size_usdt)

        logger.info(
            "RiskManager check %s: balance=%.2f base_size=%.2f mult=%.2f -> size=%.2f",
            symbol, self._balance_usdt, base_size, size_mult, size,
        )
        return RiskDecision(allowed=True, size_usdt=round(size, 2))

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
            self._daily_loss_usdt >= self.cfg.daily_loss_limit_usdt
            or self._drawdown_multiplier() == 0.0
        )

    def status_str(self) -> str:
        return (
            f"RiskManager | balance={self._balance_usdt:.2f} USDT "
            f"session_pnl={self._session_pnl:+.2f} USDT "
            f"daily_loss={self._daily_loss_usdt:.2f}/{self.cfg.daily_loss_limit_usdt:.2f} USDT "
            f"drawdown_mult={self._drawdown_multiplier():.2f} "
            f"open_symbols={self._open_symbols}"
        )

    def _drawdown_multiplier(self) -> float:
        """
        Тиры: (порог_%, множитель) где порог — верхняя граница зоны.

        Пример тиров (0.0, 1.0), (-1.0, 0.5), (-2.0, 0.25), (-3.0, 0.0):
          pnl= 0.0% → >= 0.0  → 1.00  (полный размер)
          pnl=-1.0% → >= -1.0 → 0.50  (половина)
          pnl=-1.5% → >= -2.0? нет → >= -1.0? да → 0.50
          pnl=-2.0% → >= -2.0 → 0.25
          pnl=-3.0% → >= -3.0 → 0.00  (стоп)

        Алгоритм: сортируем от меньшего к большему, идём снизу вверх,
        возвращаем множитель последнего тира чей порог pnl_pct прошёл.
        """
        if self._balance_usdt <= 0:
            return 1.0

        pnl_pct = (self._session_pnl / self._balance_usdt) * 100.0

        # Сортируем от меньшего порога к большему: -3.0, -2.0, -1.0, 0.0
        sorted_tiers = sorted(self.cfg.drawdown_tiers, key=lambda t: t[0])

        # Ищем все тиры которые pnl_pct "прошёл снизу" (pnl >= порог),
        # берём последний — он самый высокий подходящий
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

    def _reset_daily_counters(self) -> None:
        self._daily_loss_usdt = 0.0
        self._trades_today = 0
        self._session_date = self._today()
        logger.info("RiskManager: daily counters reset")

    def _check_day_rollover(self) -> None:
        today = self._today()
        if today != self._session_date:
            logger.info(
                "RiskManager: new day %s -> resetting (prev pnl=%.2f daily_loss=%.2f)",
                today, self._session_pnl, self._daily_loss_usdt,
            )
            self._session_date = today
            self._session_pnl = 0.0
            self._daily_loss_usdt = 0.0
            self._trades_today = 0

    @staticmethod
    def _today() -> str:
        return datetime.date.today().isoformat()
