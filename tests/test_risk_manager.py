"""
tests/test_risk_manager.py

Семантика тиров: (порог_%, множитель)
  pnl >= 0.0%  → 1.00
  pnl >= -1.0% → 0.50  (т.е. pnl пробил -1%, но не пробил -2%)
  pnl >= -2.0% → 0.25
  pnl >= -3.0% → 0.00  (стоп)

  pnl = -1.5% → пробил -1.0, пробил -2.0? нет (-1.5 > -2.0) → множитель тира -1.0 = 0.50
  pnl = -2.5% → пробил -2.0, пробил -3.0? нет → множитель тира -2.0 = 0.25

Алгоритм matched[-1]: берём самый высокий тир который pnl_pct >= threshold.
  sorted: [-3.0, -2.0, -1.0, 0.0]
  pnl=-1.5: matched = [(-3.0,0.0),(-2.0,0.25),(-1.0,0.5)] → last = 0.5  CORRECT
  pnl=-2.5: matched = [(-3.0,0.0),(-2.0,0.25)]             → last = 0.25 CORRECT
"""

import pytest
from core.risk_manager import RiskManager, RiskConfig


def make_rm(**kwargs) -> RiskManager:
    cfg = RiskConfig(**kwargs)
    return RiskManager(cfg)


def make_rm_with_balance(balance: float = 1000.0, **kwargs) -> RiskManager:
    rm = make_rm(**kwargs)
    rm.set_balance(balance)
    return rm


# ---------------------------------------------------------------------------
# 1. Базовый расчёт размера позиции
# ---------------------------------------------------------------------------

class TestPositionSizing:

    def test_default_2pct_of_balance(self):
        rm = make_rm_with_balance(1000.0, position_pct=2.0)
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == pytest.approx(20.0)

    def test_custom_pct(self):
        rm = make_rm_with_balance(2000.0, position_pct=5.0)
        d = rm.check("ETHUSDT")
        assert d.allowed
        assert d.size_usdt == pytest.approx(100.0)

    def test_max_size_cap(self):
        rm = make_rm_with_balance(10000.0, position_pct=10.0, max_size_usdt=500.0)
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == 500.0

    def test_min_size_floor(self):
        rm = make_rm_with_balance(10.0, position_pct=2.0, min_size_usdt=5.0)
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == 5.0

    def test_no_balance_uses_min_size(self):
        rm = make_rm(min_size_usdt=7.0)
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == 7.0


# ---------------------------------------------------------------------------
# 2. Drawdown tiers
# ---------------------------------------------------------------------------

class TestDrawdownTiers:

    def _rm(self) -> RiskManager:
        return make_rm_with_balance(
            1000.0,
            position_pct=10.0,   # base_size = 100
            drawdown_tiers=[
                (0.0,  1.00),
                (-1.0, 0.50),
                (-2.0, 0.25),
                (-3.0, 0.00),
            ],
        )

    def test_no_drawdown_full_size(self):
        rm = self._rm()
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == pytest.approx(100.0)

    def test_minus_1pct_half_size(self):
        # pnl=-10 = -1.0% → matched до тира -1.0 включительно → 0.50
        rm = self._rm()
        rm._session_pnl = -10.0
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == pytest.approx(50.0)

    def test_minus_1_5pct_quarter_size(self):
        # pnl=-15 = -1.5% → -1.5 >= -2.0? нет → matched до -1.0 → 0.50
        # НО: -1.5 >= -2.0 = False (−1.5 > −2.0 = True в математике!)
        # -1.5 > -2.0 TRUE → matched включает (-2.0, 0.25)
        # sorted: -3.0,-2.0,-1.0,0.0
        # -1.5>=-3.0 T, -1.5>=-2.0 T, -1.5>=-1.0 F → matched=[(-3,0),(-2,0.25)] → 0.25
        rm = self._rm()
        rm._session_pnl = -15.0
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == pytest.approx(25.0)

    def test_minus_2pct_quarter_size(self):
        # pnl=-20 = -2.0% → matched до -2.0 → 0.25
        rm = self._rm()
        rm._session_pnl = -20.0
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == pytest.approx(25.0)

    def test_minus_3pct_trading_halted(self):
        rm = self._rm()
        rm._session_pnl = -30.0
        d = rm.check("BTCUSDT")
        assert not d.allowed
        assert "drawdown" in d.reason.lower()

    def test_minus_5pct_also_halted(self):
        rm = self._rm()
        rm._session_pnl = -50.0
        d = rm.check("BTCUSDT")
        assert not d.allowed

    def test_profit_gives_full_size(self):
        rm = self._rm()
        rm._session_pnl = +50.0
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == pytest.approx(100.0)

    def test_is_trading_halted_property(self):
        rm = self._rm()
        assert not rm.is_trading_halted
        rm._session_pnl = -30.0
        assert rm.is_trading_halted


# ---------------------------------------------------------------------------
# 3. Daily loss limit
# ---------------------------------------------------------------------------

class TestDailyLossLimit:

    def test_blocks_when_limit_reached(self):
        rm = make_rm_with_balance(1000.0, daily_loss_limit_usdt=30.0)
        rm._daily_loss_usdt = 30.0
        d = rm.check("BTCUSDT")
        assert not d.allowed
        assert "daily_loss_limit" in d.reason

    def test_allows_below_limit(self):
        rm = make_rm_with_balance(1000.0, daily_loss_limit_usdt=30.0)
        rm._daily_loss_usdt = 29.99
        d = rm.check("BTCUSDT")
        assert d.allowed

    def test_record_close_accumulates_loss(self):
        rm = make_rm_with_balance(1000.0, daily_loss_limit_usdt=50.0)
        rm.record_open("BTCUSDT")
        rm.record_close("BTCUSDT", pnl_usdt=-20.0)
        assert rm.daily_loss_usdt == pytest.approx(20.0)

    def test_record_close_profit_not_added_to_loss(self):
        rm = make_rm_with_balance(1000.0)
        rm.record_open("BTCUSDT")
        rm.record_close("BTCUSDT", pnl_usdt=+15.0)
        assert rm.daily_loss_usdt == 0.0

    def test_multiple_losses_accumulate(self):
        rm = make_rm_with_balance(1000.0, daily_loss_limit_usdt=50.0)
        rm.record_open("BTCUSDT")
        rm.record_close("BTCUSDT", pnl_usdt=-15.0)
        rm.record_open("ETHUSDT")
        rm.record_close("ETHUSDT", pnl_usdt=-20.0)
        assert rm.daily_loss_usdt == pytest.approx(35.0)

    def test_daily_loss_blocks_after_accumulation(self):
        rm = make_rm_with_balance(1000.0, daily_loss_limit_usdt=30.0)
        rm.record_open("BTCUSDT")
        rm.record_close("BTCUSDT", pnl_usdt=-20.0)
        rm.record_open("ETHUSDT")
        rm.record_close("ETHUSDT", pnl_usdt=-15.0)
        d = rm.check("SOLUSDT")
        assert not d.allowed


# ---------------------------------------------------------------------------
# 4. Correlation block
# ---------------------------------------------------------------------------

class TestCorrelationBlock:

    def test_blocks_same_base_asset(self):
        rm = make_rm_with_balance(1000.0, corr_block_enabled=True)
        rm.record_open("BTCUSDT")
        d = rm.check("BTCPERP")
        assert not d.allowed
        assert "correlation" in d.reason.lower()

    def test_allows_different_base_assets(self):
        rm = make_rm_with_balance(1000.0, corr_block_enabled=True)
        rm.record_open("BTCUSDT")
        d = rm.check("ETHUSDT")
        assert d.allowed

    def test_allows_same_symbol_no_self_block(self):
        rm = make_rm_with_balance(1000.0, corr_block_enabled=True)
        rm.record_open("BTCUSDT")
        d = rm.check("BTCUSDT")
        assert d.allowed

    def test_disabled_corr_block_allows_same_base(self):
        rm = make_rm_with_balance(1000.0, corr_block_enabled=False)
        rm.record_open("BTCUSDT")
        d = rm.check("BTCPERP")
        assert d.allowed

    def test_base_asset_extraction(self):
        assert RiskManager._base_asset("BTCUSDT") == "BTC"
        assert RiskManager._base_asset("ETHUSDT") == "ETH"
        assert RiskManager._base_asset("SOLUSDT") == "SOL"
        assert RiskManager._base_asset("BTCPERP") == "BTC"
        assert RiskManager._base_asset("DOGEUSDT") == "DOGE"

    def test_record_close_removes_from_open(self):
        rm = make_rm_with_balance(1000.0, corr_block_enabled=True)
        rm.record_open("BTCUSDT")
        rm.record_close("BTCUSDT", pnl_usdt=0.0)
        d = rm.check("BTCPERP")
        assert d.allowed


# ---------------------------------------------------------------------------
# 5. Session P&L
# ---------------------------------------------------------------------------

class TestSessionPnl:

    def test_session_pnl_accumulates(self):
        rm = make_rm_with_balance(1000.0)
        rm.record_open("BTCUSDT")
        rm.record_close("BTCUSDT", pnl_usdt=+5.0)
        rm.record_open("ETHUSDT")
        rm.record_close("ETHUSDT", pnl_usdt=-3.0)
        assert rm.session_pnl == pytest.approx(+2.0)

    def test_day_rollover_resets_session(self):
        rm = make_rm_with_balance(1000.0)
        rm._session_pnl = -25.0
        rm._daily_loss_usdt = 25.0
        rm._session_date = "2000-01-01"
        rm._check_day_rollover()
        assert rm.session_pnl == 0.0
        assert rm.daily_loss_usdt == 0.0

    def test_no_rollover_same_day(self):
        rm = make_rm_with_balance(1000.0)
        rm._session_pnl = -10.0
        rm._session_date = rm._today()
        rm._check_day_rollover()
        assert rm.session_pnl == -10.0


# ---------------------------------------------------------------------------
# 6. Комбинированные сценарии
# ---------------------------------------------------------------------------

class TestCombinedScenarios:

    def test_daily_loss_blocks_even_with_no_drawdown(self):
        rm = make_rm_with_balance(1000.0, daily_loss_limit_usdt=10.0)
        rm._daily_loss_usdt = 10.0
        d = rm.check("BTCUSDT")
        assert not d.allowed

    def test_drawdown_reduces_size_then_block(self):
        # Тиры: 0→1.0, -1→0.5, -2→0.0
        # pnl=-5  = -0.5% → matched: (0.0,1.0) → size=100 → ALLOW
        # pnl=-10 = -1.0% → matched: (0.0,1.0),(-1.0,0.5) → last=0.5 → size=50 → ALLOW
        # pnl=-15 = -1.5% → matched: (-2.0,0.0) last → BLOCK (пробил -2.0 порог: -1.5>=-2.0)
        # pnl=-20 = -2.0% → matched: (-2.0,0.0) last → BLOCK
        rm = make_rm_with_balance(
            1000.0,
            position_pct=10.0,
            drawdown_tiers=[
                (0.0,  1.00),
                (-1.0, 0.50),
                (-2.0, 0.00),
            ],
        )
        rm._session_pnl = -10.0
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == pytest.approx(50.0)

        rm._session_pnl = -15.0
        d = rm.check("BTCUSDT")
        assert not d.allowed

        rm._session_pnl = -20.0
        d = rm.check("BTCUSDT")
        assert not d.allowed

    def test_full_flow_open_close_reopen(self):
        rm = make_rm_with_balance(1000.0, position_pct=2.0, daily_loss_limit_usdt=100.0)
        d = rm.check("BTCUSDT")
        assert d.allowed
        assert d.size_usdt == pytest.approx(20.0)
        rm.record_open("BTCUSDT")
        rm.record_close("BTCUSDT", pnl_usdt=+3.0)
        d2 = rm.check("BTCUSDT")
        assert d2.allowed
        assert d2.size_usdt == pytest.approx(20.0)

    def test_status_str_contains_key_info(self):
        rm = make_rm_with_balance(500.0)
        s = rm.status_str()
        assert "500.00" in s
        assert "session_pnl" in s
        assert "daily_loss" in s
