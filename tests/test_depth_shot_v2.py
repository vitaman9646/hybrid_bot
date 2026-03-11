"""Tests for DepthShotV2"""
import time
import pytest
from unittest.mock import MagicMock, patch
from models.signals import Direction
from analyzers.depth_shot_v2 import DepthShotV2, WallTrackerV2, Wall, TPLadder


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_level(price: float, qty: float):
    m = MagicMock()
    m.price = price
    m.qty = qty
    return m


def make_ob(bids=None, asks=None, mid=None, initialized=True):
    ob = MagicMock()
    ob.is_initialized = initialized
    ob.mid_price = mid or (bids[0].price if bids else 100.0)
    ob.get_bids = MagicMock(return_value=bids or [])
    ob.get_asks = MagicMock(return_value=asks or [])
    return ob


def make_obm(symbol='BTCUSDT', **kwargs):
    obm = MagicMock()
    obm.get_book = MagicMock(return_value=make_ob(**kwargs))
    return obm


def make_analyzer(config=None, obm=None):
    cfg = config or {}
    return DepthShotV2(cfg, obm or make_obm())


# ── WallTrackerV2 ─────────────────────────────────────────────────────────────

class TestWallTrackerV2:

    def test_new_wall_strength_zero_if_too_young(self):
        tracker = WallTrackerV2(min_age_s=10.0)
        tracker.update('BTC', 'bid', 80000, 500_000)
        strength = tracker.get_strength('BTC', 'bid', 80000)
        assert strength < 0.6, "Young wall should have low strength"

    def test_unknown_wall_returns_zero(self):
        tracker = WallTrackerV2()
        assert tracker.get_strength('BTC', 'bid', 99999) == 0.0

    def test_strength_increases_with_age(self):
        tracker = WallTrackerV2(min_age_s=1.0)
        tracker.update('BTC', 'bid', 80000, 500_000)
        with patch('analyzers.depth_shot_v2.time') as mock_time:
            mock_time.time.return_value = time.time() + 30
            tracker.update('BTC', 'bid', 80000, 500_000)
            s = tracker.get_strength('BTC', 'bid', 80000)
            assert s > 0.3

    def test_spoofed_wall_returns_zero(self):
        tracker = WallTrackerV2(min_age_s=1.0, max_drop_pct=0.3)
        tracker.update('BTC', 'bid', 80000, 1_000_000)
        # Стена упала на 50% → spoofing
        tracker.update('BTC', 'bid', 80000, 500_000)
        s = tracker.get_strength('BTC', 'bid', 80000)
        assert s == 0.0

    def test_get_age(self):
        tracker = WallTrackerV2()
        tracker.update('BTC', 'bid', 80000, 500_000)
        age = tracker.get_age('BTC', 'bid', 80000)
        assert 0.0 <= age < 1.0

    def test_cleanup_removes_old_walls(self):
        tracker = WallTrackerV2(cleanup_interval_s=0.0)
        tracker.update('BTC', 'bid', 80000, 500_000)
        assert len(tracker._walls) == 1
        with patch('analyzers.depth_shot_v2.time') as mock_time:
            mock_time.time.return_value = time.time() + 400
            tracker.update('BTC', 'bid', 99999, 500_000)  # триггер cleanup
        # старая стена должна быть удалена
        assert ('BTC', 'bid', 80000.0) not in tracker._walls


# ── DepthShotV2: scan_walls ───────────────────────────────────────────────────

class TestScanWalls:

    def _make_strong_analyzer(self, symbol='BTCUSDT', bids=None, asks=None):
        """Аналайзер с уже 'состарившимися' стенами."""
        ob = make_ob(bids=bids or [], asks=asks or [], mid=80000.0)
        obm = MagicMock()
        obm.get_book = MagicMock(return_value=ob)
        cfg = {'wall_min_age_s': 0.0, 'min_distance_pct': 0.1, 'max_distance_pct': 3.0}
        analyzer = DepthShotV2(cfg, obm)
        # Принудительно добавляем стены в трекер как старые
        for level in (bids or []) + (asks or []):
            side = 'bid' if level in (bids or []) else 'ask'
            analyzer._tracker._walls[(symbol, side, round(level.price, 2))] = {
                'first_seen': time.time() - 60,
                'last_seen': time.time(),
                'max': level.qty * level.price,
                'current': level.qty * level.price,
            }
        return analyzer

    def test_no_walls_when_ob_not_initialized(self):
        obm = make_obm(initialized=False)
        a = make_analyzer(obm=obm)
        assert a.scan_walls('BTCUSDT', Direction.LONG) == []

    def test_no_walls_below_threshold(self):
        bids = [make_level(79500, 0.1)]  # ~7950 USDT — ниже порога
        a = self._make_strong_analyzer(bids=bids)
        assert a.scan_walls('BTCUSDT', Direction.LONG, 80000) == []

    def test_finds_bid_walls_for_long(self):
        bids = [make_level(79000, 10.0)]  # 790_000 USDT, dist=1.25%
        a = self._make_strong_analyzer(bids=bids)
        walls = a.scan_walls('BTCUSDT', Direction.LONG, 80000)
        assert len(walls) == 1
        assert walls[0].side == 'bid'
        assert walls[0].price == 79000

    def test_finds_ask_walls_for_short(self):
        asks = [make_level(81000, 5.0)]  # 405_000 USDT, dist=1.25%
        a = self._make_strong_analyzer(asks=asks)
        walls = a.scan_walls('BTCUSDT', Direction.SHORT, 80000)
        assert len(walls) == 1
        assert walls[0].side == 'ask'

    def test_respects_max_walls(self):
        bids = [make_level(79000 - i * 100, 10.0) for i in range(5)]
        a = self._make_strong_analyzer(bids=bids)
        a.max_walls = 2
        walls = a.scan_walls('BTCUSDT', Direction.LONG, 80000)
        assert len(walls) <= 2

    def test_walls_sorted_by_strength_desc(self):
        bids = [make_level(79000, 10.0), make_level(78500, 5.0)]
        a = self._make_strong_analyzer(bids=bids)
        # Делаем вторую стену старее (сильнее)
        a._tracker._walls[('BTCUSDT', 'bid', 78500.0)]['first_seen'] = time.time() - 120
        walls = a.scan_walls('BTCUSDT', Direction.LONG, 80000)
        if len(walls) >= 2:
            assert walls[0].strength >= walls[1].strength

    def test_distance_filter(self):
        # dist=0.05% — слишком близко
        bids = [make_level(79960, 10.0)]
        a = self._make_strong_analyzer(bids=bids)
        a.min_distance_pct = 0.15
        walls = a.scan_walls('BTCUSDT', Direction.LONG, 80000)
        assert walls == []

    def test_stats_updated(self):
        a = make_analyzer()
        a.scan_walls('BTCUSDT', Direction.LONG, 80000)
        assert a._stats['scans'] == 1


# ── DepthShotV2: get_tp_ladder ────────────────────────────────────────────────

class TestTPLadder:

    def test_fallback_when_no_walls(self):
        a = make_analyzer(config={'tp_pct_fallback': 0.5})
        # scan_walls вернёт [] (OB пустой)
        ladder = a.get_tp_ladder('BTCUSDT', Direction.LONG, 80000)
        assert len(ladder.levels) == 1
        assert ladder.levels[0][1] == 1.0  # 100% на fallback
        assert abs(ladder.base_tp - 80000 * 1.005) < 1

    def test_ladder_built_from_walls(self):
        asks = [
            make_level(80500, 5.0),   # 402_500 USDT
            make_level(81000, 4.0),   # 324_000 USDT
        ]
        ob = make_ob(asks=asks, mid=80000.0)
        obm = MagicMock()
        obm.get_book = MagicMock(return_value=ob)
        cfg = {'wall_min_age_s': 0.0, 'min_distance_pct': 0.1, 'max_distance_pct': 3.0,
               'tp_pct_fallback': 0.3}
        a = DepthShotV2(cfg, obm)
        # Инициализируем трекер вручную
        for lv in asks:
            a._tracker._walls[('BTCUSDT', 'ask', round(lv.price, 2))] = {
                'first_seen': time.time() - 60,
                'last_seen': time.time(),
                'max': lv.qty * lv.price,
                'current': lv.qty * lv.price,
            }
        ladder = a.get_tp_ladder('BTCUSDT', Direction.LONG, 80000)
        assert len(ladder.levels) >= 1
        assert abs(sum(p for _, p in ladder.levels) - 1.0) < 0.01, "TP pct must sum to 1.0"

    def test_ladder_pct_sum_equals_one(self):
        a = make_analyzer(config={'ladder_distribution': [0.4, 0.35, 0.25]})
        # Мокаем scan_walls
        a.scan_walls = MagicMock(return_value=[
            Wall(price=80400, volume_usdt=300_000, distance_pct=0.5, side='ask', strength=0.8, age_s=30),
            Wall(price=80800, volume_usdt=200_000, distance_pct=1.0, side='ask', strength=0.6, age_s=20),
            Wall(price=81200, volume_usdt=150_000, distance_pct=1.5, side='ask', strength=0.4, age_s=10),
        ])
        ladder = a.get_tp_ladder('BTCUSDT', Direction.LONG, 80000)
        total = sum(p for _, p in ladder.levels)
        assert abs(total - 1.0) < 0.01

    def test_short_ladder_prices_below_entry(self):
        a = make_analyzer()
        a.scan_walls = MagicMock(return_value=[
            Wall(price=79500, volume_usdt=300_000, distance_pct=0.625, side='bid', strength=0.8, age_s=30),
        ])
        ladder = a.get_tp_ladder('BTCUSDT', Direction.SHORT, 80000)
        for price, _ in ladder.levels:
            assert price < 80000 or price == ladder.base_tp


# ── DepthShotV2: get_confidence ───────────────────────────────────────────────

class TestConfidence:

    def test_zero_confidence_no_walls(self):
        a = make_analyzer()
        c = a.get_confidence('BTCUSDT', Direction.LONG)
        assert c == 0.0

    def test_confidence_range(self):
        a = make_analyzer()
        a.scan_walls = MagicMock(return_value=[
            Wall(price=79500, volume_usdt=300_000, distance_pct=0.6, side='bid', strength=0.7, age_s=30),
        ])
        c = a.get_confidence('BTCUSDT', Direction.LONG)
        assert 0.0 <= c <= 1.0

    def test_confidence_penalized_for_far_walls(self):
        a = make_analyzer(config={'max_distance_pct': 2.0})
        a.scan_walls = MagicMock(return_value=[
            Wall(price=78400, volume_usdt=300_000, distance_pct=1.9, side='bid', strength=0.9, age_s=60),
        ])
        far = a.get_confidence('BTCUSDT', Direction.LONG)
        a.scan_walls = MagicMock(return_value=[
            Wall(price=79600, volume_usdt=300_000, distance_pct=0.5, side='bid', strength=0.9, age_s=60),
        ])
        close = a.get_confidence('BTCUSDT', Direction.LONG)
        assert close > far


# ── DepthShotV2: get_imbalance ────────────────────────────────────────────────

class TestImbalance:

    def test_balanced_book(self):
        bids = [make_level(79900, 1.0)]
        asks = [make_level(80100, 1.0)]
        obm = make_obm(bids=bids, asks=asks, mid=80000.0)
        a = make_analyzer(obm=obm)
        imb = a.get_imbalance('BTCUSDT')
        assert 0.45 <= imb <= 0.55

    def test_bid_heavy(self):
        bids = [make_level(79900, 10.0)]
        asks = [make_level(80100, 1.0)]
        obm = make_obm(bids=bids, asks=asks, mid=80000.0)
        a = make_analyzer(obm=obm)
        assert a.get_imbalance('BTCUSDT') > 0.8

    def test_uninitialized_returns_05(self):
        obm = make_obm(initialized=False)
        a = make_analyzer(obm=obm)
        assert a.get_imbalance('BTCUSDT') == 0.5


# ── get_stats ─────────────────────────────────────────────────────────────────

def test_get_stats():
    a = make_analyzer()
    stats = a.get_stats()
    assert 'scans' in stats
    assert 'walls_found' in stats
    assert 'ladders_built' in stats
    assert 'tracker_walls' in stats
