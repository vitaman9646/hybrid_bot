"""Tests for RealisticTPLadder"""
import time
import pytest
from unittest.mock import MagicMock
from models.signals import Direction
from core.tp_ladder import RealisticTPLadder, TPLevel


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_depth(levels=None):
    """Мок DepthShotV2.get_tp_ladder()"""
    depth = MagicMock()
    ladder = MagicMock()
    ladder.levels = levels or [(80400, 0.40), (80800, 0.35), (81200, 0.25)]
    depth.get_tp_ladder = MagicMock(return_value=ladder)
    return depth


# ── from_depth ────────────────────────────────────────────────────────────────

class TestFromDepth:

    def test_creates_correct_levels(self):
        depth = make_depth([(80400, 0.40), (80800, 0.60)])
        ladder = RealisticTPLadder.from_depth(depth, 'BTCUSDT', Direction.LONG, 80000)
        assert len(ladder.levels) == 2
        assert ladder.levels[0].price == 80400
        assert ladder.levels[1].price == 80800

    def test_fractions_match(self):
        depth = make_depth([(80400, 0.40), (80800, 0.35), (81200, 0.25)])
        ladder = RealisticTPLadder.from_depth(depth, 'BTCUSDT', Direction.LONG, 80000)
        assert ladder.levels[0].fraction == 0.40
        assert ladder.levels[1].fraction == 0.35
        assert ladder.levels[2].fraction == 0.25

    def test_labels_set(self):
        depth = make_depth([(80400, 0.5), (80800, 0.5)])
        ladder = RealisticTPLadder.from_depth(depth, 'BTCUSDT', Direction.LONG, 80000)
        assert ladder.levels[0].label == 'wall_1'
        assert ladder.levels[1].label == 'wall_2'

    def test_single_level_labeled_fallback(self):
        depth = make_depth([(80400, 1.0)])
        ladder = RealisticTPLadder.from_depth(depth, 'BTCUSDT', Direction.LONG, 80000)
        assert ladder.levels[0].label == 'fallback'

    def test_symbol_and_direction_set(self):
        depth = make_depth()
        ladder = RealisticTPLadder.from_depth(depth, 'ETHUSDT', Direction.SHORT, 3000)
        assert ladder.symbol == 'ETHUSDT'
        assert ladder.direction == Direction.SHORT
        assert ladder.entry == 3000


# ── fixed ─────────────────────────────────────────────────────────────────────

class TestFixed:

    def test_long_prices_above_entry(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         tp_pcts=[0.3, 0.6, 1.0],
                                         fractions=[0.4, 0.35, 0.25])
        for level in ladder.levels:
            assert level.price > 80000

    def test_short_prices_below_entry(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.SHORT, 80000,
                                         tp_pcts=[0.3, 0.6, 1.0],
                                         fractions=[0.4, 0.35, 0.25])
        for level in ladder.levels:
            assert level.price < 80000

    def test_fractions_sum_to_one(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         fractions=[0.4, 0.35, 0.25])
        total = sum(l.fraction for l in ladder.levels)
        assert abs(total - 1.0) < 0.01

    def test_default_params(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        assert len(ladder.levels) == 3

    def test_labels(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        assert ladder.levels[0].label == 'fixed_1'
        assert ladder.levels[2].label == 'fixed_3'


# ── get_hits ──────────────────────────────────────────────────────────────────

class TestGetHits:

    def test_long_hit_when_price_above_tp(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         tp_pcts=[0.3], fractions=[1.0])
        tp_price = ladder.levels[0].price  # ~80240
        hits = ladder.get_hits(tp_price + 1)
        assert len(hits) == 1

    def test_long_no_hit_below_tp(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         tp_pcts=[0.3], fractions=[1.0])
        hits = ladder.get_hits(80000)
        assert hits == []

    def test_short_hit_when_price_below_tp(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.SHORT, 80000,
                                         tp_pcts=[0.3], fractions=[1.0])
        tp_price = ladder.levels[0].price  # ~79760
        hits = ladder.get_hits(tp_price - 1)
        assert len(hits) == 1

    def test_done_levels_not_returned(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         tp_pcts=[0.3], fractions=[1.0])
        ladder.levels[0].done = True
        hits = ladder.get_hits(99999)
        assert hits == []

    def test_multiple_hits(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         tp_pcts=[0.3, 0.6, 1.0],
                                         fractions=[0.4, 0.35, 0.25])
        # Цена выше всех трёх уровней
        hits = ladder.get_hits(81000)
        assert len(hits) == 3


# ── mark_done ─────────────────────────────────────────────────────────────────

class TestMarkDone:

    def test_marks_level_done(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        level = ladder.levels[0]
        ladder.mark_done(level, actual_price=80250)
        assert level.done is True
        assert level.hit_price == 80250

    def test_hit_time_set(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        level = ladder.levels[0]
        before = time.time()
        ladder.mark_done(level)
        assert level.hit_time >= before

    def test_default_hit_price(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         tp_pcts=[0.5], fractions=[1.0])
        level = ladder.levels[0]
        ladder.mark_done(level)
        assert level.hit_price == level.price


# ── remaining_fraction ────────────────────────────────────────────────────────

class TestRemainingFraction:

    def test_full_at_start(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         fractions=[0.4, 0.35, 0.25])
        assert abs(ladder.remaining_fraction() - 1.0) < 0.01

    def test_decreases_after_mark_done(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         fractions=[0.4, 0.35, 0.25])
        ladder.mark_done(ladder.levels[0])
        assert abs(ladder.remaining_fraction() - 0.60) < 0.01

    def test_zero_when_all_done(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000,
                                         fractions=[0.4, 0.35, 0.25])
        for l in ladder.levels:
            ladder.mark_done(l)
        assert ladder.remaining_fraction() == 0.0


# ── is_complete / next_level ──────────────────────────────────────────────────

class TestCompletion:

    def test_not_complete_at_start(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        assert ladder.is_complete() is False

    def test_complete_when_all_done(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        for l in ladder.levels:
            l.done = True
        assert ladder.is_complete() is True

    def test_next_level_returns_first_undone(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        ladder.levels[0].done = True
        assert ladder.next_level() == ladder.levels[1]

    def test_next_level_none_when_complete(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        for l in ladder.levels:
            l.done = True
        assert ladder.next_level() is None


# ── breakeven ─────────────────────────────────────────────────────────────────

class TestBreakeven:

    def test_long_breakeven_above_entry(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        be = ladder.breakeven_price()
        assert be > 80000

    def test_short_breakeven_below_entry(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.SHORT, 80000)
        be = ladder.breakeven_price()
        assert be < 80000

    def test_should_move_false_at_start(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        assert ladder.should_move_to_breakeven() is False

    def test_should_move_true_after_first_hit(self):
        ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
        ladder.mark_done(ladder.levels[0])
        assert ladder.should_move_to_breakeven() is True


# ── summary ───────────────────────────────────────────────────────────────────

def test_summary():
    ladder = RealisticTPLadder.fixed('BTCUSDT', Direction.LONG, 80000)
    s = ladder.summary()
    assert s['symbol'] == 'BTCUSDT'
    assert s['direction'] == 'long'
    assert len(s['levels']) == 3
    assert 'remaining_fraction' in s
    assert 'is_complete' in s
