import pytest
from datetime import datetime, timezone
from core.session_filter import SessionFilter

@pytest.fixture
def sf():
    return SessionFilter()

def ts(hour):
    """UTC timestamp для заданного часа"""
    return datetime(2026, 3, 10, hour, 0, 0, tzinfo=timezone.utc).timestamp()

def test_dead_session(sf):
    assert sf.get_session(ts(3)) == 'DEAD'
    allowed, mult = sf.is_allowed(ts(3), 'all_three')
    assert not allowed
    assert mult == 0.0

def test_asia_session(sf):
    assert sf.get_session(ts(8)) == 'ASIA'
    allowed, mult = sf.is_allowed(ts(8), 'all_three')
    assert allowed
    assert mult == 0.90
    # averages_vector заблокирован в Азии
    allowed2, _ = sf.is_allowed(ts(8), 'averages_vector')
    assert not allowed2

def test_london_session(sf):
    assert sf.get_session(ts(13)) == 'LONDON'
    for sc in ['all_three', 'averages_vector', 'averages_depth', 'vector_depth']:
        allowed, mult = sf.is_allowed(ts(13), sc)
        assert allowed
        assert mult == 1.0

def test_ny_session(sf):
    assert sf.get_session(ts(18)) == 'NY'
    allowed, mult = sf.is_allowed(ts(18), 'averages_vector')
    assert allowed
    assert mult == 1.0

def test_quiet_session(sf):
    assert sf.get_session(ts(22)) == 'QUIET'
    allowed, mult = sf.is_allowed(ts(22), 'all_three')
    assert allowed
    assert mult == 0.85
    allowed2, _ = sf.is_allowed(ts(22), 'averages_vector')
    assert not allowed2

def test_score_multiplier(sf):
    assert sf.get_score_multiplier(ts(3))  == 0.0
    assert sf.get_score_multiplier(ts(8))  == 0.90
    assert sf.get_score_multiplier(ts(13)) == 1.0
    assert sf.get_score_multiplier(ts(18)) == 1.0
    assert sf.get_score_multiplier(ts(22)) == 0.85
