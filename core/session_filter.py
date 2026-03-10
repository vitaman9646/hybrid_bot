"""
SessionFilter — торговые сессии с разными правилами
Asia:   06-12 UTC — низкая волатильность, только S3 (mean reversion)
London: 12-16 UTC — высокая волатильность, все сценарии
NY:     16-21 UTC — высокая волатильность, все сценарии
Quiet:  21-02 UTC — средняя, только S4 (all_three) с повышенным порогом
Dead:   02-06 UTC — заблокировано полностью
"""
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

SESSION_RULES = {
    'DEAD':   {'allowed': False, 'scenarios': [], 'score_mult': 0.0},
    'QUIET':  {'allowed': True,  'scenarios': ['all_three'], 'score_mult': 0.85},
    'ASIA':   {'allowed': True,  'scenarios': ['all_three', 'averages_depth'], 'score_mult': 0.90},
    'LONDON': {'allowed': True,  'scenarios': ['all_three', 'averages_vector', 'averages_depth', 'vector_depth'], 'score_mult': 1.0},
    'NY':     {'allowed': True,  'scenarios': ['all_three', 'averages_vector', 'averages_depth', 'vector_depth'], 'score_mult': 1.0},
}

class SessionFilter:
    def get_session(self, ts: float) -> str:
        h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        if 2 <= h < 6:   return 'DEAD'
        elif 6 <= h < 12: return 'ASIA'
        elif 12 <= h < 16: return 'LONDON'
        elif 16 <= h < 21: return 'NY'
        else: return 'QUIET'

    def is_allowed(self, ts: float, scenario: str) -> tuple[bool, float]:
        """Возвращает (allowed, score_multiplier)"""
        session = self.get_session(ts)
        rules = SESSION_RULES[session]
        if not rules['allowed']:
            return False, 0.0
        if scenario not in rules['scenarios']:
            return False, 0.0
        return True, rules['score_mult']

    def get_score_multiplier(self, ts: float) -> float:
        session = self.get_session(ts)
        return SESSION_RULES[session]['score_mult']
