"""
ScoreDecay — экспоненциальное затухание score сигнала со временем
После генерации сигнала его confidence снижается по формуле:
    score(t) = score0 * e^(-t/tau)
где tau (half-life) зависит от сценария.
"""
import math
import time
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Half-life в секундах для каждого сценария
TAU = {
    'all_three':       30.0,   # быстро протухает — импульсный
    'averages_vector': 45.0,
    'vector_depth':    20.0,   # самый быстрый — depth меняется быстро
    'averages_depth':  60.0,   # медленнее — MA тренд устойчивее
}
DEFAULT_TAU = 30.0
MIN_CONFIDENCE = 0.10   # ниже этого — сигнал мёртв

@dataclass
class DecayedSignal:
    original_confidence: float
    current_confidence: float
    age_sec: float
    is_alive: bool

class ScoreDecay:
    def __init__(self):
        self._signal_times: dict[str, float] = {}  # symbol -> ts создания сигнала

    def register_signal(self, symbol: str):
        """Регистрируем момент появления сигнала"""
        self._signal_times[symbol] = time.time()

    def get_decayed(self, symbol: str, scenario: str, original_confidence: float) -> DecayedSignal:
        """Возвращает текущий confidence с учётом затухания"""
        if symbol not in self._signal_times:
            return DecayedSignal(original_confidence, original_confidence, 0.0, True)

        age = time.time() - self._signal_times[symbol]
        tau = TAU.get(scenario, DEFAULT_TAU)
        decayed = original_confidence * math.exp(-age / tau)

        is_alive = decayed >= MIN_CONFIDENCE
        return DecayedSignal(original_confidence, decayed, age, is_alive)

    def clear(self, symbol: str):
        """Очищаем после входа или отмены"""
        self._signal_times.pop(symbol, None)

    def apply(self, symbol: str, scenario: str, confidence: float) -> float:
        """Быстрый метод — регистрирует если новый, возвращает decayed confidence"""
        if symbol not in self._signal_times:
            self.register_signal(symbol)
        result = self.get_decayed(symbol, scenario, confidence)
        if not result.is_alive:
            logger.debug("ScoreDecay: signal DEAD [%s] age=%.1fs confidence=%.3f",
                symbol, result.age_sec, result.current_confidence)
            self.clear(symbol)
        return result.current_confidence if result.is_alive else 0.0
