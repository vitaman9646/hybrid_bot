"""
MTFDirectionFilter: EMA(20) на 15m и 1h klines определяет направление тренда.
Блокирует входы против тренда на масштабе часов.
"""
import time
import asyncio
import logging
from typing import Optional
from enum import Enum

logger = logging.getLogger(__name__)


class MTFBias(Enum):
    LONG   = "long"
    SHORT  = "short"
    NEUTRAL = "neutral"


class MTFDirectionFilter:
    """
    Каждые 5 минут запрашивает klines через REST API.
    Считает EMA(20) на 15m и 1h.
    Определяет bias и силу тренда.
    """

    def __init__(self, client, symbols: list[str], update_interval: int = 600):
        self._client = client
        self._symbols = symbols
        self._update_interval = update_interval
        self._bias: dict[str, MTFBias] = {}      # symbol → bias
        self._strength: dict[str, float] = {}    # symbol → 0.0-1.0
        self._last_update: dict[str, float] = {}
        self._running = False

    def get_bias(self, symbol: str) -> MTFBias:
        return self._bias.get(symbol, MTFBias.NEUTRAL)

    def get_strength(self, symbol: str) -> float:
        return self._strength.get(symbol, 0.0)

    def is_blocked(self, symbol: str, direction: str, scenario: str = None) -> bool:
        """
        Проверяем блок входа.
        S3 (mean reversion) — инвертированная логика, блок если В направлении тренда.
        """
        bias = self.get_bias(symbol)
        strength = self.get_strength(symbol)

        if bias == MTFBias.NEUTRAL:
            return False

        is_mean_reversion = scenario in ('averages_depth', 's3')

        if is_mean_reversion:
            # S3 торгует против тренда — блокируем если тренд слабый
            # (нет смысла торговать mean reversion в сильном тренде)
            if strength >= 0.6 and bias == MTFBias.LONG and direction == 'long':
                return True
            if strength >= 0.6 and bias == MTFBias.SHORT and direction == 'short':
                return True
            return False
        else:
            # Обычные сценарии — блок если против сильного тренда
            if strength >= 0.6 and bias == MTFBias.LONG and direction == 'short':
                return True
            if strength >= 0.6 and bias == MTFBias.SHORT and direction == 'long':
                return True
            return False

    def get_score_multiplier(self, symbol: str, direction: str) -> float:
        """Score multiplier: по тренду +10%, против слабого тренда -30%"""
        bias = self.get_bias(symbol)
        strength = self.get_strength(symbol)

        if bias == MTFBias.NEUTRAL:
            return 1.0

        if (bias == MTFBias.LONG and direction == 'long') or \
           (bias == MTFBias.SHORT and direction == 'short'):
            return 1.1  # по тренду

        if strength < 0.6:
            return 0.7  # против слабого тренда — penalty
        return 1.0

    async def start(self):
        """Запуск фонового обновления"""
        self._running = True
        asyncio.create_task(self._update_loop())
        logger.info("MTFDirectionFilter started for %s", self._symbols)

    async def stop(self):
        self._running = False

    async def _update_loop(self):
        while self._running:
            for symbol in self._symbols:
                try:
                    await self._update_symbol(symbol)
                except Exception as e:
                    logger.warning("MTF update error [%s]: %s", symbol, e)
                await asyncio.sleep(3.0)  # rate limit protection
            await asyncio.sleep(self._update_interval)

    async def _update_symbol(self, symbol: str):
        """Обновляем EMA для одного символа"""
        loop = asyncio.get_event_loop()

        # 15m klines
        ema15 = await loop.run_in_executor(
            None, self._fetch_ema, symbol, '15', 20
        )
        # 1h klines
        ema1h = await loop.run_in_executor(
            None, self._fetch_ema, symbol, '60', 20
        )

        if ema15 is None or ema1h is None:
            return

        # Определяем bias
        bullish15 = ema15['trend'] == 'up'
        bullish1h = ema1h['trend'] == 'up'

        if bullish15 and bullish1h:
            self._bias[symbol] = MTFBias.LONG
            self._strength[symbol] = 1.0
        elif not bullish15 and not bullish1h:
            self._bias[symbol] = MTFBias.SHORT
            self._strength[symbol] = 1.0
        elif bullish1h:
            self._bias[symbol] = MTFBias.LONG
            self._strength[symbol] = 0.6
        elif bullish15:
            self._bias[symbol] = MTFBias.SHORT
            self._strength[symbol] = 0.6
        else:
            self._bias[symbol] = MTFBias.NEUTRAL
            self._strength[symbol] = 0.0

        self._last_update[symbol] = time.time()
        logger.debug(
            "MTF [%s]: bias=%s strength=%.1f ema15=%s ema1h=%s",
            symbol, self._bias[symbol].value, self._strength[symbol],
            ema15['trend'], ema1h['trend']
        )

    def _fetch_ema(self, symbol: str, interval: str, period: int) -> Optional[dict]:
        """Синхронный запрос klines и расчёт EMA"""
        try:
            result = self._client.get_kline(
                category='linear',
                symbol=symbol,
                interval=interval,
                limit=period + 5,
            )
            candles = result.get('result', {}).get('list', [])
            if len(candles) < period:
                return None

            # Bybit возвращает [ts, open, high, low, close, volume, turnover]
            closes = [float(c[4]) for c in reversed(candles)]

            # EMA расчёт
            k = 2 / (period + 1)
            ema = closes[0]
            for price in closes[1:]:
                ema = price * k + ema * (1 - k)

            current_price = closes[-1]
            trend = 'up' if current_price > ema else 'down'

            return {'ema': ema, 'price': current_price, 'trend': trend}
        except Exception as e:
            logger.warning("MTF fetch error [%s %s]: %s", symbol, interval, e)
            return None

    def get_stats(self) -> dict:
        return {
            symbol: {
                'bias': self._bias.get(symbol, MTFBias.NEUTRAL).value,
                'strength': self._strength.get(symbol, 0.0),
                'last_update': self._last_update.get(symbol, 0),
            }
            for symbol in self._symbols
        }
