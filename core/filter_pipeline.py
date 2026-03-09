from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

from pybit.unified_trading import HTTP

if TYPE_CHECKING:
    from models.signals import Signal

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    passed: bool
    reason: str = ""
    confidence_multiplier: float = 1.0  # для penalty фильтров
    details: dict = field(default_factory=dict)

    def __str__(self):
        if self.passed:
            return f"PASS (x{self.confidence_multiplier:.2f})"
        return f"BLOCK [{self.reason}]"


@dataclass
class _SymbolDirection:
    symbol: str
    direction: str  # 'long' / 'short'
    timestamp: float = field(default_factory=time.time)


class FilterPipeline:
    """
    Фаза 3: Дополнительные фильтры перед открытием позиции.

    Hard block (сигнал убивается):
        1. TimeOfDayFilter   — запрет 02:00-06:00 UTC
        2. FundingRateFilter — |funding| > threshold
        3. MarkPriceFilter   — расхождение mark/last > threshold

    Score penalty (снижает confidence):
        4. DeltaFilter       — слабый CVD → x0.7
        5. VolumeFilter      — малый объём → x0.8
        6. CorrelationFilter — BTC+ETH в одну сторону → x0.6
    """

    def __init__(self, config: dict, http_client: Optional[HTTP] = None, orderbook_manager=None):
        cfg = config.get('filter_pipeline', {})

        # TimeOfDay
        self._time_block_start = cfg.get('time_block_start_utc', 2)   # час
        self._time_block_end = cfg.get('time_block_end_utc', 6)        # час
        self._time_filter_enabled = cfg.get('time_filter_enabled', True)
        self._weekend_filter_enabled = cfg.get('weekend_filter_enabled', True)

        # FundingRate
        self._max_funding_rate = cfg.get('max_funding_rate', 0.001)    # 0.1%
        self._funding_filter_enabled = cfg.get('funding_filter_enabled', True)
        self._funding_cache: dict[str, tuple[float, float]] = {}       # symbol → (rate, ts)
        self._funding_cache_ttl = cfg.get('funding_cache_ttl_sec', 30)

        # MarkPrice
        self._max_mark_deviation_pct = cfg.get('max_mark_deviation_pct', 0.1)
        self._mark_filter_enabled = cfg.get('mark_filter_enabled', True)
        self._mark_cache: dict[str, tuple[float, float]] = {}          # symbol → (mark, ts)
        self._mark_cache_ttl = cfg.get('mark_cache_ttl_sec', 5)

        # Delta (CVD)
        self._min_delta_usdt = cfg.get('min_delta_usdt', 50_000)
        self._delta_filter_enabled = cfg.get('delta_filter_enabled', True)
        self._delta_penalty = cfg.get('delta_penalty', 0.7)
        # delta накапливается извне через add_trade()
        self._delta_window: dict[str, list[tuple[float, float]]] = {}  # symbol → [(ts, delta)]
        self._delta_window_sec = cfg.get('delta_window_sec', 10)

        # Volume
        self._min_volume_usdt = cfg.get('min_volume_usdt', 500_000)
        self._volume_filter_enabled = cfg.get('volume_filter_enabled', True)
        self._volume_penalty = cfg.get('volume_penalty', 0.8)
        self._volume_window: dict[str, list[tuple[float, float]]] = {}  # symbol → [(ts, vol)]
        self._volume_window_sec = cfg.get('volume_window_sec', 10)

        # Correlation
        self._correlation_filter_enabled = cfg.get('correlation_filter_enabled', True)
        self._correlation_penalty = cfg.get('correlation_penalty', 0.6)
        self._correlated_pairs = cfg.get('correlated_pairs', [['BTCUSDT', 'ETHUSDT']])
        self._recent_signals: list[_SymbolDirection] = []
        self._correlation_window_sec = cfg.get('correlation_window_sec', 60)

        # Liquidity filter
        self._liquidity_filter_enabled = cfg.get('liquidity_filter_enabled', True)
        self._min_depth_multiplier = cfg.get('min_depth_multiplier', 2.0)  # depth > size * N
        self._max_spread_pct = cfg.get('max_spread_pct', 0.1)              # 0.1%
        self._orderbook_manager = orderbook_manager

        # HTTP клиент для funding/mark (опционально)
        self._http = http_client

        # v3: Stop-Hunt Detector
        self._stop_hunt_enabled: bool = cfg.get('stop_hunt_filter_enabled', True)
        self._stop_hunt_threshold_pct: float = cfg.get('stop_hunt_threshold_pct', 0.5)
        self._stop_hunt_window_s: float = cfg.get('stop_hunt_window_s', 3.0)
        self._stop_hunt_block_s: float = cfg.get('stop_hunt_block_s', 10.0)
        self._price_history: dict = {}
        self._stop_hunt_blocked_until: dict = {}

        # Stats
        self._stats = {
            'total': 0,
            'blocked': 0,
            'passed': 0,
            'block_reasons': {},
            'penalties_applied': 0,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def check(self, signal: "Signal") -> FilterResult:
        """
        Главный метод. Возвращает FilterResult.
        Если passed=False — сигнал блокируется.
        Если passed=True — signal.confidence умножается на confidence_multiplier.
        """
        self._stats['total'] += 1
        symbol = signal.symbol
        direction = signal.direction.value  # 'long' / 'short'
        price = signal.entry_price

        # === HARD BLOCK FILTERS ===

        # 0. Stop-Hunt Detector
        if self._stop_hunt_enabled:
            self._update_price_history(symbol, price)
            result = self._check_stop_hunt(symbol, price)
            if not result.passed:
                return self._block(result)

        # 1. Time of day
        if self._time_filter_enabled:
            result = self._check_time_of_day()
            if not result.passed:
                return self._block(result)

        # 2. Funding rate
        if self._funding_filter_enabled and self._http:
            result = await self._check_funding_rate(symbol)
            if not result.passed:
                return self._block(result)

        # 3. Mark price
        if self._mark_filter_enabled and self._http:
            result = await self._check_mark_price(symbol, price)
            if not result.passed:
                return self._block(result)

        # === PENALTY FILTERS ===
        total_multiplier = 1.0

        # 4. Delta (CVD)
        if self._delta_filter_enabled:
            result = self._check_delta(symbol, direction)
            if not result.passed:
                # Delta penalty — не блокируем, штрафуем
                total_multiplier *= self._delta_penalty
                self._stats['penalties_applied'] += 1
                logger.debug(
                    f"[{symbol}] Delta penalty x{self._delta_penalty} "
                    f"({result.reason})"
                )

        # 5. Volume
        if self._volume_filter_enabled:
            result = self._check_volume(symbol)
            if not result.passed:
                total_multiplier *= self._volume_penalty
                self._stats['penalties_applied'] += 1
                logger.debug(
                    f"[{symbol}] Volume penalty x{self._volume_penalty} "
                    f"({result.reason})"
                )

        # 6. Correlation
        if self._correlation_filter_enabled:
            result = self._check_correlation(symbol, direction)
            if not result.passed:
                total_multiplier *= self._correlation_penalty
                self._stats['penalties_applied'] += 1
                logger.debug(
                    f"[{symbol}] Correlation penalty x{self._correlation_penalty} "
                    f"({result.reason})"
                )

        # 7. Liquidity filter (hard block)
        if self._liquidity_filter_enabled and self._orderbook_manager is not None:
            result = self._check_liquidity(symbol, signal.size_usdt)
            if not result.passed:
                return self._block(result)

        # Применяем итоговый multiplier к confidence
        if total_multiplier < 1.0:
            old_conf = signal.confidence
            signal.confidence = signal.confidence * total_multiplier
            logger.debug(
                f"[{symbol}] Confidence {old_conf:.2f} → {signal.confidence:.2f} "
                f"(multiplier={total_multiplier:.2f})"
            )

        # Регистрируем сигнал для correlation filter
        self._register_signal(symbol, direction)

        self._stats['passed'] += 1
        return FilterResult(
            passed=True,
            confidence_multiplier=total_multiplier,
        )

    def add_trade(self, symbol: str, price: float, qty: float, side: str, ts: float):
        """
        Вызывается из engine на каждый трейд.
        Накапливает delta (CVD) и volume для фильтров.
        side: 'Buy' / 'Sell'
        """
        usdt_vol = price * qty
        signed_delta = usdt_vol if side == 'Buy' else -usdt_vol

        now = ts if ts > 1e9 else time.time()

        # Delta window
        if symbol not in self._delta_window:
            self._delta_window[symbol] = []
        self._delta_window[symbol].append((now, signed_delta))
        self._evict(self._delta_window[symbol], self._delta_window_sec)

        # Volume window
        if symbol not in self._volume_window:
            self._volume_window[symbol] = []
        self._volume_window[symbol].append((now, usdt_vol))
        self._evict(self._volume_window[symbol], self._volume_window_sec)

    def get_stats(self) -> dict:
        return dict(self._stats)

    # ------------------------------------------------------------------
    # Hard block filters
    # ------------------------------------------------------------------

    def _check_time_of_day(self) -> FilterResult:
        now = datetime.now(timezone.utc)
        hour = now.hour
        weekday = now.weekday()  # 0=Mon, 5=Sat, 6=Sun

        # Weekend фильтр (суббота и воскресенье)
        if self._weekend_filter_enabled and weekday >= 5:
            return FilterResult(
                passed=False,
                reason=f"weekend filter (weekday={weekday}, {now.strftime('%A')})",
            )

        # Ночной фильтр
        start = self._time_block_start
        end = self._time_block_end
        if start <= hour < end:
            return FilterResult(
                passed=False,
                reason=f"time_of_day {hour:02d}:xx UTC (block {start:02d}-{end:02d})",
            )
        return FilterResult(passed=True)

    async def _check_funding_rate(self, symbol: str) -> FilterResult:
        # Проверяем кэш
        cached = self._funding_cache.get(symbol)
        if cached:
            rate, ts = cached
            if time.time() - ts < self._funding_cache_ttl:
                return self._eval_funding(symbol, rate)

        try:
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: self._http.get_tickers(category='linear', symbol=symbol),
            )
            rate = float(
                resp['result']['list'][0].get('fundingRate', 0)
            )
            self._funding_cache[symbol] = (rate, time.time())
            return self._eval_funding(symbol, rate)
        except Exception as e:
            logger.warning(f"FundingRate fetch failed for {symbol}: {e}")
            return FilterResult(passed=True)  # fail open — не блокируем

    def _eval_funding(self, symbol: str, rate: float) -> FilterResult:
        if abs(rate) > self._max_funding_rate:
            return FilterResult(
                passed=False,
                reason=f"funding_rate {rate:.4%} > limit {self._max_funding_rate:.4%}",
                details={'funding_rate': rate},
            )
        return FilterResult(passed=True, details={'funding_rate': rate})

    async def _check_mark_price(self, symbol: str, last_price: float) -> FilterResult:
        cached = self._mark_cache.get(symbol)
        if cached:
            mark, ts = cached
            if time.time() - ts < self._mark_cache_ttl:
                return self._eval_mark(symbol, mark, last_price)

        try:
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: self._http.get_tickers(category='linear', symbol=symbol),
            )
            mark = float(resp['result']['list'][0].get('markPrice', last_price))
            self._mark_cache[symbol] = (mark, time.time())
            return self._eval_mark(symbol, mark, last_price)
        except Exception as e:
            logger.warning(f"MarkPrice fetch failed for {symbol}: {e}")
            return FilterResult(passed=True)

    def _eval_mark(self, symbol: str, mark: float, last: float) -> FilterResult:
        if last == 0:
            return FilterResult(passed=True)
        deviation_pct = abs(mark - last) / last * 100
        if deviation_pct > self._max_mark_deviation_pct:
            return FilterResult(
                passed=False,
                reason=f"mark_deviation {deviation_pct:.3f}% > limit {self._max_mark_deviation_pct}%",
                details={'mark_price': mark, 'last_price': last, 'deviation_pct': deviation_pct},
            )
        return FilterResult(passed=True, details={'mark_price': mark, 'deviation_pct': deviation_pct})

    # ------------------------------------------------------------------
    # Penalty filters
    # ------------------------------------------------------------------

    def _check_delta(self, symbol: str, direction: str) -> FilterResult:
        window = self._delta_window.get(symbol, [])
        if not window:
            return FilterResult(passed=True)

        net_delta = sum(d for _, d in window)

        # Для long — delta должна быть положительной, для short — отрицательной
        aligned = (direction == 'long' and net_delta > 0) or \
                  (direction == 'short' and net_delta < 0)

        if not aligned or abs(net_delta) < self._min_delta_usdt:
            return FilterResult(
                passed=False,
                reason=f"delta {net_delta:+,.0f} USDT weak for {direction}",
                details={'net_delta': net_delta},
            )
        return FilterResult(passed=True, details={'net_delta': net_delta})

    def _check_volume(self, symbol: str) -> FilterResult:
        window = self._volume_window.get(symbol, [])
        if not window:
            return FilterResult(passed=True)

        total_vol = sum(v for _, v in window)

        if total_vol < self._min_volume_usdt:
            return FilterResult(
                passed=False,
                reason=f"volume {total_vol:,.0f} USDT < min {self._min_volume_usdt:,.0f}",
                details={'volume_usdt': total_vol},
            )
        return FilterResult(passed=True, details={'volume_usdt': total_vol})

    def _check_liquidity(self, symbol: str, size_usdt: float) -> FilterResult:
        """Проверяет глубину стакана и спред перед входом."""
        try:
            ob = self._orderbook_manager.get_orderbook(symbol)
            if ob is None:
                return FilterResult(passed=True)  # нет данных — не блокируем

            best_bid = ob.best_bid
            best_ask = ob.best_ask
            if not best_bid or not best_ask or best_bid <= 0:
                return FilterResult(passed=True)

            # Спред
            spread_pct = (best_ask - best_bid) / best_bid * 100
            if spread_pct > self._max_spread_pct:
                return FilterResult(
                    passed=False,
                    reason=f"liquidity: spread {spread_pct:.3f}% > {self._max_spread_pct}%",
                    details={"spread_pct": spread_pct},
                )

            # Глубина стакана на 0.5% от цены
            mid = (best_bid + best_ask) / 2
            depth_range = mid * 0.005
            bid_depth = sum(v for p, v in ob.bids.items() if p >= best_bid - depth_range) if hasattr(ob, "bids") else 0
            ask_depth = sum(v for p, v in ob.asks.items() if p <= best_ask + depth_range) if hasattr(ob, "asks") else 0
            total_depth_usdt = (bid_depth + ask_depth) * mid
            min_depth = size_usdt * self._min_depth_multiplier

            if total_depth_usdt > 0 and total_depth_usdt < min_depth:
                return FilterResult(
                    passed=False,
                    reason=f"liquidity: depth {total_depth_usdt:.0f} USDT < {min_depth:.0f} USDT",
                    details={"depth_usdt": total_depth_usdt},
                )
        except Exception as e:
            logger.debug("Liquidity check error %s: %s", symbol, e)

        return FilterResult(passed=True)

    def _check_correlation(self, symbol: str, direction: str) -> FilterResult:
        cutoff = time.time() - self._correlation_window_sec
        recent = [s for s in self._recent_signals if s.timestamp > cutoff]

        for group in self._correlated_pairs:
            if symbol not in group:
                continue
            # Ищем недавние сигналы других символов из той же группы
            for other in group:
                if other == symbol:
                    continue
                other_signals = [s for s in recent if s.symbol == other]
                if any(s.direction == direction for s in other_signals):
                    return FilterResult(
                        passed=False,
                        reason=f"correlation: {other} already {direction} within {self._correlation_window_sec}s",
                        details={'correlated_with': other},
                    )
        return FilterResult(passed=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _block(self, result: FilterResult) -> FilterResult:
        self._stats['blocked'] += 1
        reason = result.reason
        self._stats['block_reasons'][reason] = \
            self._stats['block_reasons'].get(reason, 0) + 1
        logger.info(f"FilterPipeline BLOCK: {reason}")
        return result

    def _register_signal(self, symbol: str, direction: str):
        self._recent_signals.append(
            _SymbolDirection(symbol=symbol, direction=direction)
        )
        # Чистим старые
        cutoff = time.time() - self._correlation_window_sec
        self._recent_signals = [
            s for s in self._recent_signals if s.timestamp > cutoff
        ]

    @staticmethod
    def _evict(window: list, window_sec: float):
        cutoff = time.time() - window_sec
        while window and window[0][0] < cutoff:
            window.pop(0)

    def _update_price_history(self, symbol: str, price: float) -> None:
        """Сохраняем историю цен для stop-hunt детектора."""
        now = time.time()
        if symbol not in self._price_history:
            self._price_history[symbol] = []
        self._price_history[symbol].append((now, price))
        # Чистим старые записи (старше window×3)
        cutoff = now - self._stop_hunt_window_s * 3
        self._price_history[symbol] = [
            (ts, p) for ts, p in self._price_history[symbol] if ts >= cutoff
        ]

    def _check_stop_hunt(self, symbol: str, price: float) -> FilterResult:
        """Stop-Hunt Detector: вспышка цены ≥0.5% и откат за 3s → блок 10s."""
        now = time.time()

        # Проверяем активный блок
        if symbol in self._stop_hunt_blocked_until:
            if now < self._stop_hunt_blocked_until[symbol]:
                remaining = self._stop_hunt_blocked_until[symbol] - now
                return FilterResult(
                    passed=False,
                    reason=f"stop_hunt_block ({remaining:.1f}s remaining)",
                    details={'symbol': symbol},
                )
            else:
                del self._stop_hunt_blocked_until[symbol]

        history = self._price_history.get(symbol, [])
        if len(history) < 2:
            return FilterResult(passed=True)

        window_start = now - self._stop_hunt_window_s
        recent = [(ts, p) for ts, p in history if ts >= window_start]
        if len(recent) < 2:
            return FilterResult(passed=True)

        prices = [p for _, p in recent]
        price_max = max(prices)
        price_min = min(prices)
        swing_pct = (price_max - price_min) / price_min * 100

        if swing_pct >= self._stop_hunt_threshold_pct:
            # Проверяем откат: текущая цена вернулась к середине диапазона
            mid = (price_max + price_min) / 2
            if abs(price - mid) / mid * 100 < self._stop_hunt_threshold_pct * 0.5:
                self._stop_hunt_blocked_until[symbol] = now + self._stop_hunt_block_s
                logger.warning(
                    "[%s] Stop-hunt detected: swing=%.2f%% in %.1fs → block %.1fs",
                    symbol, swing_pct, self._stop_hunt_window_s, self._stop_hunt_block_s,
                )
                return FilterResult(
                    passed=False,
                    reason=f"stop_hunt detected (swing={swing_pct:.2f}%)",
                    details={'swing_pct': swing_pct, 'symbol': symbol},
                )

        return FilterResult(passed=True)

