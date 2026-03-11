# core/engine.py — ИСПРАВЛЕННЫЕ ИМПОРТЫ
from __future__ import annotations

import asyncio
import time
import logging
import signal as os_signal  # переименовали чтобы не конфликтовать с models.signals
import sys

import yaml

from core.data_feed import BybitDataFeed
from core.btc_bias import BTCDirectionBias
from core.session_filter import SessionFilter
from core.score_decay import ScoreDecay
from core.regime_filter import RegimeFilter
from core.mtf_filter import MTFDirectionFilter
from core.latency_guard import LatencyGuard
from core.orderbook import OrderBookManager
from core.volatility_tracker import VolatilityTracker
from execution.order_executor import OrderExecutor
from storage.trade_logger import TradeLogger
from monitoring.telegram_alerts import TelegramAlerts
from models.signals import TradeData, LatencyLevel
from analyzers.vector_analyzer import VectorAnalyzer
from analyzers.averages_analyzer import AveragesAnalyzer
from analyzers.depth_shot_analyzer import DepthShotAnalyzer
from analyzers.signal_aggregator import SignalAggregator
from core.filter_pipeline import FilterPipeline
from monitoring.telegram_commands import TelegramCommands
from monitoring.health_server import HealthServer
from backtester.market_saver import MarketSaver
from core.position_manager import PositionManager
from core.risk_manager import RiskManager, RiskConfig
from core.circuit_breaker import CircuitBreaker, CBState
from core.data_health import DataHealthMonitor

logger = logging.getLogger(__name__)


class HybridEngine:
    """
    Главный движок гибридного бота.
    Фаза 1: инфраструктура (DataFeed, OrderBook,
             LatencyGuard, Executor, Logging)
    Фаза 2: анализаторы (Vector, Averages, DepthShot)
    Фаза 3: фильтры + PositionManager
    """

    def __init__(self, config_path: str):
        # Загружаем конфигурацию
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        # Загружаем стратегию
        strategy_path = self.config.get(
            'strategy_path',
            'config/strategies/hybrid.yaml',
        )
        with open(strategy_path) as f:
            self.strategy = yaml.safe_load(f)

        # Инициализация компонентов
        self._init_components()

        # Состояние
        self._running = False
        self._shutdown_event: asyncio.Event | None = None

    def _init_components(self):
        """Инициализация всех компонентов"""
        exchange_config = self.config.get('exchange', {})
        latency_config = self.config.get('latency', {})
        pairs_config = self.config.get('pairs', {})
        monitoring_config = self.config.get('monitoring', {})

        # 1. Latency Guard
        self.latency_guard = LatencyGuard(latency_config)
        self.latency_guard.on_level_change(
            self._on_latency_change
        )

        # 2. OrderBook Manager
        self.orderbook_manager = OrderBookManager(max_depth=200)

        # 3. Volatility Tracker
        self.volatility_tracker = VolatilityTracker(
            window_seconds=60
        )

        # 4. Order Executor
        self.executor = OrderExecutor(
            exchange_config, self.latency_guard
        )

        # 5. Trade Logger
        self.trade_logger = TradeLogger()

        # 5b. Market Saver (запись тиков для Backtester)
        self.market_saver = MarketSaver(
            db_path=self.config.get('storage', {}).get('market_db', 'data/market.db')
        )

        # DataHealthMonitor
        symbols = self.config.get('pairs', {}).get('symbols', [])
        self.data_health = DataHealthMonitor(symbols)

        # CircuitBreaker
        cb_config = self.strategy.get('circuit_breaker', {})
        self.circuit_breaker = CircuitBreaker(
            max_consecutive_losses=cb_config.get('max_consecutive_losses', 3),
            max_losses_per_hour=cb_config.get('max_losses_per_hour', 5),
            max_drawdown_pct=cb_config.get('max_drawdown_pct', 5.0),
            soft_cooldown_sec=cb_config.get('soft_cooldown_sec', 900),
            hard_cooldown_sec=cb_config.get('hard_cooldown_sec', 1800),
        )

        # 6. Telegram Alerts
        self.alerts = TelegramAlerts(
            monitoring_config.get('telegram', {})
        )

        # 7. Data Feed
        feed_config = {
            **exchange_config,
            'symbols': pairs_config.get('symbols', []),
        }
        self.data_feed = BybitDataFeed(
            config=feed_config,
            latency_guard=self.latency_guard,
            orderbook_manager=self.orderbook_manager,
            volatility_tracker=self.volatility_tracker,
        )

        # 8. Анализаторы (Фаза 2)
        strategy_config = self.strategy.get('analyzers', {})

        self.vector = VectorAnalyzer(
            strategy_config.get('vector', {})
        )
        self.averages = AveragesAnalyzer(
            strategy_config.get('averages', {})
        )
        self.depth = DepthShotAnalyzer(
            strategy_config.get('depth_shot', {}),
            self.orderbook_manager,
        )
        self.aggregator = SignalAggregator(
            self.strategy.get('aggregator', {}),
            self.vector,
            self.averages,
            self.depth,
        )
        self.aggregator.on_signal(self._on_aggregated_signal)
        self.aggregator.on_opposite_exit(self._on_opposite_exit)

        # BTCDirectionBias
        self.btc_bias = BTCDirectionBias(threshold_pct=0.3, window_sec=300)
        self.session_filter = SessionFilter()
        self.score_decay = ScoreDecay()
        self.regime_filter = RegimeFilter()

        # Сессия 4: DepthShotV2, RealisticTPLadder, MomentumFadeExit
        from analyzers.depth_shot_v2 import DepthShotV2
        from core.momentum_fade import MomentumFadeExit
        depth_cfg = self.config.get('depth_shot_v2', {})
        self.depth_v2 = DepthShotV2(depth_cfg, self.orderbook_manager)
        fade_cfg = self.config.get('momentum_fade', {})
        self.momentum_fade = MomentumFadeExit(fade_cfg)

        # MTFDirectionFilter
        symbols = self.config.get('pairs', {}).get('symbols', [])
        self.mtf_filter = MTFDirectionFilter(
            client=self.executor._client,
            symbols=symbols,
            update_interval=300,
        )

        # Дедупликация алертов: symbol → last_alert_ts
        self._alert_dedup: dict[str, float] = {}
        self._alert_dedup_ttl: float = 60.0  # секунд между алертами на один символ

        # 9. Filter Pipeline (Фаза 3)
        self.filter_pipeline = FilterPipeline(
            self.config,
            http_client=self.executor._client,
            orderbook_manager=self.orderbook_manager,
        )

        # Регистрируем callbacks
        self.data_feed.on_trade(self._on_trade)
        self.data_feed.on_orderbook_update(
            self._on_orderbook_update
        )
        # MTFDirectionFilter запускается в run() после event loop готов

        # 10. Position Manager (Фаза 3)
        self.position_manager = PositionManager(
            config=self.strategy,
            executor=self.executor,
            volatility_tracker=self.volatility_tracker,
        )

        # 11. Risk Manager
        risk_cfg = self.strategy.get('risk', {})
        self.risk_manager = RiskManager(RiskConfig(
            position_pct=risk_cfg.get('position_pct', 2.0),
            daily_loss_limit_usdt=risk_cfg.get('daily_loss_limit_usdt', 50.0),
            corr_block_enabled=risk_cfg.get('corr_block_enabled', True),
            min_size_usdt=risk_cfg.get('min_size_usdt', 5.0),
            max_size_usdt=risk_cfg.get('max_size_usdt', 500.0),
        ))
        self.position_manager._risk_manager = self.risk_manager
        self.position_manager._circuit_breaker = self.circuit_breaker
        self.position_manager._alerts = self.alerts
        self.position_manager._aggregator = self.aggregator
        self.position_manager._depth_v2 = self.depth_v2
        self.data_health._alerts = self.alerts

        # 12. Telegram Commands
        self.telegram_commands = TelegramCommands(
            monitoring_config.get('telegram', {}),
            engine=self,
        )

        self._loop: asyncio.AbstractEventLoop | None = None
        logger.info("All components initialized")

    async def run(self):
        """Запуск бота"""
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._shutdown_event = asyncio.Event()

        logger.info("=" * 60)
        logger.info("  HYBRID TRADING BOT — Phase 1")
        logger.info(f"  Strategy: {self.strategy.get('name')}")
        logger.info(f"  Mode: {self.strategy.get('mode')}")
        logger.info(
            f"  Symbols: "
            f"{self.config.get('pairs', {}).get('symbols', [])}"
        )
        logger.info(
            f"  Testnet: "
            f"{self.config.get('exchange', {}).get('testnet')}"
        )
        logger.info("=" * 60)

        # Ловим сигналы завершения
        loop = asyncio.get_event_loop()
        for sig in (os_signal.SIGINT, os_signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    sig, self._handle_shutdown
                )
            except NotImplementedError:
                # Windows не поддерживает add_signal_handler
                pass

        # Отправляем стартовое сообщение
        await self.alerts.send(
            "🚀 <b>Bot Started</b>\n"
            f"Strategy: {self.strategy.get('name')}\n"
            f"Symbols: "
            f"{self.config.get('pairs', {}).get('symbols', [])}"
        )

        # Получаем начальный баланс ДО старта data_feed
        for attempt in range(3):
            try:
                balance = await self.executor.get_balance()
                if balance > 0:
                    self.risk_manager.set_balance(balance)
                    logger.info("Initial balance: %.2f USDT", balance)
                    break
                await asyncio.sleep(1)
            except Exception as e:
                logger.warning("Initial balance fetch attempt %d failed: %s", attempt + 1, e)
                await asyncio.sleep(2)

        # Устанавливаем isolated margin + безопасное плечо
        symbols = self.config.get('pairs', {}).get('symbols', [])
        await self.executor.init_leverage(symbols, leverage=3)

        # Запускаем задачи
        tasks = [
            asyncio.create_task(
                self.data_feed.start(), name="data_feed"
            ),
            asyncio.create_task(
                self.mtf_filter.start(), name="mtf_filter"
            ),
            asyncio.create_task(
                self._stats_reporter(), name="stats_reporter"
            ),
            asyncio.create_task(
                self._position_sync_loop(), name="position_sync"
            ),
            asyncio.create_task(
                self.telegram_commands.start(), name="telegram_commands"
            ),
            asyncio.create_task(
                self.data_health.run(), name="data_health"
            ),
            asyncio.create_task(
                self._shutdown_waiter(), name="shutdown_waiter"
            ),
        ]

        try:
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_EXCEPTION,
            )

            # Если какая-то задача завершилась — отменяем остальные
            for task in pending:
                task.cancel()

            # Проверяем ошибки
            for task in done:
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    import traceback
                    logger.critical(
                        f"Task {task.get_name()} failed: {e}\n{traceback.format_exc()}"
                    )

        except Exception as e:
            logger.critical(f"Engine error: {e}")
        finally:
            await self._cleanup()

    def _on_trade(self, trade: TradeData):
        """
        Callback для каждого трейда.
        Обновляем все анализаторы и оцениваем сигналы.
        """
        _loop_start = time.time()

        # DataHealthMonitor: фиксируем входящий тик
        self.data_health.on_trade(trade.symbol)

        # MarketSaver: пишем тик в SQLite для Backtester
        self.market_saver.save_trade(trade)

        # BTCDirectionBias: обновляем на каждом тике
        self.btc_bias.on_trade(trade.symbol, trade.price, trade.timestamp)
        self.regime_filter.update(trade.symbol, trade.price * 1.001, trade.price * 0.999, trade.price)

        # Обновляем Averages (всегда)
        self.averages.on_trade(trade)

        # Обновляем Vector — он возвращает сигнал если условия выполнены
        vector_signal = self.vector.on_trade(trade)

        # FilterPipeline: накапливаем delta и volume
        self.filter_pipeline.add_trade(
            symbol=trade.symbol,
            price=trade.price,
            qty=trade.qty,
            side=trade.side,
            ts=trade.timestamp,
        )

        # Передаём в агрегатор
        pos_dir = self.position_manager.get_direction(trade.symbol)
        signal = self.aggregator.evaluate(
            symbol=trade.symbol,
            vector_signal=vector_signal,
            current_price=trade.price,
            current_position_direction=pos_dir,
        )

        # PositionManager: обновляем trailing stop
        # Используем run_coroutine_threadsafe т.к. callback из pybit-треда
        if self.position_manager.has_position(trade.symbol) and self._loop:
            asyncio.run_coroutine_threadsafe(
                self.position_manager.update_price(trade.symbol, trade.price),
                self._loop,
            )

        # MomentumFadeExit: обновляем тики и проверяем угасание
        self.momentum_fade.update(trade.symbol, trade.price, trade.timestamp)
        self.depth_v2._tracker.update(
            trade.symbol,
            'bid' if trade.side == 'Buy' else 'ask',
            trade.price,
            trade.qty * trade.price,
        )
        if self.position_manager.has_position(trade.symbol) and self._loop:
            pos = self.position_manager.get_position(trade.symbol)
            if pos:
                from models.signals import Direction
                direction = Direction.LONG if pos.direction == 'long' else Direction.SHORT
                if self.momentum_fade.should_exit(
                    trade.symbol, direction, pos.entry_price, trade.price
                ):
                    asyncio.run_coroutine_threadsafe(
                        self.position_manager.close_position(
                            trade.symbol, reason='momentum_fade'
                        ),
                        self._loop,
                    )

        # LoopMonitor: замер времени обработки тика
        _loop_ms = (time.time() - _loop_start) * 1000
        if not hasattr(self, '_loop_latencies'):
            from collections import deque
            self._loop_latencies = deque(maxlen=1000)
        self._loop_latencies.append(_loop_ms)
        if _loop_ms > 100:
            logger.warning("LoopMonitor: tick processing %.1fms > 100ms", _loop_ms)

    def _on_orderbook_update(self, message: dict):
        """
        Callback для обновлений стакана.
        OrderBookManager уже обновлён в DataFeed.
        DepthShot читает стакан напрямую через OrderBookManager.
        """
        pass  # DepthShot работает через orderbook_manager напрямую

    def _on_aggregated_signal(self, signal):
        if self._loop:
            asyncio.run_coroutine_threadsafe(
                self._filter_and_handle_signal(signal), self._loop
            )

    def _on_opposite_exit(self, exit_signal):
        if self._loop:
            asyncio.run_coroutine_threadsafe(
                self._handle_opposite_exit(exit_signal), self._loop
            )

    async def _handle_opposite_exit(self, exit_signal):
        from analyzers.signal_aggregator import ExitReason
        symbol = exit_signal.symbol
        reason = exit_signal.reason

        logger.info("OPPOSITE EXIT [%s] %s score=%.2f",
                    reason.value.upper(), symbol, exit_signal.score)

        pos = self.position_manager.get_position(symbol)
        if pos is None:
            return

        # Закрываем текущую позицию
        await self.position_manager.close_position(symbol, reason='opposite_exit')

        # Реверс — только если все 5 ограничений SafeReverse выполнены
        if reason == ExitReason.REVERSE:
            if not self._safe_reverse_allowed(pos, exit_signal):
                logger.info("REVERSE BLOCKED by SafeReverse rules [%s]", symbol)
                return
            from models.signals import Direction
            new_dir = Direction.SHORT if pos.direction == Direction.LONG else Direction.LONG
            logger.info("REVERSE %s → %s", symbol, new_dir.value)
            # Обновляем счётчик реверсов
            import time as _time
            self._reverse_timestamps.append(_time.time())
            # Даём рынку 200ms успокоиться
            await asyncio.sleep(0.2)
            # Сигнал реверса создаётся в следующем evaluate() цикле автоматически

    async def _entry_confirmation(self, signal) -> bool:
        """
        EntryConfirmation: ждём 2s после сигнала.
        Цена должна пойти в нашу сторону — 3+ favorable тика,
        max adverse move < 0.03%.
        """
        from models.signals import Direction
        symbol = signal.symbol
        direction = signal.direction
        entry_price = signal.entry_price

        if entry_price <= 0:
            return True  # нет данных — пропускаем проверку

        favorable_ticks = 0
        max_adverse_pct = 0.0
        last_price = entry_price
        deadline = time.time() + 2.0

        while time.time() < deadline:
            await asyncio.sleep(0.05)
            # Берём последнюю цену из стакана
            try:
                ob = self.orderbook_manager.get_orderbook(symbol)
                if ob:
                    bid = ob.best_bid()
                    ask = ob.best_ask()
                    if bid and ask:
                        current_price = (bid + ask) / 2
                        if direction == Direction.LONG:
                            if current_price > last_price:
                                favorable_ticks += 1
                            adverse_pct = (entry_price - current_price) / entry_price * 100
                        else:
                            if current_price < last_price:
                                favorable_ticks += 1
                            adverse_pct = (current_price - entry_price) / entry_price * 100
                        max_adverse_pct = max(max_adverse_pct, adverse_pct)
                        last_price = current_price

                        # Ранний выход если цена сильно против нас
                        if max_adverse_pct > 0.05:
                            logger.debug(
                                "EntryConfirmation REJECTED [%s]: adverse %.4f%%",
                                symbol, max_adverse_pct
                            )
                            return False
            except Exception:
                pass

        # Итог
        confirmed = favorable_ticks >= 2 and max_adverse_pct <= 0.05
        logger.debug(
            "EntryConfirmation [%s]: favorable=%d adverse=%.4f%% → %s",
            symbol, favorable_ticks, max_adverse_pct,
            "OK" if confirmed else "REJECTED"
        )
        return confirmed

    def _infra_circuit_breaker_active(self) -> bool:
        """Проверка инфраструктурных проблем — блокировка входов"""
        import time as _time

        # Инициализация
        if not hasattr(self, '_infra_cb_blocked_until'):
            self._infra_cb_blocked_until = 0.0
        if not hasattr(self, '_ws_reconnect_times'):
            from collections import deque
            self._ws_reconnect_times = deque(maxlen=20)

        now = _time.time()

        # Уже заблокированы?
        if now < self._infra_cb_blocked_until:
            return True

        # ① Спред > 0.08% — проверяем через orderbook
        try:
            for symbol in list(self._positions_symbols if hasattr(self, '_positions_symbols') else []):
                ob = self.orderbook_manager.get_orderbook(symbol)
                if ob:
                    bid, ask = ob.best_bid(), ob.best_ask()
                    if bid and ask and ask > 0:
                        spread_pct = (ask - bid) / ask * 100
                        if spread_pct > 0.08:
                            logger.warning("InfraCB: spread %.3f%% > 0.08%% on %s, blocking 5 min", spread_pct, symbol)
                            self._infra_cb_blocked_until = now + 300
                            return True
        except Exception:
            pass

        # ② Loop latency > 200ms
        try:
            lat = self.latency_guard.get_stats()
            if lat.get('ws_latency_ms', 0) > 200:
                logger.warning("InfraCB: ws_latency %sms > 200ms, blocking", lat.get('ws_latency_ms'))
                self._infra_cb_blocked_until = now + 300
                return True
        except Exception:
            pass

        # ③ WS reconnects > 5 за последний час
        try:
            hour_ago = now - 3600
            recent_reconnects = sum(1 for t in self._ws_reconnect_times if t > hour_ago)
            if recent_reconnects > 5:
                logger.warning("InfraCB: %d WS reconnects in last hour, blocking", recent_reconnects)
                self._infra_cb_blocked_until = now + 1800
                return True
        except Exception:
            pass

        return False

    def _safe_reverse_allowed(self, pos, exit_signal) -> bool:
        """5 ограничений SafeReverse — реверс только если все выполнены"""
        import time as _time
        from collections import deque

        # Инициализация счётчиков если нет
        if not hasattr(self, '_reverse_timestamps'):
            self._reverse_timestamps = deque(maxlen=10)
        if not hasattr(self, '_last_reverse_time'):
            self._last_reverse_time = 0.0

        now = _time.time()

        # ① Позиция не в убытке > 0.5×ATR
        try:
            atr = self.volatility_tracker.get_atr(pos.symbol)
            if atr and pos.unrealized_pnl_pct < -(0.5 * atr / pos.entry_price * 100):
                logger.info("SafeReverse ①: position in loss > 0.5×ATR")
                return False
        except Exception:
            pass

        # ② Сценарий реверса = S2 или S4 (трендовые)
        scenario = getattr(exit_signal, 'scenario', None)
        if scenario is not None:
            from analyzers.signal_aggregator import Scenario
            allowed_scenarios = {Scenario.AVERAGES_VECTOR, Scenario.ALL_THREE}
            if scenario not in allowed_scenarios:
                logger.info("SafeReverse ②: scenario %s not trending", scenario)
                return False

        # ③ Прошло >10 минут с последнего реверса
        if now - self._last_reverse_time < 600:
            logger.info("SafeReverse ③: last reverse < 10 min ago")
            return False

        # ④ Не более 2 реверсов за день
        day_start = now - 86400
        reverses_today = sum(1 for t in self._reverse_timestamps if t > day_start)
        if reverses_today >= 2:
            logger.info("SafeReverse ④: %d reverses today >= 2", reverses_today)
            return False

        # ⑤ Направление реверса совпадает с MTF трендом (если есть)
        # MTFDirectionFilter будет добавлен в Сессии 2, пока пропускаем
        # if hasattr(self, 'mtf_filter'):
        #     ...

        self._last_reverse_time = now
        return True

    async def _filter_and_handle_signal(self, signal):
        """Обработка финального сигнала от агрегатора"""
        # InfrastructureCircuitBreaker
        if self._infra_circuit_breaker_active():
            logger.warning("SIGNAL BLOCKED: InfrastructureCircuitBreaker active")
            return

        # SessionFilter: проверяем сессию и сценарий
        import time
        scenario = signal.scenario.value if hasattr(signal.scenario, 'value') else str(signal.scenario)
        allowed, sess_mult = self.session_filter.is_allowed(time.time(), scenario)
        if not allowed:
            session = self.session_filter.get_session(time.time())
            logger.info("SIGNAL BLOCKED by SessionFilter [%s %s]: session=%s scenario=%s",
                signal.symbol, signal.direction.value, session, scenario)
            return
        # Применяем score multiplier сессии
        signal = signal._replace(confidence=signal.confidence * sess_mult) if hasattr(signal, '_replace') else signal

        # RegimeFilter: проверяем режим рынка
        regime_allowed, regime_mult = self.regime_filter.is_allowed(signal.symbol, scenario)
        if not regime_allowed:
            regime = self.regime_filter.get_regime(signal.symbol)
            logger.info("SIGNAL BLOCKED by RegimeFilter [%s %s]: regime=%s scenario=%s",
                signal.symbol, signal.direction.value, regime, scenario)
            return
        if hasattr(signal, '_replace') and regime_mult != 1.0:
            signal = signal._replace(confidence=signal.confidence * regime_mult)

        # ScoreDecay: затухание confidence со временем
        decayed_confidence = self.score_decay.apply(signal.symbol, scenario, signal.confidence)
        if decayed_confidence == 0.0:
            logger.info("SIGNAL DEAD by ScoreDecay [%s %s]: age too old", signal.symbol, signal.direction.value)
            return
        if hasattr(signal, '_replace'):
            signal = signal._replace(confidence=decayed_confidence)

        # FilterPipeline: проверяем сигнал
        result = await self.filter_pipeline.check(signal)
        if not result.passed:
            logger.info(
                f"SIGNAL FILTERED [{signal.symbol}]: {result.reason}"
            )
            return

        logger.info(
            f"AGGREGATED SIGNAL: [{signal.scenario.value}] "
            f"{signal.symbol} {signal.direction.value} "
            f"entry={signal.entry_price:.6f} "
            f"tp={signal.tp_price:.6f} "
            f"confidence={signal.confidence:.2f}"
        )

        # BTCDirectionBias: блок если BTC идёт против направления альта
        if self.btc_bias.is_blocked(signal.symbol, signal.direction.value):
            logger.info(
                "SIGNAL BLOCKED by BTCBias [%s %s]: BTC bias=%s",
                signal.symbol, signal.direction.value, self.btc_bias.get_bias()
            )
            return

        # MTFDirectionFilter: блок против тренда на 15m/1h
        scenario = signal.scenario.value if hasattr(signal.scenario, 'value') else str(signal.scenario)
        if self.mtf_filter.is_blocked(signal.symbol, signal.direction.value, scenario):
            logger.info(
                "SIGNAL BLOCKED by MTF [%s %s]: bias=%s strength=%.1f",
                signal.symbol, signal.direction.value,
                self.mtf_filter.get_bias(signal.symbol).value,
                self.mtf_filter.get_strength(signal.symbol)
            )
            return

        # EntryConfirmation: ждём 2s — цена должна пойти в нашу сторону
        confirmed = await self._entry_confirmation(signal)
        if not confirmed:
            logger.info("ENTRY REJECTED by EntryConfirmation [%s]", signal.symbol)
            self.score_decay.clear(signal.symbol)
            return

        # Алерт с дедупликацией
        now = time.time()
        last_alert = self._alert_dedup.get(signal.symbol, 0)
        if now - last_alert >= self._alert_dedup_ttl:
            self._alert_dedup[signal.symbol] = now
            asyncio.create_task(
                self.alerts.send(
                    f"?? <b>Signal [{signal.scenario.value}]</b>\n"
                    f"Symbol: {signal.symbol}\n"
                    f"Direction: {signal.direction.value.upper()}\n"
                    f"Entry: {signal.entry_price:.2f}\n"
                    f"TP: {signal.tp_price:.2f}\n"
                    f"Confidence: {signal.confidence:.2f}\n"
                    f"Score: {signal.score:.2f}"
                )
            )
        else:
            logger.debug("Alert dedup %s: %.0fs ago", signal.symbol, now - last_alert)

        # Проверяем паузу
        if self.telegram_commands.is_paused:
            logger.info('Trading paused, skipping signal %s', signal.symbol)
            return

        # CircuitBreaker: проверяем состояние
        can_trade, cb_state, cb_reason = self.circuit_breaker.check()
        if not can_trade:
            logger.warning('CIRCUIT BREAKER [%s]: %s', signal.symbol, cb_reason)
            return

        # RiskManager: проверяем лимиты и получаем размер позиции
        # Получаем ATR для sl_distance
        vol_tracker = self.volatility_tracker.get(signal.symbol)
        atr_pct = vol_tracker.get_volatility() if vol_tracker else 0.0
        sl_dist = max(atr_pct * 2.5, self.risk_manager.cfg.sl_pct_default) if atr_pct else self.risk_manager.cfg.sl_pct_default

        decision = self.risk_manager.check(
            signal.symbol,
            score=signal.score,
            sl_distance_pct=sl_dist,
            scenario_threshold=signal.threshold if hasattr(signal, 'threshold') else 0.4,
        )
        if not decision.allowed:
            logger.info(
                f"RISK BLOCK [{signal.symbol}]: {decision.reason}"
            )
            return
        signal.size_usdt = decision.size_usdt

        pos = await self.position_manager.open_position(signal)
        if pos:
            self.risk_manager.record_open(signal.symbol)

    async def _position_sync_loop(self):
        """Периодическая синхронизация позиций с биржей."""
        import time as _time
        last_cleanup = _time.time()
        while self._running:
            await asyncio.sleep(30)
            await self.position_manager.sync_with_exchange()
            # Обновляем баланс для RiskManager
            try:
                balance = await self.executor.get_balance()
                if balance > 0:
                    self.risk_manager.set_balance(balance)
                    logger.debug("RiskManager balance updated: %.2f USDT", balance)
            except Exception as e:
                logger.warning("Balance update error: %s", e)
            # Ежедневная очистка старых тиков
            if _time.time() - last_cleanup > 86400:
                try:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None, lambda: self.market_saver.cleanup_old_data(30)
                    )
                    last_cleanup = _time.time()
                except Exception as e:
                    logger.warning("MarketSaver cleanup error: %s", e)
            # Обновляем баланс для RiskManager
            try:
                balance = await self.executor.get_balance()
                if balance > 0:
                    self.risk_manager.set_balance(balance)
                    logger.debug("RiskManager balance updated: %.2f USDT", balance)
            except Exception as e:
                logger.warning("Balance update error: %s", e)

    def _on_latency_change(
        self,
        old_level: LatencyLevel,
        new_level: LatencyLevel,
        latency_ms: float,
    ):
        """Реакция на изменение уровня задержки"""
        asyncio.create_task(
            self.alerts.alert_latency(
                new_level.value, latency_ms
            )
        )

        if new_level == LatencyLevel.CRITICAL:
            logger.critical(
                "CRITICAL latency — cancelling pending orders"
            )
            # TODO Phase 3: self.position_manager.cancel_all_pending()

        elif new_level == LatencyLevel.EMERGENCY:
            logger.critical(
                "EMERGENCY — stopping all trading"
            )
            # TODO Phase 3: self.position_manager.emergency_close_all()

    async def _stats_reporter(self):
        """Периодический отчёт о состоянии + алерты на аномалии"""
        import time as _time
        import datetime
        last_signal_ts = _time.time()
        no_signal_alerted = False
        NO_SIGNAL_TIMEOUT = 600
        CHECK_INTERVAL = 60

        while self._running:
            await asyncio.sleep(CHECK_INTERVAL)

            stats = self._collect_stats()
            df = stats.get('data_feed', {})
            lat = stats.get('latency', {})
            exe = stats.get('executor', {})
            rl = exe.get('rate_limiter', {})

            logger.info(
                "=== STATUS === "
                "Trades: %s | OB updates: %s | Latency: %sms (%s) | API remaining: %s",
                df.get('trade_count', 0),
                df.get('orderbook_updates', 0),
                lat.get('ws_latency_ms', 0),
                lat.get('current_level', 'unknown'),
                rl.get('remaining', 0),
            )

            pm_stats = self.position_manager.get_stats()
            rm = self.risk_manager

            # Сигнал был — сбрасываем таймер
            if pm_stats.get('total_opened', 0) > 0:
                last_signal_ts = _time.time()
                no_signal_alerted = False

            # Алерт: нет сигналов N минут
            elapsed = _time.time() - last_signal_ts
            if elapsed > NO_SIGNAL_TIMEOUT and not no_signal_alerted:
                no_signal_alerted = True
                msg = "No signals for %.0f min. Check market conditions." % (elapsed / 60)
                asyncio.create_task(self.alerts.send(msg))

            # Алерт: большой drawdown
            if rm._balance_usdt > 0:
                drawdown_pct = abs(rm.session_pnl / rm._balance_usdt * 100)
                if drawdown_pct >= 2.0 and rm.session_pnl < 0:
                    msg = (
                        "Drawdown Alert\n"
                        "Session P&L: %.2f USDT\n"
                        "Drawdown: %.1f%%\n"
                        "Daily loss: %.2f / %.2f USDT"
                    ) % (rm.session_pnl, drawdown_pct,
                         rm.daily_loss_usdt, rm.cfg.daily_loss_limit_usdt)
                    asyncio.create_task(self.alerts.send(msg, urgent=True))

            # Алерт: trading halted
            if rm.is_trading_halted:
                msg = "Trading HALTED\nDaily loss limit reached.\nSession P&L: %.2f USDT" % rm.session_pnl
                asyncio.create_task(self.alerts.send(msg, urgent=True))

            # Ежечасный статус
            if datetime.datetime.now().minute < 1:
                msg = (
                    "Hourly Status\n"
                    "Balance: %.2f USDT\n"
                    "Session P&L: %.2f USDT\n"
                    "Open positions: %d\n"
                    "Trades: %d closed (%d TP / %d SL)\n"
                    "API remaining: %s"
                ) % (
                    rm._balance_usdt, rm.session_pnl,
                    len(self.position_manager.get_all_positions()),
                    pm_stats.get('total_closed', 0),
                    pm_stats.get('tp_hits', 0),
                    pm_stats.get('sl_hits', 0),
                    rl.get('remaining', 0),
                )
                asyncio.create_task(self.alerts.send(msg))

    def _collect_stats(self) -> dict:
        """Собрать статистику со всех компонентов"""
        symbols = self.config.get(
            'pairs', {}
        ).get('symbols', [])

        # LoopMonitor P99
        loop_stats = {}
        if hasattr(self, '_loop_latencies') and self._loop_latencies:
            import statistics
            lats = list(self._loop_latencies)
            lats.sort()
            p99_idx = int(len(lats) * 0.99)
            loop_stats = {
                'p99_ms': round(lats[p99_idx], 2),
                'max_ms': round(max(lats), 2),
                'avg_ms': round(statistics.mean(lats), 2),
            }

        return {
            'loop_monitor': loop_stats,
            'data_feed': self.data_feed.get_stats(),
            'latency': self.latency_guard.get_stats(),
            'orderbooks': self.orderbook_manager.get_all_stats(),
            'volatility': {
                symbol: self.volatility_tracker.get_stats(symbol)
                for symbol in symbols
            },
            'executor': self.executor.get_stats(),
            'trade_logger': self.trade_logger.get_stats(),
            'vector': self.vector.get_stats(),
            'averages': self.averages.get_stats(),
            'aggregator': self.aggregator.get_stats(),
        }

    def _handle_shutdown(self):
        """Обработка сигнала завершения"""
        logger.info("Shutdown signal received")
        self._running = False
        self._shutdown_event.set()

    async def _shutdown_waiter(self):
        """Ждём сигнал завершения"""
        await self._shutdown_event.wait()

    async def _cleanup(self):
        """Очистка при завершении"""
        logger.info("Cleaning up...")

        self._running = False

        # Сбрасываем буфер MarketSaver
        try:
            self.market_saver.flush()
        except Exception:
            pass

        # Останавливаем data feed
        await self.data_feed.stop()

        # TODO Phase 3: закрыть все позиции если нужно

        # Отправляем сообщение
        final_stats = self.data_feed.get_stats()
        await self.alerts.send(
            "🛑 <b>Bot Stopped</b>\n"
            f"Total trades processed: "
            f"{final_stats.get('trade_count', 0)}"
        )

        logger.info("Cleanup complete")
