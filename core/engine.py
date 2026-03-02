# core/engine.py — ИСПРАВЛЕННЫЕ ИМПОРТЫ
from __future__ import annotations

import asyncio
import logging
import signal as os_signal  # переименовали чтобы не конфликтовать с models.signals
import sys

import yaml

from core.data_feed import BybitDataFeed
from core.latency_guard import LatencyGuard
from core.orderbook import OrderBookManager
from core.volatility_tracker import VolatilityTracker
from execution.order_executor import OrderExecutor
from storage.trade_logger import TradeLogger
from monitoring.telegram_alerts import TelegramAlerts
from models.signals import TradeData, LatencyLevel

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
        self._shutdown_event = asyncio.Event()

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

        # Регистрируем callbacks
        self.data_feed.on_trade(self._on_trade)
        self.data_feed.on_orderbook_update(
            self._on_orderbook_update
        )

        logger.info("All components initialized")

    async def run(self):
        """Запуск бота"""
        self._running = True

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

        # Запускаем задачи
        tasks = [
            asyncio.create_task(
                self.data_feed.start(), name="data_feed"
            ),
            asyncio.create_task(
                self._stats_reporter(), name="stats_reporter"
            ),
            asyncio.create_task(
                self._shutdown_waiter(), name="shutdown_waiter"
            ),
        ]

        try:
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
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
                    logger.critical(
                        f"Task {task.get_name()} failed: {e}"
                    )

        except Exception as e:
            logger.critical(f"Engine error: {e}")
        finally:
            await self._cleanup()

    def _on_trade(self, trade: TradeData):
        """
        Callback для каждого трейда.
        Фаза 1: логирование и мониторинг.
        Фаза 2: здесь будут вызываться анализаторы.
        """
        # TODO Phase 2:
        # signals = []
        # for analyzer in self.analyzers.values():
        #     signal = analyzer.on_trade(trade)
        #     if signal:
        #         signals.append(signal)
        #
        # if signals:
        #     final = self.aggregator.evaluate(signals)
        #     if final and self._pass_filters(final):
        #         asyncio.create_task(
        #             self.position_manager.open_position(final)
        #         )
        pass

    def _on_orderbook_update(self, message: dict):
        """
        Callback для обновлений стакана.
        OrderBookManager уже обновлён в DataFeed.
        """
        # TODO Phase 2: DepthShot analyzer
        pass

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
        """Периодический отчёт о состоянии"""
        while self._running:
            await asyncio.sleep(60)

            stats = self._collect_stats()

            # Безопасное извлечение данных
            df = stats.get('data_feed', {})
            lat = stats.get('latency', {})
            exe = stats.get('executor', {})
            rl = exe.get('rate_limiter', {})

            logger.info(
                f"=== STATUS === "
                f"Trades: {df.get('trade_count', 0)} | "
                f"OB updates: {df.get('orderbook_updates', 0)} | "
                f"Latency: {lat.get('ws_latency_ms', 0)}ms "
                f"({lat.get('current_level', 'unknown')}) | "
                f"API remaining: {rl.get('remaining', 0)}"
            )

    def _collect_stats(self) -> dict:
        """Собрать статистику со всех компонентов"""
        symbols = self.config.get(
            'pairs', {}
        ).get('symbols', [])

        return {
            'data_feed': self.data_feed.get_stats(),
            'latency': self.latency_guard.get_stats(),
            'orderbooks': self.orderbook_manager.get_all_stats(),
            'volatility': {
                symbol: self.volatility_tracker.get_stats(symbol)
                for symbol in symbols
            },
            'executor': self.executor.get_stats(),
            'trade_logger': self.trade_logger.get_stats(),
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
