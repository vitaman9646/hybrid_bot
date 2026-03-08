"""
monitoring/telegram_commands.py — обработка команд из Telegram

Команды:
    /status   — текущий статус бота
    /positions — открытые позиции
    /pnl      — P&L сессии
    /pause    — приостановить торговлю
    /resume   — возобновить торговлю
    /stop     — остановить бота
    /risk     — статус RiskManager
    /help     — список команд
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.engine import HybridEngine

logger = logging.getLogger(__name__)


class TelegramCommands:
    """
    Long-polling обработчик команд.
    Запускается как отдельная asyncio задача.
    """

    def __init__(self, config: dict, engine: "HybridEngine"):
        self._token = config.get('bot_token', '')
        self._chat_id = str(config.get('chat_id', ''))
        self._enabled = config.get('enabled', False) and bool(self._token)
        self._engine = engine
        self._offset = 0
        self._running = False
        self._paused = False    # флаг паузы торговли
        self._poll_interval = 2.0
        self._base_url = f"https://api.telegram.org/bot{self._token}"
        logger.info("TelegramCommands initialized (enabled=%s)", self._enabled)

    # ------------------------------------------------------------------
    # Запуск
    # ------------------------------------------------------------------

    async def start(self):
        if not self._enabled:
            logger.info("TelegramCommands disabled")
            return

        self._running = True
        logger.info("TelegramCommands polling started")

        while self._running:
            try:
                updates = await self._get_updates()
                for update in updates:
                    await self._handle_update(update)
            except Exception as e:
                logger.warning("TelegramCommands poll error: %s", e)

            await asyncio.sleep(self._poll_interval)

    def stop(self):
        self._running = False

    @property
    def is_paused(self) -> bool:
        return self._paused

    # ------------------------------------------------------------------
    # Обработка обновлений
    # ------------------------------------------------------------------

    async def _get_updates(self) -> list[dict]:
        import httpx
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"{self._base_url}/getUpdates",
                    params={'offset': self._offset, 'timeout': 25},
                )
                if resp.status_code != 200:
                    return []
                data = resp.json()
                updates = data.get('result', [])
                if updates:
                    self._offset = updates[-1]['update_id'] + 1
                return updates
        except Exception as e:
            logger.debug("getUpdates error: %s", e)
            return []

    async def _handle_update(self, update: dict):
        msg = update.get('message', {})
        if not msg:
            return

        # Проверяем что сообщение от нашего chat_id
        chat_id = str(msg.get('chat', {}).get('id', ''))
        if self._chat_id and chat_id != self._chat_id:
            logger.warning("TelegramCommands: ignored message from chat_id=%s", chat_id)
            return

        text = msg.get('text', '').strip()
        if not text.startswith('/'):
            return

        command = text.split()[0].lower().lstrip('/')
        # Убираем @botname если есть
        command = command.split('@')[0]

        # Парсим аргументы команды
        parts = text.split()
        self._last_cmd_args = parts[1:] if len(parts) > 1 else []

        logger.info("TelegramCommands: received /%s args=%s from chat %s", command, self._last_cmd_args, chat_id)

        handlers = {
            'status':    self._cmd_status,
            'positions': self._cmd_positions,
            'pnl':       self._cmd_pnl,
            'pause':     self._cmd_pause,
            'resume':    self._cmd_resume,
            'stop':      self._cmd_stop,
            'risk':      self._cmd_risk,
            'help':      self._cmd_help,
            'start':     self._cmd_help,
            'backtest':  self._cmd_backtest,
            'optimize':  self._cmd_optimize,
            'kill':      self._cmd_kill,
        }

        handler = handlers.get(command)
        if handler:
            try:
                await handler(chat_id)
            except Exception as e:
                logger.error("Command /%s error: %s", command, e)
                await self._reply(chat_id, f"❌ Error: {e}")
        else:
            await self._reply(chat_id, f"❓ Unknown command: /{command}\n\n/help — list commands")

    # ------------------------------------------------------------------
    # Команды
    # ------------------------------------------------------------------

    async def _cmd_help(self, chat_id: str):
        text = (
            "?? <b>Hybrid Trading Bot</b>\n\n"
            "/status    — bot status\n"
            "/positions — open positions\n"
            "/pnl       — session P&amp;L\n"
            "/risk      — risk manager status\n"
            "/pause     — pause trading\n"
            "/resume    — resume trading\n"
            "/stop      — stop bot\n"
            "/help      — this message"
        )
        await self._reply(chat_id, text)

    async def _cmd_status(self, chat_id: str):
        e = self._engine
        stats = e.data_feed.get_stats() if hasattr(e, 'data_feed') else {}
        df = stats.get('data_feed', stats)

        pos_count = len(e.position_manager.get_all_positions())
        paused_str = "⏸ PAUSED" if self._paused else "▶️ RUNNING"

        # Slippage
        slip = ""
        if hasattr(e, 'order_executor'):
            avg_slip = e.order_executor.get_avg_slippage()
            slip_stats = e.order_executor.get_slippage_stats()
            n = slip_stats.get('total_orders', 0)
            slip = f"\nAvg slippage: {avg_slip:.4f}% ({n} orders)"

        text = (
            f"?? <b>Bot Status</b>\n\n"
            f"State: {paused_str}\n"
            f"Open positions: {pos_count}\n"
            f"Balance: {e.risk_manager._balance_usdt:.2f} USDT\n"
            f"Session P&amp;L: {e.risk_manager.session_pnl:+.2f} USDT\n"
            f"Daily loss: {e.risk_manager.daily_loss_usdt:.2f} USDT\n"
            f"{slip}\n"
            f"DataFeed: {'OK' if df else 'N/A'}\n"
            f"Testnet: {e.config.get('exchange', {}).get('testnet', False)}"
        )
        await self._reply(chat_id, text)

    async def _cmd_positions(self, chat_id: str):
        positions = self._engine.position_manager.get_all_positions()
        if not positions:
            await self._reply(chat_id, "?? No open positions")
            return

        lines = ["?? <b>Open Positions</b>\n"]
        for pos in positions:
            age = (time.time() - pos.timestamp) / 60
            lines.append(
                f"<b>{pos.symbol}</b> {pos.direction.upper()}\n"
                f"  Entry: {pos.entry_price:.4f}\n"
                f"  TP: {pos.tp_price:.4f} | SL: {pos.sl_price:.4f}\n"
                f"  Qty: {pos.qty} | Age: {age:.0f}m\n"
                f"  Trailing: {pos.trailing_stop_price:.4f}\n"
            )
        await self._reply(chat_id, "\n".join(lines))

    async def _cmd_pnl(self, chat_id: str):
        rm = self._engine.risk_manager
        pm = self._engine.position_manager
        stats = pm.get_stats()

        text = (
            f"?? <b>Session P&amp;L</b>\n\n"
            f"Total P&amp;L: {rm.session_pnl:+.2f} USDT\n"
            f"Daily loss: {rm.daily_loss_usdt:.2f} USDT\n"
            f"Balance: {rm._balance_usdt:.2f} USDT\n\n"
            f"Trades opened: {stats['total_opened']}\n"
            f"Trades closed: {stats['total_closed']}\n"
            f"TP hits: {stats['tp_hits']}\n"
            f"SL hits: {stats['sl_hits']}\n"
            f"Win rate: {stats['tp_hits'] / max(stats['total_closed'], 1):.1%}"
        )
        await self._reply(chat_id, text)

    async def _cmd_risk(self, chat_id: str):
        rm = self._engine.risk_manager
        mult = rm._drawdown_multiplier()
        halted = rm.is_trading_halted

        text = (
            f"?? <b>Risk Manager</b>\n\n"
            f"Balance: {rm._balance_usdt:.2f} USDT\n"
            f"Position size: {rm.cfg.position_pct}% "
            f"= {rm._balance_usdt * rm.cfg.position_pct / 100:.2f} USDT\n"
            f"Session P&amp;L: {rm.session_pnl:+.2f} USDT\n"
            f"Daily loss: {rm.daily_loss_usdt:.2f} / "
            f"{rm.cfg.daily_loss_limit_usdt:.2f} USDT\n"
            f"Drawdown mult: {mult:.2f}\n"
            f"Trading halted: {'?? YES' if halted else '?? NO'}\n"
            f"Open symbols: {list(rm._open_symbols) or 'none'}"
        )
        await self._reply(chat_id, text)

    async def _cmd_pause(self, chat_id: str):
        self._paused = True
        await self._reply(chat_id, "⏸ Trading PAUSED\n\nOpen positions will continue.\nNew signals will be ignored.\n\n/resume to continue")

    async def _cmd_resume(self, chat_id: str):
        self._paused = False
        await self._reply(chat_id, "▶️ Trading RESUMED")

    async def _cmd_stop(self, chat_id: str):
        await self._reply(chat_id, "?? Stopping bot...\n\nAll positions will remain open on exchange.")
        logger.info("TelegramCommands: stop requested")
        self._engine._running = False
        self._engine._shutdown_event.set()

    async def _cmd_kill(self, chat_id: str):
        """Закрыть ВСЕ позиции рыночными ордерами + остановить бота."""
        await self._reply(chat_id, "\U0001f534 <b>KILL initiated</b>\nClosing all positions...")
        logger.warning("TelegramCommands: KILL command received")

        pm = self._engine.position_manager
        symbols = list(pm._positions.keys())
        closed = []
        failed = []

        for symbol in symbols:
            try:
                result = await pm.close_position(symbol, reason="kill_command")
                if result:
                    closed.append(symbol)
                else:
                    failed.append(symbol)
            except Exception as e:
                logger.error("KILL: failed to close %s: %s", symbol, e)
                failed.append(symbol)

        msg = "\U0001f534 <b>KILL complete</b>\n"
        if closed:
            msg += f"Closed: {', '.join(closed)}\n"
        if failed:
            msg += f"Failed: {', '.join(failed)}\n"
        if not symbols:
            msg += "No open positions.\n"
        msg += "\nBot stopped."

        await self._reply(chat_id, msg)
        self._engine._running = False
        self._engine._shutdown_event.set()

    # ------------------------------------------------------------------
    # Отправка
    # ------------------------------------------------------------------

    async def _reply(self, chat_id: str, text: str):
        import httpx
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"{self._base_url}/sendMessage",
                    json={
                        'chat_id': chat_id,
                        'text': text,
                        'parse_mode': 'HTML',
                    },
                )
        except Exception as e:
            logger.error("TelegramCommands reply error: %s", e)

    async def _cmd_backtest(self, chat_id: str):
        """
        /backtest BTCUSDT 7
        Запускает бэктест в фоне, отвечает результатом.
        """
        # Парсим аргументы из последнего сообщения (уже в chat_id контексте)
        # Вызывается через _handle_update где text доступен
        symbol = getattr(self, '_last_cmd_args', ['BTCUSDT'])[0] if hasattr(self, '_last_cmd_args') else 'BTCUSDT'
        days = int(getattr(self, '_last_cmd_args', [None, '7'])[1]) if hasattr(self, '_last_cmd_args') and len(self._last_cmd_args) > 1 else 7

        await self._reply(chat_id, f"⏳ Backtest <b>{symbol}</b> {days}d — starting...")
        asyncio.create_task(self._run_backtest(chat_id, symbol, days))

    async def _run_backtest(self, chat_id: str, symbol: str, days: int):
        import time, yaml
        from backtester.replay_engine import ReplayEngine
        try:
            cfg = yaml.safe_load(open('config/strategies/hybrid.yaml'))
            engine = ReplayEngine(cfg, db_path='data/history.db')
            ts_to = time.time()
            ts_from = ts_to - days * 86400
            t0 = time.time()
            result = await asyncio.get_event_loop().run_in_executor(
                None, engine.run, symbol, ts_from, ts_to
            )
            elapsed = time.time() - t0
            if result.total_trades == 0:
                await self._reply(chat_id, f"⚠️ <b>{symbol}</b>: no trades found\nTry different params or more days")
                return
            text = (
                f"✅ <b>Backtest {symbol} {days}d</b> ({elapsed:.0f}s)\n\n"
                f"Trades:    {result.total_trades}\n"
                f"Win rate:  {result.win_rate:.1%}\n"
                f"Total PnL: {result.total_pnl:+.2f} USDT\n"
                f"Sharpe:    {result.sharpe:.3f}\n"
                f"Max DD:    {result.max_drawdown:.2f} USDT\n"
                f"Prof.Factor: {result.profit_factor:.2f}"
            )
            await self._reply(chat_id, text)
        except Exception as e:
            await self._reply(chat_id, f"❌ Backtest error: {e}")

    async def _cmd_optimize(self, chat_id: str):
        """
        /optimize BTCUSDT
        Запускает быструю оптимизацию в фоне.
        """
        symbol = getattr(self, '_last_cmd_args', ['BTCUSDT'])[0] if hasattr(self, '_last_cmd_args') else 'BTCUSDT'
        await self._reply(chat_id, f"⏳ Optimization <b>{symbol}</b> (fast, 32 combos) — starting...\nThis takes ~10-30 min")
        asyncio.create_task(self._run_optimize(chat_id, symbol))

    async def _run_optimize(self, chat_id: str, symbol: str):
        import time, yaml
        from backtester.replay_engine import ReplayEngine
        from backtester.optimizer import ParameterOptimizer
        PARAM_GRID_FAST = {
            'analyzers.vector.min_spread_size':  [0.003, 0.01],
            'analyzers.vector.min_quote_volume': [50, 500],
            'order.take_profit.percent':         [0.5, 0.8, 1.2],
            'order.stop_loss.percent':           [1.0, 1.5, 2.0],
            'min_score':                         [0.5, 0.6],
        }
        try:
            cfg = yaml.safe_load(open('config/strategies/hybrid.yaml'))
            opt = ParameterOptimizer(cfg, db_path='data/history.db')
            ts_to = time.time()
            ts_from = ts_to - 7 * 86400
            t0 = time.time()
            results = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: opt.run(
                    symbol=symbol,
                    ts_from=ts_from,
                    ts_to=ts_to,
                    param_grid=PARAM_GRID_FAST,
                    metric='sharpe',
                    min_trades=20,
                )
            )
            elapsed = time.time() - t0
            if not results:
                await self._reply(chat_id, f"⚠️ <b>{symbol}</b>: no results (try --min-trades 5)")
                return
            best = results[0]
            r = best.result
            # Сохраняем
            import yaml as _yaml
            out = f"data/best_{symbol}_sharpe.yaml"
            with open(out, 'w') as f:
                _yaml.dump({
                    'symbol': symbol, 'metric': 'sharpe',
                    'metric_value': round(best.metric_value, 4),
                    'params': best.params,
                    'stats': {
                        'trades': r.total_trades,
                        'win_rate': round(r.win_rate, 4),
                        'total_pnl': round(r.total_pnl, 4),
                        'sharpe': round(r.sharpe, 4),
                        'max_drawdown': round(r.max_drawdown, 4),
                        'profit_factor': round(r.profit_factor, 4),
                    },
                }, f, default_flow_style=False)
            text = (
                f"✅ <b>Optimization {symbol}</b> ({elapsed:.0f}s, {len(results)} results)\n\n"
                f"Best sharpe: {best.metric_value:.3f}\n"
                f"Trades:      {r.total_trades}\n"
                f"Win rate:    {r.win_rate:.1%}\n"
                f"PnL:         {r.total_pnl:+.2f} USDT\n"
                f"Prof.Factor: {r.profit_factor:.2f}\n\n"
                f"<b>Best params:</b>\n"
                + "\n".join(f"  {k}: {v}" for k, v in best.params.items())
                + f"\n\nSaved to {out}\n"
                f"Apply: /apply_{symbol.lower()}"
            )
            await self._reply(chat_id, text)
        except Exception as e:
            await self._reply(chat_id, f"❌ Optimize error: {e}")
