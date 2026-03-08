"""
monitoring/health_server.py — простой HTTP health check сервер
GET /health → {"status": "ok", "uptime": 123}
GET /status → полный статус бота
GET /metrics → числовые метрики для Prometheus/Grafana
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING
from aiohttp import web

if TYPE_CHECKING:
    from core.engine import HybridEngine

logger = logging.getLogger(__name__)


class HealthServer:

    def __init__(self, engine: "HybridEngine", port: int = 8080):
        self._engine = engine
        self._port = port
        self._start_time = time.time()
        self._app = web.Application()
        self._app.router.add_get('/health',  self._handle_health)
        self._app.router.add_get('/status',  self._handle_status)
        self._app.router.add_get('/metrics', self._handle_metrics)
        self._runner = None

    async def start(self):
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, '0.0.0.0', self._port)
        await site.start()
        logger.info("HealthServer started on port %d", self._port)

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    # ------------------------------------------------------------------

    async def _handle_health(self, request: web.Request) -> web.Response:
        e = self._engine
        uptime = int(time.time() - self._start_time)
        is_healthy = (
            hasattr(e, 'data_feed') and
            e.data_feed.is_connected and
            not e.risk_manager.is_trading_halted
        )
        data = {
            "status": "ok" if is_healthy else "degraded",
            "uptime_sec": uptime,
            "connected": e.data_feed.is_connected if hasattr(e, 'data_feed') else False,
            "trading_halted": e.risk_manager.is_trading_halted,
            "testnet": e.config.get('exchange', {}).get('testnet', True),
        }
        status = 200 if is_healthy else 503
        return web.Response(
            text=json.dumps(data, indent=2),
            content_type='application/json',
            status=status,
        )

    async def _handle_status(self, request: web.Request) -> web.Response:
        e = self._engine
        rm = e.risk_manager
        pm = e.position_manager
        stats = pm.get_stats()
        positions = pm.get_all_positions()

        data = {
            "uptime_sec": int(time.time() - self._start_time),
            "testnet": e.config.get('exchange', {}).get('testnet', True),
            "trading_halted": rm.is_trading_halted,
            "balance_usdt": round(rm._balance_usdt, 2),
            "session_pnl": round(rm.session_pnl, 4),
            "daily_loss_usdt": round(rm.daily_loss_usdt, 4),
            "open_positions": len(positions),
            "positions": [
                {
                    "symbol": p.symbol,
                    "direction": p.direction,
                    "entry": p.entry_price,
                    "tp": p.tp_price,
                    "sl": p.sl_price,
                    "qty": p.qty,
                    "age_sec": int(time.time() - p.timestamp),
                }
                for p in positions
            ],
            "stats": {
                "total_opened": stats.get('total_opened', 0),
                "total_closed": stats.get('total_closed', 0),
                "tp_hits": stats.get('tp_hits', 0),
                "sl_hits": stats.get('sl_hits', 0),
                "win_rate": round(
                    stats.get('tp_hits', 0) /
                    max(stats.get('total_closed', 0), 1), 4
                ),
            },
            "data_feed": e.data_feed.get_stats() if hasattr(e, 'data_feed') else {},
        }
        return web.Response(
            text=json.dumps(data, indent=2),
            content_type='application/json',
        )

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        """Prometheus-совместимый формат"""
        e = self._engine
        rm = e.risk_manager
        pm = e.position_manager
        stats = pm.get_stats()
        uptime = int(time.time() - self._start_time)

        lines = [
            f"# HELP bot_uptime_seconds Bot uptime in seconds",
            f"# TYPE bot_uptime_seconds gauge",
            f"bot_uptime_seconds {uptime}",
            f"# HELP bot_balance_usdt Current balance in USDT",
            f"# TYPE bot_balance_usdt gauge",
            f"bot_balance_usdt {rm._balance_usdt:.4f}",
            f"# HELP bot_session_pnl Session PnL in USDT",
            f"# TYPE bot_session_pnl gauge",
            f"bot_session_pnl {rm.session_pnl:.4f}",
            f"# HELP bot_daily_loss Daily loss in USDT",
            f"# TYPE bot_daily_loss gauge",
            f"bot_daily_loss {rm.daily_loss_usdt:.4f}",
            f"# HELP bot_open_positions Number of open positions",
            f"# TYPE bot_open_positions gauge",
            f"bot_open_positions {len(pm.get_all_positions())}",
            f"# HELP bot_trades_total Total trades opened",
            f"# TYPE bot_trades_total counter",
            f"bot_trades_total {stats.get('total_opened', 0)}",
            f"# HELP bot_tp_hits Total TP hits",
            f"# TYPE bot_tp_hits counter",
            f"bot_tp_hits {stats.get('tp_hits', 0)}",
            f"# HELP bot_sl_hits Total SL hits",
            f"# TYPE bot_sl_hits counter",
            f"bot_sl_hits {stats.get('sl_hits', 0)}",
            f"# HELP bot_trading_halted Trading halted flag",
            f"# TYPE bot_trading_halted gauge",
            f"bot_trading_halted {1 if rm.is_trading_halted else 0}",
            f"# HELP bot_connected WebSocket connected flag",
            f"# TYPE bot_connected gauge",
            f"bot_connected {1 if e.data_feed.is_connected else 0}",
        ]
        return web.Response(
            text="\n".join(lines) + "\n",
            content_type='text/plain',
        )
