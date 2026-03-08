"""
tests/test_health_server.py — тесты для HealthServer
"""
import pytest
import json
from unittest.mock import MagicMock, AsyncMock, patch
from aiohttp.test_utils import AioHTTPTestCase
from aiohttp import web

from monitoring.health_server import HealthServer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_engine(
    connected: bool = True,
    halted: bool = False,
    balance: float = 100.0,
    pnl: float = 1.5,
    daily_loss: float = 2.0,
    open_positions: int = 1,
    testnet: bool = False,
) -> MagicMock:
    engine = MagicMock()
    engine.config = {'exchange': {'testnet': testnet}}

    # data_feed
    engine.data_feed = MagicMock()
    engine.data_feed.is_connected = connected
    engine.data_feed.get_stats = MagicMock(return_value={})

    # risk_manager
    engine.risk_manager = MagicMock()
    engine.risk_manager.is_trading_halted = halted
    engine.risk_manager._balance_usdt = balance
    engine.risk_manager.session_pnl = pnl
    engine.risk_manager.daily_loss_usdt = daily_loss

    # position_manager
    pos = MagicMock()
    pos.symbol = "BTCUSDT"
    pos.direction = "long"
    pos.entry_price = 50000.0
    pos.tp_price = 50400.0
    pos.sl_price = 49250.0
    pos.qty = 0.002
    pos.timestamp = 1000.0

    engine.position_manager = MagicMock()
    engine.position_manager.get_all_positions = MagicMock(
        return_value=[pos] * open_positions
    )
    engine.position_manager.get_stats = MagicMock(return_value={
        'total_opened': 10,
        'total_closed': 8,
        'tp_hits': 6,
        'sl_hits': 2,
    })

    return engine


def make_server(engine=None) -> HealthServer:
    if engine is None:
        engine = make_engine()
    return HealthServer(engine, port=18080)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHealthEndpoint(AioHTTPTestCase):

    async def get_application(self):
        self.engine = make_engine(connected=True, halted=False)
        self.server = make_server(self.engine)
        return self.server._app

    async def test_health_ok_when_connected(self):
        resp = await self.client.get('/health')
        assert resp.status == 200
        data = await resp.json()
        assert data['status'] == 'ok'
        assert data['connected'] is True
        assert data['trading_halted'] is False

    async def test_health_degraded_when_disconnected(self):
        self.engine.data_feed.is_connected = False
        resp = await self.client.get('/health')
        assert resp.status == 503
        data = await resp.json()
        assert data['status'] == 'degraded'

    async def test_health_degraded_when_halted(self):
        self.engine.risk_manager.is_trading_halted = True
        resp = await self.client.get('/health')
        assert resp.status == 503
        data = await resp.json()
        assert data['status'] == 'degraded'

    async def test_health_has_uptime(self):
        resp = await self.client.get('/health')
        data = await resp.json()
        assert 'uptime_sec' in data
        assert data['uptime_sec'] >= 0

    async def test_health_testnet_flag(self):
        self.engine.config = {'exchange': {'testnet': True}}
        resp = await self.client.get('/health')
        data = await resp.json()
        assert data['testnet'] is True


class TestStatusEndpoint(AioHTTPTestCase):

    async def get_application(self):
        self.engine = make_engine(balance=103.5, pnl=2.5, open_positions=2)
        self.server = make_server(self.engine)
        return self.server._app

    async def test_status_200(self):
        resp = await self.client.get('/status')
        assert resp.status == 200

    async def test_status_balance(self):
        resp = await self.client.get('/status')
        data = await resp.json()
        assert data['balance_usdt'] == 103.5

    async def test_status_pnl(self):
        resp = await self.client.get('/status')
        data = await resp.json()
        assert data['session_pnl'] == 2.5

    async def test_status_positions(self):
        resp = await self.client.get('/status')
        data = await resp.json()
        assert data['open_positions'] == 2
        assert len(data['positions']) == 2
        assert data['positions'][0]['symbol'] == 'BTCUSDT'

    async def test_status_stats(self):
        resp = await self.client.get('/status')
        data = await resp.json()
        assert data['stats']['total_opened'] == 10
        assert data['stats']['tp_hits'] == 6
        assert data['stats']['win_rate'] == pytest.approx(0.75)


class TestMetricsEndpoint(AioHTTPTestCase):

    async def get_application(self):
        self.engine = make_engine(balance=100.0, pnl=1.0)
        self.server = make_server(self.engine)
        return self.server._app

    async def test_metrics_200(self):
        resp = await self.client.get('/metrics')
        assert resp.status == 200

    async def test_metrics_content_type(self):
        resp = await self.client.get('/metrics')
        assert 'text/plain' in resp.content_type

    async def test_metrics_contains_balance(self):
        resp = await self.client.get('/metrics')
        text = await resp.text()
        assert 'bot_balance_usdt' in text
        assert '100.0' in text

    async def test_metrics_contains_all_keys(self):
        resp = await self.client.get('/metrics')
        text = await resp.text()
        for key in ['bot_uptime_seconds', 'bot_balance_usdt', 'bot_session_pnl',
                    'bot_open_positions', 'bot_trades_total', 'bot_tp_hits',
                    'bot_sl_hits', 'bot_trading_halted', 'bot_connected']:
            assert key in text, f"Missing metric: {key}"

    async def test_metrics_halted_flag(self):
        self.engine.risk_manager.is_trading_halted = True
        resp = await self.client.get('/metrics')
        text = await resp.text()
        assert 'bot_trading_halted 1' in text
