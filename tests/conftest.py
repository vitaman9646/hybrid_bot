# tests/conftest.py
"""
Pytest конфигурация и общие фикстуры.
"""
import pytest
import asyncio
import time
import sys
import os

# Добавляем корень проекта в path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def latency_config():
    """Конфигурация LatencyGuard для тестов"""
    return {
        'warn_threshold_ms': 300,
        'critical_threshold_ms': 500,
        'emergency_threshold_ms': 1000,
        'check_interval': 5,
        'track_order_rtt': True,
        'rtt_window_size': 100,
    }


@pytest.fixture
def exchange_config():
    """Конфигурация биржи для тестов"""
    return {
        'name': 'bybit',
        'testnet': True,
        'api_key': 'test_key',
        'api_secret': 'test_secret',
        'rate_limit': 950,
        'max_retries': 3,
        'retry_delay': 0.1,
        'ws_channel_type': 'linear',
        'ws_ping_interval': 20,
        'ws_reconnect_delay': 5,
        'ws_max_reconnect_attempts': 50,
        'ws_trace_logging': False,
        'symbols': ['BTCUSDT', 'ETHUSDT'],
    }


@pytest.fixture
def sample_trade_message():
    """Пример trade сообщения от Bybit WebSocket"""
    return {
        'topic': 'publicTrade.BTCUSDT',
        'type': 'snapshot',
        'data': [
            {
                's': 'BTCUSDT',
                'p': '60000.50',
                'v': '0.001',
                'S': 'Buy',
                'T': int(time.time() * 1000),
                'i': 'trade_001',
            },
        ],
        'ts': int(time.time() * 1000),
    }


@pytest.fixture
def sample_orderbook_snapshot():
    """Пример orderbook snapshot от Bybit"""
    return {
        'topic': 'orderbook.50.BTCUSDT',
        'type': 'snapshot',
        'ts': int(time.time() * 1000),
        'data': {
            's': 'BTCUSDT',
            'b': [
                ['60000.00', '1.000'],
                ['59999.50', '2.500'],
                ['59999.00', '3.000'],
                ['59998.00', '5.000'],
                ['59995.00', '10.000'],
            ],
            'a': [
                ['60000.50', '1.200'],
                ['60001.00', '2.000'],
                ['60001.50', '3.500'],
                ['60002.00', '4.000'],
                ['60005.00', '8.000'],
            ],
            'u': 1,
        },
    }


@pytest.fixture
def sample_orderbook_delta():
    """Пример orderbook delta от Bybit"""
    return {
        'topic': 'orderbook.50.BTCUSDT',
        'type': 'delta',
        'ts': int(time.time() * 1000),
        'data': {
            's': 'BTCUSDT',
            'b': [
                ['60000.00', '2.000'],   # обновлённый уровень
                ['59997.00', '7.000'],   # новый уровень
            ],
            'a': [
                ['60000.50', '0'],       # удалённый уровень
                ['60003.00', '5.000'],   # новый уровень
            ],
            'u': 2,
        },
    }
