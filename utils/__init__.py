# utils/__init__.py
"""Утилиты и хелперы"""

import time
from typing import Any


def timestamp_ms() -> int:
    """Текущий timestamp в миллисекундах"""
    return int(time.time() * 1000)


def timestamp_s() -> float:
    """Текущий timestamp в секундах"""
    return time.time()


def round_price(price: float, tick_size: float) -> float:
    """Округлить цену до шага тика"""
    if tick_size <= 0:
        return price
    return round(round(price / tick_size) * tick_size, 10)


def round_qty(qty: float, step_size: float) -> float:
    """Округлить количество до шага лота"""
    if step_size <= 0:
        return qty
    return round(round(qty / step_size) * step_size, 10)


def pct_change(old: float, new: float) -> float:
    """Процентное изменение"""
    if old == 0:
        return 0.0
    return ((new - old) / old) * 100


def format_usdt(amount: float) -> str:
    """Форматирование USDT"""
    if abs(amount) >= 1000:
        return f"${amount:,.2f}"
    return f"${amount:.4f}"


def safe_divide(a: float, b: float, default: float = 0.0) -> float:
    """Безопасное деление"""
    if b == 0:
        return default
    return a / b
