# execution/rate_limiter.py
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Контроль API лимитов Bybit.
    Bybit limit: 1000 req/min для большинства эндпоинтов.
    Используем 950 для безопасности.
    """
    
    def __init__(
        self,
        max_requests: int = 950,
        window: int = 60,
    ):
        self.max_requests = max_requests
        self.window = window
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()
        
        # Статистика
        self._total_requests = 0
        self._total_waits = 0
        self._total_wait_time = 0.0
    
    async def acquire(self):
        """
        Ждёт пока не освободится слот.
        Thread/task safe через asyncio.Lock.
        """
        async with self._lock:
            now = time.time()
            
            # Очищаем записи старше window
            cutoff = now - self.window
            while (
                self._timestamps
                and self._timestamps[0] < cutoff
            ):
                self._timestamps.popleft()
            
            if len(self._timestamps) >= self.max_requests:
                # Вычисляем время ожидания
                oldest = self._timestamps[0]
                wait_time = oldest + self.window - now + 0.05
                wait_time = max(0.01, wait_time)
                
                self._total_waits += 1
                self._total_wait_time += wait_time
                
                logger.warning(
                    f"Rate limit reached ({len(self._timestamps)}"
                    f"/{self.max_requests}), "
                    f"waiting {wait_time:.2f}s"
                )
                
                await asyncio.sleep(wait_time)
                
                # После ожидания очищаем снова
                now = time.time()
                cutoff = now - self.window
                while (
                    self._timestamps
                    and self._timestamps[0] < cutoff
                ):
                    self._timestamps.popleft()
            
            self._timestamps.append(now)
            self._total_requests += 1
    
    @property
    def remaining(self) -> int:
        """Оставшееся количество запросов"""
        now = time.time()
        cutoff = now - self.window
        active = sum(
            1 for t in self._timestamps if t > cutoff
        )
        return max(0, self.max_requests - active)
    
    @property
    def usage_pct(self) -> float:
        """Процент использования лимита"""
        used = self.max_requests - self.remaining
        return (used / self.max_requests) * 100
    
    def get_stats(self) -> dict:
        return {
            'remaining': self.remaining,
            'usage_pct': round(self.usage_pct, 1),
            'total_requests': self._total_requests,
            'total_waits': self._total_waits,
            'total_wait_time': round(
                self._total_wait_time, 2
            ),
            'avg_wait_time': round(
                (
                    self._total_wait_time / self._total_waits
                    if self._total_waits > 0
                    else 0
                ),
                3,
            ),
        }
