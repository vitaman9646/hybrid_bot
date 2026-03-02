# tests/test_rate_limiter.py
import pytest
import asyncio
import time
from execution.rate_limiter import RateLimiter


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_basic_acquire(self):
        limiter = RateLimiter(max_requests=10, window=1)
        
        await limiter.acquire()
        assert limiter.remaining == 9
    
    @pytest.mark.asyncio
    async def test_remaining(self):
        limiter = RateLimiter(max_requests=5, window=1)
        
        for _ in range(3):
            await limiter.acquire()
        
        assert limiter.remaining == 2
    
    @pytest.mark.asyncio
    async def test_rate_limit_wait(self):
        limiter = RateLimiter(max_requests=3, window=1)
        
        # Быстро исчерпываем лимит
        for _ in range(3):
            await limiter.acquire()
        
        # Следующий запрос должен ждать
        start = time.time()
        await limiter.acquire()
        elapsed = time.time() - start
        
        # Должен был подождать (примерно 1 секунду)
        assert elapsed >= 0.5
    
    @pytest.mark.asyncio
    async def test_stats(self):
        limiter = RateLimiter(max_requests=100, window=60)
        
        for _ in range(5):
            await limiter.acquire()
        
        stats = limiter.get_stats()
        assert stats['total_requests'] == 5
        assert stats['remaining'] == 95
