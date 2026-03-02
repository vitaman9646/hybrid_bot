# tests/test_rate_limiter.py
import pytest
import asyncio
import time

from execution.rate_limiter import RateLimiter


@pytest.mark.asyncio
class TestRateLimiter:
    async def test_basic_acquire(self):
        limiter = RateLimiter(max_requests=10, window=1)

        await limiter.acquire()
        assert limiter.remaining == 9

    async def test_multiple_acquire(self):
        limiter = RateLimiter(max_requests=5, window=1)

        for _ in range(3):
            await limiter.acquire()

        assert limiter.remaining == 2

    async def test_usage_pct(self):
        limiter = RateLimiter(max_requests=10, window=1)

        for _ in range(5):
            await limiter.acquire()

        assert limiter.usage_pct == pytest.approx(50.0, abs=5)

    async def test_rate_limit_wait(self):
        limiter = RateLimiter(max_requests=3, window=1)

        for _ in range(3):
            await limiter.acquire()

        start = time.time()
        await limiter.acquire()
        elapsed = time.time() - start

        assert elapsed >= 0.5

    async def test_stats(self):
        limiter = RateLimiter(max_requests=100, window=60)

        for _ in range(5):
            await limiter.acquire()

        stats = limiter.get_stats()
        assert stats['total_requests'] == 5
        assert stats['remaining'] == 95
        assert stats['total_waits'] == 0
