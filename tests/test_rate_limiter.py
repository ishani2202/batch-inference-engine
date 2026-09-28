import asyncio
import time

import pytest

from app.rate_limiter import AdaptiveLimiter


async def test_429_halves_limit():
    lim = AdaptiveLimiter(1, 32, 16)
    epoch = await lim.acquire()
    await lim.on_rate_limited(epoch)
    assert lim.limit == 8
    assert lim.rate_limited_count == 1


async def test_limit_never_below_min():
    lim = AdaptiveLimiter(3, 32, 4)
    for _ in range(5):
        epoch = await lim.acquire()
        await lim.release()
        await lim.on_rate_limited(epoch)
    assert lim.limit == 3


async def test_one_burst_of_429s_cuts_once():
    """Requests sent before the first cut shouldn't cut again: 10 x 429 from one burst = one halving."""
    lim = AdaptiveLimiter(1, 32, 16)
    epochs = [await lim.acquire() for _ in range(10)]
    for e in epochs:
        await lim.on_rate_limited(e)
    assert lim.limit == 8
    assert lim.rate_limited_count == 10


async def test_slow_start_grows_one_per_success():
    lim = AdaptiveLimiter(1, 32, 4)
    for _ in range(4):
        await lim.on_success()
    assert lim.limit == 8


async def test_additive_increase_after_429():
    lim = AdaptiveLimiter(1, 32, 16)
    await lim.on_rate_limited(await lim.acquire())
    assert lim.limit == 8
    for _ in range(7):
        await lim.on_success()
    assert lim.limit == 8  # needs `limit` (8) successes for +1
    await lim.on_success()
    assert lim.limit == 9


async def test_limit_never_above_max():
    lim = AdaptiveLimiter(1, 5, 4)
    for _ in range(50):
        await lim.on_success()
    assert lim.limit == 5


async def test_acquire_blocks_at_limit_until_release():
    lim = AdaptiveLimiter(1, 1, 1)
    await lim.acquire()
    waiter = asyncio.create_task(lim.acquire())
    await asyncio.sleep(0.02)
    assert not waiter.done()
    await lim.release()
    await asyncio.wait_for(waiter, 1)
    assert lim.in_flight == 1


async def test_after_a_cut_new_requests_wait_for_in_flight_to_drain():
    lim = AdaptiveLimiter(1, 8, 4)
    epochs = [await lim.acquire() for _ in range(4)]
    await lim.on_rate_limited(epochs[0])  # limit 4 -> 2, still 4 in flight
    await lim.release()  # 3 in flight
    waiter = asyncio.create_task(lim.acquire())
    await asyncio.sleep(0.02)
    assert not waiter.done()  # 3 >= 2: must wait
    await lim.release()
    await asyncio.sleep(0.02)
    assert not waiter.done()  # 2 >= 2: still waits
    await lim.release()
    await asyncio.wait_for(waiter, 1)  # 1 < 2: goes
    assert lim.in_flight == 2


async def test_retry_after_pauses_all_new_requests():
    lim = AdaptiveLimiter(1, 8, 4)
    await lim.on_rate_limited(await lim.acquire(), retry_after=0.2)
    await lim.release()
    t0 = time.monotonic()
    await lim.acquire()
    assert time.monotonic() - t0 >= 0.18


def test_invalid_bounds_rejected():
    with pytest.raises(ValueError):
        AdaptiveLimiter(5, 2, 3)
