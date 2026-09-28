import asyncio

import pytest

from app.workers import run_pipeline


async def test_every_item_is_handled_once():
    seen = []

    async def handle(i, item):
        await asyncio.sleep(0)
        seen.append(i)

    await run_pipeline(((i, f"item{i}") for i in range(100)), handle, num_workers=5, queue_size=3)
    assert sorted(seen) == list(range(100))


async def test_skip_already_done():
    seen = []

    async def handle(i, item):
        seen.append(i)

    await run_pipeline(((i, None) for i in range(10)), handle, num_workers=2, queue_size=2, skip={1, 3, 5})
    assert sorted(seen) == [0, 2, 4, 6, 7, 8, 9]


async def test_workers_are_bounded():
    active = peak = 0

    async def handle(i, item):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.001)
        active -= 1

    await run_pipeline(((i, None) for i in range(50)), handle, num_workers=4, queue_size=10)
    assert peak == 4


async def test_reader_cannot_run_ahead_of_workers():
    """Backpressure: items pulled from the reader stay within queue_size + workers of items finished."""
    pulled = finished = 0
    max_gap = 0

    def reader():
        nonlocal pulled
        for i in range(200):
            pulled += 1
            yield i, None

    async def handle(i, item):
        nonlocal finished, max_gap
        max_gap = max(max_gap, pulled - finished)
        await asyncio.sleep(0.001)
        finished += 1

    await run_pipeline(reader(), handle, num_workers=3, queue_size=5)
    assert max_gap <= 5 + 3 + 1


async def test_fatal_error_stops_everything():
    handled = 0

    async def handle(i, item):
        nonlocal handled
        handled += 1
        if i == 3:
            raise PermissionError("bad key")
        await asyncio.sleep(0.01)

    with pytest.raises(PermissionError):
        await run_pipeline(((i, None) for i in range(1000)), handle, num_workers=2, queue_size=2)
    assert handled < 20
