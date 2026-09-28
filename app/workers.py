"""Scatter: a bounded queue feeding a fixed pool of worker tasks.

The producer pulls items from the streaming reader and puts them on a queue with
a maximum size. When the queue is full, `put` waits, so the reader can never
race ahead of the workers. That is the backpressure that keeps memory flat: at
most `queue_size + num_workers` items exist in memory, whatever the file size.
"""

import asyncio
from collections.abc import Awaitable, Callable, Container, Iterable
from typing import Any

_STOP = object()

Handler = Callable[[int, Any], Awaitable[None]]


async def run_pipeline(
    items: Iterable[tuple[int, Any]],
    handle: Handler,
    num_workers: int,
    queue_size: int,
    skip: Container[int] = frozenset(),
) -> None:
    """Feed every (index, item) not in `skip` to `handle`, using `num_workers` workers.

    `handle` is expected to record per-item failures itself. If it raises, that
    is fatal (e.g. bad API key): the TaskGroup cancels the reader and every
    other worker, and the exception is re-raised here.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)

    async def producer() -> None:
        for index, item in items:
            if index in skip:
                continue
            await queue.put((index, item))
        for _ in range(num_workers):
            await queue.put(_STOP)

    async def worker() -> None:
        while True:
            entry = await queue.get()
            if entry is _STOP:
                return
            await handle(*entry)

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(producer())
            for _ in range(num_workers):
                tg.create_task(worker())
    except BaseExceptionGroup as eg:
        # Surface the original error (e.g. AuthError) rather than a group.
        raise eg.exceptions[0] from None
