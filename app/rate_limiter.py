"""Shared adaptive concurrency controller (AIMD), one per job.

Every worker must hold a slot while its HTTP request is in flight. The number of
slots (the "limit") adapts to the upstream API:

* Slow start: until the first 429, every success adds one slot, so the limit
  roughly doubles each round trip and quickly finds the API's ceiling.
* Additive increase: after a 429, the limit grows by one only after `limit`
  successes in a row, i.e. about one slot per round trip.
* Multiplicative decrease: a 429 halves the limit (never below the minimum).
* Retry-After: a 429 carrying Retry-After pauses *all* new requests until then.

Why shared instead of per-request backoff: if 30 workers back off independently
they wake up at about the same time and hit the limit again (thundering herd).
Here the whole pool slows down together and then probes back up. This is the
same idea TCP uses for congestion control.
"""

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

log = logging.getLogger(__name__)


class AdaptiveLimiter:
    def __init__(
        self,
        min_limit: int,
        max_limit: int,
        start_limit: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 1 <= min_limit <= max_limit:
            raise ValueError("need 1 <= min_limit <= max_limit")
        self.min_limit = min_limit
        self.max_limit = max_limit
        self._limit = max(min_limit, min(start_limit, max_limit))
        self._clock = clock
        self._cond = asyncio.Condition()
        self._in_flight = 0
        self._pause_until = 0.0
        self._slow_start = True
        self._streak = 0  # successes since the last limit change
        # Bumped on every decrease. A 429 only cuts the limit if the request was
        # sent in the current epoch, so one burst of 429s causes a single cut.
        self._epoch = 0
        self.rate_limited_count = 0
        self.peak_in_flight = 0

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def in_flight(self) -> int:
        return self._in_flight

    async def acquire(self) -> int:
        """Wait for a free slot (and for any Retry-After pause). Returns the epoch."""
        async with self._cond:
            while True:
                wait = self._pause_until - self._clock()
                if wait > 0:
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=wait)
                    except TimeoutError:
                        pass
                    continue
                if self._in_flight < self._limit:
                    self._in_flight += 1
                    self.peak_in_flight = max(self.peak_in_flight, self._in_flight)
                    return self._epoch
                await self._cond.wait()

    async def release(self) -> None:
        async with self._cond:
            self._in_flight -= 1
            self._cond.notify_all()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[int]:
        epoch = await self.acquire()
        try:
            yield epoch
        finally:
            await self.release()

    async def on_success(self) -> None:
        async with self._cond:
            self._streak += 1
            threshold = 1 if self._slow_start else self._limit
            if self._streak >= threshold and self._limit < self.max_limit:
                self._limit += 1
                self._streak = 0
                if not self._slow_start:
                    log.info("concurrency limit raised to %d", self._limit)
                self._cond.notify_all()

    async def on_rate_limited(self, epoch: int, retry_after: float | None = None) -> None:
        async with self._cond:
            self.rate_limited_count += 1
            if retry_after and retry_after > 0:
                self._pause_until = max(self._pause_until, self._clock() + retry_after)
            if epoch != self._epoch:
                return  # this burst has already been counted
            self._epoch += 1
            self._slow_start = False
            self._streak = 0
            old = self._limit
            self._limit = max(self.min_limit, self._limit // 2)
            log.info(
                "429 received: concurrency limit %d -> %d%s",
                old,
                self._limit,
                f", pausing {retry_after:.1f}s (Retry-After)" if retry_after else "",
            )
            self._cond.notify_all()
