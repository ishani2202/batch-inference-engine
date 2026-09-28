"""Completion webhook: POST a job summary to the caller's URL when a job finishes.

Delivery is best effort with a few retries. A webhook failure is logged but never
changes the job's result: the work is done and downloadable either way.
"""

import asyncio
import logging
from typing import Any

import httpx

from app.inference import backoff_delay

log = logging.getLogger(__name__)


async def send_webhook(
    url: str,
    payload: dict[str, Any],
    retries: int = 3,
    timeout: float = 10.0,
    backoff_base: float = 1.0,
) -> bool:
    """Returns True if the receiver answered 2xx."""
    async with httpx.AsyncClient(timeout=timeout) as http:
        for attempt in range(retries + 1):
            if attempt:
                await asyncio.sleep(backoff_delay(attempt - 1, backoff_base, 30.0))
            try:
                resp = await http.post(url, json=payload)
                if resp.is_success:
                    log.info("webhook delivered to %s", url)
                    return True
                # A 4xx other than 408/429 means the receiver rejected it; retrying won't help.
                if 400 <= resp.status_code < 500 and resp.status_code not in (408, 429):
                    log.warning("webhook rejected by %s: HTTP %d", url, resp.status_code)
                    return False
                log.warning("webhook attempt %d got HTTP %d", attempt + 1, resp.status_code)
            except httpx.HTTPError as exc:
                log.warning("webhook attempt %d failed: %s", attempt + 1, exc)
    log.error("webhook to %s failed after %d attempts", url, retries + 1)
    return False
