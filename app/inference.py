"""Client for an OpenAI-compatible chat completions endpoint, with retries.

Retry policy:
* Retry: 429, 408, 5xx, timeouts, connection errors, and malformed 200 bodies.
* Fail the item immediately: every other 4xx (retrying a bad request won't fix it).
* Stop the whole job: 401/403 or a malformed request (bad URL): every item would fail.
* 429s and errors have separate retry budgets (MAX_RATE_LIMIT_RETRIES vs
  MAX_RETRIES): a 429 means "slow down", not "this item is broken".
* Backoff: "full jitter", a random wait in [0, min(cap, base * 2^attempt)], so
  retries from many workers spread out instead of colliding.
* Retry-After is handed to the shared controller, which pauses *every* worker
  until then. We deliberately don't make each worker sleep exactly Retry-After:
  they would all wake at the same instant and collide again.

A worker holds a controller slot only while its request is in flight; it sleeps
between retries without one, so backing off never blocks other workers.
"""

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from app.config import Settings
from app.rate_limiter import AdaptiveLimiter

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


class FatalError(Exception):
    """Every item would fail the same way, so stop the whole job instead of retrying."""


class AuthError(FatalError):
    """The API rejected our credentials (401/403)."""


class ConfigError(FatalError):
    """Our own request is malformed (bad INFERENCE_URL, invalid header), so no retry can help."""


class ItemError(Exception):
    """An item failed for good. `kind` becomes the error_type in errors.jsonl."""

    def __init__(self, kind: str, message: str, attempts: int, status_code: int | None = None):
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.attempts = attempts
        self.status_code = status_code


@dataclass
class Completion:
    text: str
    input_tokens: int
    output_tokens: int
    attempts: int
    latency_ms: int


class _Retryable(Exception):
    def __init__(self, reason: str, status_code: int | None = None):
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code


def parse_retry_after(value: str | None, cap: float) -> float | None:
    """Retry-After may be seconds ("5") or an HTTP date. Returns seconds, capped."""
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        seconds = (when - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, min(seconds, cap))


def backoff_delay(attempt: int, base: float, cap: float, rng: Callable[[], float] = random.random) -> float:
    """Full-jitter exponential backoff for the given retry number (0-based)."""
    return rng() * min(cap, base * (2**attempt))


class InferenceClient:
    def __init__(
        self,
        settings: Settings,
        http: httpx.AsyncClient,
        limiter: AdaptiveLimiter,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.settings = settings
        self.http = http
        self.limiter = limiter
        self._sleep = sleep
        self._url = settings.inference_url.rstrip("/") + "/chat/completions"
        self._headers = {"Authorization": f"Bearer {settings.model_access_key}"}
        self.retries = 0  # total retries across all items, for metrics

    async def complete(self, prompt: str) -> Completion:
        s = self.settings
        payload = {
            "model": s.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": s.max_tokens,
            "temperature": s.temperature,
        }
        # Two separate budgets: a 429 says "slow down", not "this item is broken",
        # so it gets a much larger budget than real errors (5xx, timeouts).
        error_retries = rate_limit_retries = 0
        attempt = 0
        while True:
            attempt += 1
            try:
                return await self._attempt(payload, attempt)
            except _Retryable as exc:
                if exc.status_code == 429:
                    rate_limit_retries += 1
                    exhausted = rate_limit_retries > s.max_rate_limit_retries
                else:
                    error_retries += 1
                    exhausted = error_retries > s.max_retries
                if exhausted:
                    raise ItemError(
                        "retries_exhausted",
                        f"gave up after {attempt} attempts; last error: {exc.reason}",
                        attempts=attempt,
                        status_code=exc.status_code,
                    ) from None
                self.retries += 1
                delay = backoff_delay(attempt - 1, s.backoff_base, s.backoff_cap)
                log.info("retry #%d in %.2fs (%s)", attempt, delay, exc.reason)
                await self._sleep(delay)

    async def _attempt(self, payload: dict, attempt: int) -> Completion:
        async with self.limiter.slot() as epoch:
            start = time.perf_counter()
            try:
                resp = await self.http.post(self._url, json=payload, headers=self._headers)
            except httpx.TimeoutException:
                raise _Retryable("timeout") from None
            except (httpx.UnsupportedProtocol, httpx.LocalProtocolError) as exc:
                raise ConfigError(f"cannot send request to {self._url!r}: check INFERENCE_URL ({exc})") from None
            except httpx.TransportError as exc:
                raise _Retryable(f"connection error: {type(exc).__name__}") from None
        latency_ms = int((time.perf_counter() - start) * 1000)
        status = resp.status_code

        if status == 429:
            retry_after = parse_retry_after(resp.headers.get("retry-after"), self.settings.max_retry_after)
            await self.limiter.on_rate_limited(epoch, retry_after)
            raise _Retryable("429 rate limited", 429)
        if status in (401, 403):
            raise AuthError(f"inference API returned {status}: check MODEL_ACCESS_KEY ({_snippet(resp)})")
        if status in RETRYABLE_STATUS or status >= 500:
            raise _Retryable(f"HTTP {status}", status)
        if status >= 400:
            raise ItemError("client_error", f"HTTP {status}: {_snippet(resp)}", attempt, status)

        try:
            body = resp.json()
            text = body["choices"][0]["message"]["content"]
            usage = body.get("usage") or {}
        except (ValueError, KeyError, IndexError, TypeError):
            raise _Retryable("malformed response body", status) from None
        await self.limiter.on_success()
        return Completion(
            text=text or "",
            input_tokens=int(usage.get("prompt_tokens") or 0),
            output_tokens=int(usage.get("completion_tokens") or 0),
            attempts=attempt,
            latency_ms=latency_ms,
        )


def _snippet(resp: httpx.Response, limit: int = 300) -> str:
    return resp.text[:limit].replace("\n", " ")
