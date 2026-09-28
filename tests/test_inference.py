import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest
import respx

from app.inference import AuthError, BillingError, ConfigError, InferenceClient, ItemError, backoff_delay, parse_retry_after
from app.rate_limiter import AdaptiveLimiter
from tests.conftest import CHAT_URL, ok


@pytest.fixture
async def client(settings):
    limiter = AdaptiveLimiter(1, 8, 4)
    async with httpx.AsyncClient() as http:
        yield InferenceClient(settings, http, limiter)


@respx.mock
async def test_success_first_try(client):
    route = respx.post(CHAT_URL).mock(return_value=ok("hello", 7, 3))
    result = await client.complete("hi")
    assert (result.text, result.input_tokens, result.output_tokens, result.attempts) == ("hello", 7, 3, 1)
    sent = route.calls[0].request
    assert sent.headers["authorization"] == "Bearer test-key"
    body = sent.read().decode()
    assert '"model":"test-model"' in body.replace(" ", "")


@respx.mock
async def test_429_then_success(client):
    respx.post(CHAT_URL).mock(side_effect=[httpx.Response(429), ok()])
    result = await client.complete("hi")
    assert result.attempts == 2
    assert client.retries == 1
    assert client.limiter.rate_limited_count == 1
    assert client.limiter.limit == 2  # halved from 4


@respx.mock
async def test_multiple_429s_then_success(client):
    respx.post(CHAT_URL).mock(side_effect=[httpx.Response(429)] * 3 + [ok()])
    result = await client.complete("hi")
    assert result.attempts == 4
    assert client.limiter.rate_limited_count == 3


@respx.mock
async def test_429s_have_their_own_larger_budget(client):
    """10 x 429 exceeds max_retries (3) but not max_rate_limit_retries: the item must not be dropped."""
    respx.post(CHAT_URL).mock(side_effect=[httpx.Response(429)] * 10 + [ok()])
    assert (await client.complete("hi")).attempts == 11


@respx.mock
async def test_endless_429s_eventually_give_up(client, settings):
    respx.post(CHAT_URL).mock(return_value=httpx.Response(429))
    with pytest.raises(ItemError) as info:
        await client.complete("hi")
    assert info.value.attempts == settings.max_rate_limit_retries + 1
    assert info.value.status_code == 429


@respx.mock
async def test_retry_after_is_respected(client):
    respx.post(CHAT_URL).mock(side_effect=[httpx.Response(429, headers={"Retry-After": "0.3"}), ok()])
    t0 = time.monotonic()
    await client.complete("hi")
    assert time.monotonic() - t0 >= 0.28


@respx.mock
async def test_500_then_success(client):
    respx.post(CHAT_URL).mock(side_effect=[httpx.Response(500), httpx.Response(503), ok()])
    assert (await client.complete("hi")).attempts == 3


@respx.mock
async def test_persistent_500_gives_up_after_max_retries(client):
    route = respx.post(CHAT_URL).mock(return_value=httpx.Response(500))
    with pytest.raises(ItemError) as info:
        await client.complete("hi")
    assert info.value.kind == "retries_exhausted"
    assert info.value.attempts == 4  # 1 try + max_retries (3)
    assert info.value.status_code == 500
    assert route.call_count == 4


@respx.mock
async def test_timeout_is_retried(client):
    respx.post(CHAT_URL).mock(side_effect=[httpx.ReadTimeout("slow"), ok()])
    assert (await client.complete("hi")).attempts == 2


@respx.mock
async def test_connection_error_is_retried(client):
    respx.post(CHAT_URL).mock(side_effect=[httpx.ConnectError("refused"), ok()])
    assert (await client.complete("hi")).attempts == 2


@respx.mock
async def test_malformed_body_is_retried(client):
    respx.post(CHAT_URL).mock(side_effect=[httpx.Response(200, json={"oops": 1}), ok()])
    assert (await client.complete("hi")).attempts == 2


@respx.mock
async def test_400_is_not_retried(client):
    route = respx.post(CHAT_URL).mock(return_value=httpx.Response(400, json={"error": "context too long"}))
    with pytest.raises(ItemError) as info:
        await client.complete("hi")
    assert info.value.kind == "client_error"
    assert info.value.status_code == 400
    assert "context too long" in info.value.message
    assert route.call_count == 1


@pytest.mark.parametrize("status", [401, 403])
@respx.mock
async def test_auth_errors_stop_immediately(client, status):
    route = respx.post(CHAT_URL).mock(return_value=httpx.Response(status))
    with pytest.raises(AuthError):
        await client.complete("hi")
    assert route.call_count == 1


@respx.mock
async def test_402_payment_required_stops_immediately(client):
    """A billing problem is account-wide: every item would get the same 402."""
    route = respx.post(CHAT_URL).mock(
        return_value=httpx.Response(402, json={"id": "Payment Required", "message": "You are not allowed to perform this operation"})
    )
    with pytest.raises(BillingError) as info:
        await client.complete("hi")
    assert "billing" in str(info.value)
    assert route.call_count == 1


async def test_malformed_url_stops_instead_of_retrying(settings):
    settings.inference_url = "ftp://not-http/v1"
    async with httpx.AsyncClient() as http:
        client = InferenceClient(settings, http, AdaptiveLimiter(1, 8, 4))
        with pytest.raises(ConfigError):
            await client.complete("hi")
    assert client.retries == 0


@respx.mock
async def test_slot_is_released_while_backing_off(settings):
    """A worker sleeping between retries must not hold a concurrency slot."""
    limiter = AdaptiveLimiter(1, 8, 4)
    in_flight_during_sleep = []

    async def fake_sleep(_: float) -> None:
        in_flight_during_sleep.append(limiter.in_flight)

    respx.post(CHAT_URL).mock(side_effect=[httpx.Response(500), httpx.Response(429), ok()])
    async with httpx.AsyncClient() as http:
        await InferenceClient(settings, http, limiter, sleep=fake_sleep).complete("hi")
    assert in_flight_during_sleep == [0, 0]


def test_backoff_is_full_jitter_and_capped():
    assert backoff_delay(0, 1.0, 30.0, rng=lambda: 1.0) == 1.0
    assert backoff_delay(3, 1.0, 30.0, rng=lambda: 1.0) == 8.0
    assert backoff_delay(10, 1.0, 30.0, rng=lambda: 1.0) == 30.0  # capped
    assert backoff_delay(5, 1.0, 30.0, rng=lambda: 0.0) == 0.0  # jitter can pick zero
    assert backoff_delay(3, 1.0, 30.0, rng=lambda: 0.5) == 4.0


def test_parse_retry_after():
    assert parse_retry_after("5", 60) == 5.0
    assert parse_retry_after("1.5", 60) == 1.5
    assert parse_retry_after("9999", 60) == 60  # capped
    assert parse_retry_after(None, 60) is None
    assert parse_retry_after("garbage", 60) is None
    future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=10), usegmt=True)
    assert 8 <= parse_retry_after(future, 60) <= 10
