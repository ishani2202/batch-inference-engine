"""End-to-end tests through the HTTP API, with the inference API mocked."""

import asyncio
import json
import time

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from app.main import create_app
from tests.conftest import CHAT_URL, ok, prompts, write_batch


@pytest.fixture
def api(settings):
    with respx.mock(assert_all_called=False) as mock:
        with TestClient(create_app(settings)) as client:
            client.mock = mock
            yield client


def wait_until_finished(api, job_id, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = api.get(f"/job/{job_id}/status").json()
        if body["status"] in ("completed", "failed"):
            return body
        time.sleep(0.02)
    raise AssertionError("job did not finish in time")


def test_full_flow(api, settings):
    api.mock.post(CHAT_URL).mock(return_value=ok("answer"))
    write_batch(settings, prompts(30) + [None, {"prompt": ""}], name="sample_batch.json")

    created = api.post("/job", json={})
    assert created.status_code == 202
    job_id = created.json()["job_id"]
    assert created.json()["status_url"] == f"/job/{job_id}/status"

    status = wait_until_finished(api, job_id)
    assert status["status"] == "completed"
    assert status["progress"] == {"total": 32, "done": 32, "succeeded": 30, "failed": 2, "percent": 100.0}

    download = api.get(f"/job/{job_id}/download")
    assert download.status_code == 200
    results = download.json()
    assert len(results) == 30
    assert all(r["response"] == "answer" for r in results)
    assert sorted(r["index"] for r in results) == list(range(30))

    errors = api.get(f"/job/{job_id}/download", params={"kind": "errors"}).json()
    assert sorted(e["index"] for e in errors) == [30, 31]
    assert all(e["error_type"] == "invalid_input" for e in errors)


def test_create_returns_before_work_finishes(api, settings):
    async def slow(request):
        await asyncio.sleep(0.3)
        return ok()

    api.mock.post(CHAT_URL).mock(side_effect=slow)
    write_batch(settings, prompts(5))
    t0 = time.monotonic()
    job_id = api.post("/job", json={"input_file": "batch.json"}).json()["job_id"]
    assert time.monotonic() - t0 < 0.25  # job ID came back before any inference finished

    # Download while running -> 409
    resp = api.get(f"/job/{job_id}/download")
    assert resp.status_code == 409
    assert wait_until_finished(api, job_id)["status"] == "completed"


def test_unknown_job_is_404(api):
    assert api.get("/job/nope/status").status_code == 404
    assert api.get("/job/nope/download").status_code == 404


@pytest.mark.parametrize("name", ["../../etc/passwd", "missing.json"])
def test_bad_input_file_is_400(api, name):
    resp = api.post("/job", json={"input_file": name})
    assert resp.status_code == 400


def test_invalid_webhook_url_is_422(api):
    assert api.post("/job", json={"webhook_url": "not a url"}).status_code == 422


def test_webhook_via_api(api, settings):
    api.mock.post(CHAT_URL).mock(return_value=ok())
    hook = api.mock.post("https://hooks.test/cb").mock(return_value=httpx.Response(204))
    write_batch(settings, prompts(3))
    job_id = api.post("/job", json={"input_file": "batch.json", "webhook_url": "https://hooks.test/cb"}).json()[
        "job_id"
    ]
    wait_until_finished(api, job_id)
    deadline = time.monotonic() + 5
    while hook.call_count == 0 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert json.loads(hook.calls[0].request.content)["status"] == "completed"


def test_failed_job_is_downloadable(api, settings):
    api.mock.post(CHAT_URL).mock(return_value=httpx.Response(401))
    write_batch(settings, prompts(3))
    job_id = api.post("/job", json={"input_file": "batch.json"}).json()["job_id"]
    status = wait_until_finished(api, job_id)
    assert status["status"] == "failed"
    assert "MODEL_ACCESS_KEY" in status["error"]
    assert api.get(f"/job/{job_id}/download").json() == []


def test_health(api):
    assert api.get("/health").json() == {"status": "ok"}
