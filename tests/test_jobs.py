"""Job-level tests: the whole pipeline with a mocked inference API."""

import asyncio
import json

import httpx
import pytest
import respx

from app.jobs import InputPathError, JobManager, resolve_input_path
from app.schemas import JobStatus
from app.spaces import SpacesUploader
from app.storage import JobStore
from tests.conftest import CHAT_URL, ok, prompts, write_batch

BAD_ITEMS = [None, 42, "a string", {"prompt": ""}, {"prompt": "   "}, {"id": "x"}, {"prompt": ["list"]}]


async def run_job(settings, items, **kwargs):
    write_batch(settings, items)
    manager = JobManager(settings, **kwargs)
    job = manager.create("batch.json")
    await manager.wait(job.id)
    return job


@respx.mock
async def test_all_items_accounted_for(settings):
    respx.post(CHAT_URL).mock(return_value=ok())
    items = prompts(50) + BAD_ITEMS
    job = await run_job(settings, items)

    assert job.status == JobStatus.COMPLETED
    assert job.total == 57
    assert job.succeeded == 50
    assert job.failed == 7
    results = list(job.store.iter_results())
    errors = list(job.store.iter_errors())
    assert sorted(r["index"] for r in results + errors) == list(range(57))  # nothing lost, nothing doubled
    assert {r["id"] for r in results} == {f"p{i}" for i in range(50)}
    snap = job.snapshot()
    assert snap["tokens"] == {"input": 500, "output": 250}
    assert snap["estimated_cost_usd"] == pytest.approx(500 / 1e6 * 1.0 + 250 / 1e6 * 2.0)
    assert snap["progress"]["percent"] == 100.0


@respx.mock
async def test_bad_inputs_never_call_the_api(settings):
    route = respx.post(CHAT_URL).mock(return_value=ok())
    job = await run_job(settings, BAD_ITEMS)
    assert route.call_count == 0
    assert job.failed == len(BAD_ITEMS)
    assert job.errors_by_type == {"invalid_input": len(BAD_ITEMS)}


@respx.mock
async def test_oversized_prompt_rejected_locally(settings):
    route = respx.post(CHAT_URL).mock(return_value=ok())
    job = await run_job(settings, [{"prompt": "x" * (settings.max_prompt_chars + 1)}])
    assert route.call_count == 0
    assert "over the" in next(job.store.iter_errors())["message"]


@respx.mock
async def test_persistent_failures_are_isolated(settings):
    """Items whose API calls keep failing become errors; the others still succeed."""

    def respond(request):
        prompt = json.loads(request.content)["messages"][0]["content"]
        if prompt.endswith(("3", "7")):
            return httpx.Response(500)
        if prompt == "question 5":
            return httpx.Response(400, json={"error": "bad"})
        return ok()

    respx.post(CHAT_URL).mock(side_effect=respond)
    job = await run_job(settings, prompts(20))
    assert job.status == JobStatus.COMPLETED
    assert job.failed == 5  # 3, 7, 13, 17 (500s) + 5 (400)
    assert job.succeeded == 15
    assert job.errors_by_type == {"retries_exhausted": 4, "client_error": 1}


@respx.mock
async def test_rate_limit_storm_loses_nothing(settings):
    """Every other call is a 429: all items still complete and the limiter reacts."""
    calls = 0

    def respond(request):
        nonlocal calls
        calls += 1
        return httpx.Response(429) if calls % 2 else ok()

    respx.post(CHAT_URL).mock(side_effect=respond)
    job = await run_job(settings, prompts(40))
    snap = job.snapshot()
    assert job.succeeded == 40 and job.failed == 0
    assert snap["performance"]["rate_limited_429s"] >= 40
    assert snap["performance"]["retries"] >= 40


@respx.mock
async def test_every_request_holds_a_slot_and_pool_is_bounded(settings):
    active = calls = 0
    mismatches = []
    holder = {}

    async def respond(request):
        nonlocal active, calls
        active += 1
        calls += 1
        if active != holder["job"].limiter.in_flight:
            mismatches.append(active)
        await asyncio.sleep(0.005)
        active -= 1
        return httpx.Response(429) if calls % 5 == 0 else ok()

    respx.post(CHAT_URL).mock(side_effect=respond)
    write_batch(settings, prompts(100))
    manager = JobManager(settings)
    job = manager.create("batch.json")
    holder["job"] = job
    await manager.wait(job.id)
    assert job.succeeded == 100
    assert mismatches == []  # no request ever ran without a controller slot
    assert job.limiter.peak_in_flight <= settings.max_concurrency
    assert job.limiter.rate_limited_count > 0


@respx.mock
async def test_bad_api_key_stops_job_early(settings):
    route = respx.post(CHAT_URL).mock(return_value=httpx.Response(401, json={"error": "unauthorized"}))
    job = await run_job(settings, prompts(500))
    assert job.status == JobStatus.FAILED
    assert "MODEL_ACCESS_KEY" in job.error
    assert route.call_count <= settings.max_concurrency  # not 500 wasted calls


@respx.mock
async def test_missing_api_key_fails_job_immediately(settings):
    route = respx.post(CHAT_URL).mock(return_value=ok())
    settings.model_access_key = ""
    job = await run_job(settings, prompts(5))
    assert job.status == JobStatus.FAILED
    assert job.error == "MODEL_ACCESS_KEY is not set"
    assert route.call_count == 0


@respx.mock
async def test_corrupt_file_fails_before_any_api_call(settings):
    route = respx.post(CHAT_URL).mock(return_value=ok())
    (settings.data_dir / "batch.json").write_text('[{"prompt": "a"}, {"prompt": "b"')
    manager = JobManager(settings)
    job = manager.create("batch.json")
    await manager.wait(job.id)
    assert job.status == JobStatus.FAILED
    assert "invalid JSON" in job.error
    assert route.call_count == 0


@respx.mock
async def test_meta_json_reflects_final_state(settings):
    respx.post(CHAT_URL).mock(return_value=ok())
    job = await run_job(settings, prompts(5))
    meta = job.store.read_meta()
    assert meta["status"] == "completed"
    assert meta["progress"]["succeeded"] == 5
    assert meta["resume"]["input_path"].endswith("batch.json")


# ---------- crash recovery ----------


@respx.mock
async def test_resume_after_crash_skips_finished_items(settings):
    route = respx.post(CHAT_URL).mock(return_value=ok())
    write_batch(settings, prompts(10))

    # Simulate a crash: job was 'running', 4 items were written, the 5th line is torn.
    first = JobManager(settings)
    job = first.create("batch.json")
    await first.shutdown()  # cancel before it does anything (like kill -9 at startup)
    job.store.results_path.write_text(
        "".join(json.dumps({"index": i, "id": f"p{i}", "input_tokens": 10, "output_tokens": 5}) + "\n" for i in (0, 2))
        + json.dumps({"index": 3, "error_type": "invalid_input"})
        + "\n"
        + '{"index": 9, "id": "p9", "resp'
    )
    job.store.errors_path.write_text(json.dumps({"index": 5, "error_type": "client_error"}) + "\n")
    assert job.store.read_meta()["status"] in ("pending", "running")  # unfinished on disk
    route.reset()

    second = JobManager(settings)
    assert second.recover() == 1
    await second.wait(job.id)
    resumed = second.get(job.id)

    assert resumed.status == JobStatus.COMPLETED
    assert resumed.resumed
    assert route.call_count == 6  # 10 minus indexes 0, 2, 3, 5
    indexes = sorted(r["index"] for r in list(resumed.store.iter_results()) + list(resumed.store.iter_errors()))
    assert indexes == list(range(10))


@respx.mock
async def test_finished_jobs_are_reloaded_not_rerun(settings):
    route = respx.post(CHAT_URL).mock(return_value=ok())
    job = await run_job(settings, prompts(3))
    route.reset()
    manager = JobManager(settings)
    assert manager.recover() == 0
    reloaded = manager.get(job.id)
    assert reloaded.status == JobStatus.COMPLETED and reloaded.succeeded == 3
    assert reloaded.snapshot()["performance"] == job.snapshot()["performance"]
    assert route.call_count == 0


# ---------- extensions ----------


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body


@respx.mock
async def test_job_uploads_results_and_final_files_to_spaces(settings):
    respx.post(CHAT_URL).mock(return_value=ok())
    settings.spaces_bucket, settings.spaces_key, settings.spaces_secret = "bucket", "k", "s"
    settings.spaces_part_size = 3
    s3 = FakeS3()
    job = await run_job(settings, prompts(10) + [None], s3_client_factory=lambda _: s3)

    parts = sorted(k for k in s3.objects if "/results/part-" in k)
    assert parts
    assert b"".join(s3.objects[k] for k in parts) == job.store.results_path.read_bytes()
    base = f"batch-jobs/{job.id}"
    assert json.loads(s3.objects[f"{base}/meta.json"])["status"] == "completed"
    assert s3.objects[f"{base}/errors.jsonl"] == job.store.errors_path.read_bytes()


async def test_uploader_sends_only_new_bytes_as_numbered_parts(tmp_path):
    store = JobStore(tmp_path, "j")
    store.open()
    s3 = FakeS3()
    up = SpacesUploader(s3, "bucket", "pre", "j", store, part_size=2)
    for i in range(3):
        store.append_result({"index": i})
    await up.flush_part()
    store.append_result({"index": 3})
    await up.flush_part()
    await up.flush_part()  # nothing new -> no empty part
    store.close()
    assert sorted(s3.objects) == ["pre/j/results/part-00001.jsonl", "pre/j/results/part-00002.jsonl"]
    assert s3.objects["pre/j/results/part-00001.jsonl"].count(b"\n") == 3
    assert s3.objects["pre/j/results/part-00002.jsonl"] == b'{"index": 3}\n'
    assert up.state() == {"uploaded_offset": store.results_size(), "next_part": 3}


@respx.mock
async def test_spaces_failure_does_not_fail_job(settings):
    respx.post(CHAT_URL).mock(return_value=ok())
    settings.spaces_bucket, settings.spaces_key, settings.spaces_secret = "bucket", "k", "s"
    settings.spaces_part_size = 2

    class BrokenS3:
        def put_object(self, **_):
            raise ConnectionError("spaces down")

    job = await run_job(settings, prompts(5), s3_client_factory=lambda _: BrokenS3())
    assert job.status == JobStatus.COMPLETED and job.succeeded == 5


@respx.mock
async def test_webhook_called_on_completion(settings):
    respx.post(CHAT_URL).mock(return_value=ok())
    hook = respx.post("https://hooks.test/done").mock(side_effect=[httpx.Response(503), httpx.Response(200)])
    write_batch(settings, prompts(3))
    manager = JobManager(settings)
    job = manager.create("batch.json", webhook_url="https://hooks.test/done")
    await manager.wait(job.id)

    assert hook.call_count == 2  # retried once after the 503
    payload = json.loads(hook.calls[-1].request.content)
    assert payload["job_id"] == job.id
    assert payload["status"] == "completed"
    assert payload["progress"]["succeeded"] == 3
    assert payload["download_url"] == f"/job/{job.id}/download"


@respx.mock
async def test_webhook_failure_does_not_change_job_result(settings):
    respx.post(CHAT_URL).mock(return_value=ok())
    respx.post("https://hooks.test/done").mock(return_value=httpx.Response(500))
    write_batch(settings, prompts(2))
    manager = JobManager(settings)
    job = manager.create("batch.json", webhook_url="https://hooks.test/done")
    await manager.wait(job.id)
    assert job.status == JobStatus.COMPLETED


# ---------- input path safety ----------


def test_path_traversal_rejected(settings):
    (settings.data_dir.parent / "secret.json").write_text("[]")
    for name in ["../secret.json", "/etc/passwd", "../../../../etc/passwd"]:
        with pytest.raises(InputPathError):
            resolve_input_path(settings.data_dir, name)


def test_missing_input_rejected(settings):
    with pytest.raises(InputPathError):
        resolve_input_path(settings.data_dir, "nope.json")
