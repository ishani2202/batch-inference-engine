"""Job lifecycle: create, run in the background, track metrics, resume after a crash."""

import asyncio
import logging
import time
import uuid
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from app.config import Settings
from app.inference import AuthError, InferenceClient, ItemError
from app.rate_limiter import AdaptiveLimiter
from app.reader import InputFileError, count_items, iter_items
from app.schemas import JobStatus
from app.spaces import SpacesUploader, make_s3_client
from app.storage import DoneSet, JobStore, iter_jsonl
from app.webhook import send_webhook
from app.workers import run_pipeline

log = logging.getLogger(__name__)


class InputPathError(Exception):
    """The requested input file is outside DATA_DIR or doesn't exist."""


class InvalidItem(Exception):
    pass


def resolve_input_path(data_dir: Path, name: str) -> Path:
    """Resolve `name` inside data_dir, rejecting anything that escapes it (e.g. ../../etc/passwd)."""
    base = data_dir.resolve()
    path = (base / name).resolve()
    if not path.is_relative_to(base):
        raise InputPathError("input_file must be inside the data directory")
    if not path.is_file():
        raise InputPathError(f"input file not found: {name}")
    return path


def validate_item(item: Any, max_chars: int) -> str:
    """A valid item is an object with a non-empty string `prompt`. Returns the prompt."""
    if not isinstance(item, dict):
        raise InvalidItem(f"item must be a JSON object, got {type(item).__name__}")
    prompt = item.get("prompt")
    if not isinstance(prompt, str):
        raise InvalidItem("missing or non-string 'prompt' field")
    if not prompt.strip():
        raise InvalidItem("prompt is empty")
    if len(prompt) > max_chars:
        raise InvalidItem(f"prompt is {len(prompt)} chars, over the {max_chars} limit")
    return prompt


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Job:
    def __init__(
        self,
        job_id: str,
        input_path: Path,
        settings: Settings,
        webhook_url: str | None = None,
        s3_client_factory: Callable[[Settings], Any] = make_s3_client,
    ) -> None:
        self.id = job_id
        self.input_path = input_path
        self.settings = settings
        self.webhook_url = webhook_url
        self.store = JobStore(settings.output_dir, job_id)
        self._s3_client_factory = s3_client_factory

        self.status = JobStatus.PENDING
        self.error: str | None = None
        self.created_at = _now()
        self.started_at: str | None = None
        self.finished_at: str | None = None
        self.resumed = False

        self.total: int | None = None
        self.succeeded = 0
        self.failed = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.errors_by_type: Counter[str] = Counter()
        # Retry/429 counts from earlier runs of this job (before a restart).
        self._prior_retries = 0
        self._prior_429s = 0
        self._spaces_state: dict[str, int] = {}

        self.limiter: AdaptiveLimiter | None = None
        self.client: InferenceClient | None = None
        self.uploader: SpacesUploader | None = None
        self._run_t0: float | None = None
        self._run_t1: float | None = None
        self._processed_this_run = 0

    # ---------- status ----------

    def snapshot(self) -> dict[str, Any]:
        """What GET /job/{id}/status returns."""
        done = self.succeeded + self.failed
        elapsed = 0.0
        if self._run_t0 is not None:
            elapsed = (self._run_t1 or time.monotonic()) - self._run_t0
        retries = self._prior_retries + (self.client.retries if self.client else 0)
        rate_limited = self._prior_429s + (self.limiter.rate_limited_count if self.limiter else 0)
        s = self.settings
        cost = self.input_tokens / 1e6 * s.price_input_per_m + self.output_tokens / 1e6 * s.price_output_per_m
        return {
            "job_id": self.id,
            "status": self.status.value,
            "error": self.error,
            "input_file": self.input_path.name,
            "model": s.model,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "resumed": self.resumed,
            "progress": {
                "total": self.total,
                "done": done,
                "succeeded": self.succeeded,
                "failed": self.failed,
                "percent": round(100 * done / self.total, 1) if self.total else (100.0 if self.total == 0 else 0.0),
            },
            "performance": {
                "elapsed_seconds": round(elapsed, 2),
                "items_per_second": round(self._processed_this_run / elapsed, 2) if elapsed > 0 else 0.0,
                "retries": retries,
                "rate_limited_429s": rate_limited,
                "concurrency_limit": self.limiter.limit if self.limiter else None,
                "peak_concurrency": self.limiter.peak_in_flight if self.limiter else None,
            },
            "tokens": {"input": self.input_tokens, "output": self.output_tokens},
            "estimated_cost_usd": round(cost, 6),
            "errors_by_type": dict(self.errors_by_type),
        }

    def meta(self) -> dict[str, Any]:
        """snapshot() plus what we need to resume after a restart."""
        snap = self.snapshot()
        snap["resume"] = {
            "input_path": str(self.input_path),
            "webhook_url": self.webhook_url,
            "retries": snap["performance"]["retries"],
            "rate_limited_429s": snap["performance"]["rate_limited_429s"],
            "spaces": self.uploader.state() if self.uploader else self._spaces_state,
        }
        return snap

    def save(self) -> None:
        self.store.write_meta(self.meta())

    @classmethod
    def from_meta(cls, meta: dict[str, Any], settings: Settings, **kwargs: Any) -> "Job":
        r = meta["resume"]
        job = cls(meta["job_id"], Path(r["input_path"]), settings, r.get("webhook_url"), **kwargs)
        job.status = JobStatus(meta["status"])
        job.error = meta.get("error")
        job.created_at = meta["created_at"]
        job.started_at = meta.get("started_at")
        job.finished_at = meta.get("finished_at")
        job.resumed = meta.get("resumed", False)
        p = meta["progress"]
        job.total, job.succeeded, job.failed = p["total"], p["succeeded"], p["failed"]
        job.input_tokens = meta["tokens"]["input"]
        job.output_tokens = meta["tokens"]["output"]
        job.errors_by_type = Counter(meta.get("errors_by_type", {}))
        job._prior_retries = r.get("retries", 0)
        job._prior_429s = r.get("rate_limited_429s", 0)
        job._spaces_state = r.get("spaces") or {}
        return job

    # ---------- running ----------

    async def run(self) -> None:
        s = self.settings
        self.status = JobStatus.RUNNING
        self.started_at = self.started_at or _now()
        self._run_t0 = time.monotonic()
        self.save()
        saver = asyncio.create_task(self._save_periodically())
        cancelled = False
        try:
            self.total = await asyncio.to_thread(count_items, self.input_path)
            done = await asyncio.to_thread(self._load_progress)
            if done.count:
                self.resumed = True
                log.info("job %s: resuming, %d/%d items already done", self.id, done.count, self.total)
            log.info("job %s: %d items, model=%s", self.id, self.total, s.model)

            self.store.open()
            if s.spaces_enabled:
                self.uploader = SpacesUploader(
                    self._s3_client_factory(s),
                    s.spaces_bucket,
                    s.spaces_prefix,
                    self.id,
                    self.store,
                    s.spaces_part_size,
                    **self._spaces_state,
                )
            self.limiter = AdaptiveLimiter(s.min_concurrency, s.max_concurrency, s.start_concurrency)
            limits = httpx.Limits(max_connections=s.max_concurrency, max_keepalive_connections=s.max_concurrency)
            async with httpx.AsyncClient(timeout=s.request_timeout, limits=limits) as http:
                self.client = InferenceClient(s, http, self.limiter)
                await run_pipeline(
                    iter_items(self.input_path),
                    self._process,
                    num_workers=s.max_concurrency,
                    queue_size=s.queue_size,
                    skip=done,
                )
            self.status = JobStatus.COMPLETED
        except (AuthError, InputFileError) as exc:
            self.status, self.error = JobStatus.FAILED, str(exc)
            log.error("job %s failed: %s", self.id, exc)
        except asyncio.CancelledError:
            # Server shutting down: leave status RUNNING so the job resumes on restart.
            cancelled = True
            raise
        except Exception as exc:
            self.status, self.error = JobStatus.FAILED, f"internal error: {exc!r}"
            log.exception("job %s crashed", self.id)
        finally:
            saver.cancel()
            self.store.close()
            self._run_t1 = time.monotonic()
            if not cancelled:
                self.finished_at = _now()
            self.save()
            if not cancelled:
                await self._on_finished()

    async def _process(self, index: int, item: Any) -> None:
        """Handle one item. Per-item failures are recorded, never raised (except AuthError)."""
        item_id = item.get("id") if isinstance(item, dict) else None
        try:
            prompt = validate_item(item, self.settings.max_prompt_chars)
            result = await self.client.complete(prompt)
        except InvalidItem as exc:
            self._record_error(index, item_id, "invalid_input", str(exc), attempts=0)
        except ItemError as exc:
            self._record_error(index, item_id, exc.kind, exc.message, exc.attempts, exc.status_code)
        except AuthError:
            raise
        except Exception as exc:  # a bug in our code must not lose the item silently
            log.exception("unexpected error on item %d", index)
            self._record_error(index, item_id, "internal_error", repr(exc), attempts=0)
        else:
            self.store.append_result(
                {
                    "index": index,
                    "id": item_id,
                    "prompt": prompt,
                    "response": result.text,
                    "input_tokens": result.input_tokens,
                    "output_tokens": result.output_tokens,
                    "attempts": result.attempts,
                    "latency_ms": result.latency_ms,
                }
            )
            self.succeeded += 1
            self.input_tokens += result.input_tokens
            self.output_tokens += result.output_tokens
            self._processed_this_run += 1
            if self.uploader:
                self.uploader.on_result()

    def _record_error(
        self, index: int, item_id: Any, kind: str, message: str, attempts: int, status_code: int | None = None
    ) -> None:
        self.store.append_error(
            {
                "index": index,
                "id": item_id,
                "error_type": kind,
                "message": message,
                "status_code": status_code,
                "attempts": attempts,
            }
        )
        self.failed += 1
        self.errors_by_type[kind] += 1
        self._processed_this_run += 1
        log.info("item %d failed (%s): %s", index, kind, message)

    def _load_progress(self) -> DoneSet:
        """Rebuild counts from the JSONL files (the source of truth) and return finished indexes."""
        done = DoneSet(self.total or 0)
        self.succeeded = self.failed = self.input_tokens = self.output_tokens = 0
        self.errors_by_type = Counter()
        for rec in iter_jsonl(self.store.results_path):
            if done.add(rec.get("index", -1)):
                self.succeeded += 1
                self.input_tokens += rec.get("input_tokens", 0)
                self.output_tokens += rec.get("output_tokens", 0)
        for rec in iter_jsonl(self.store.errors_path):
            if done.add(rec.get("index", -1)):
                self.failed += 1
                self.errors_by_type[rec.get("error_type", "unknown")] += 1
        return done

    async def _save_periodically(self) -> None:
        while True:
            await asyncio.sleep(self.settings.meta_flush_interval)
            self.save()

    async def _on_finished(self) -> None:
        summary = self.snapshot()
        log.info(
            "job %s %s: %d succeeded, %d failed, %d retries, %d x 429 in %.1fs",
            self.id,
            self.status.value,
            self.succeeded,
            self.failed,
            summary["performance"]["retries"],
            summary["performance"]["rate_limited_429s"],
            summary["performance"]["elapsed_seconds"],
        )
        if self.uploader:
            await self.uploader.finalize(self.meta())
            self.save()  # persist final Spaces offsets
        if self.webhook_url:
            await send_webhook(
                self.webhook_url,
                {**summary, "download_url": f"/job/{self.id}/download"},
                retries=self.settings.webhook_retries,
                timeout=self.settings.webhook_timeout,
                backoff_base=self.settings.backoff_base,
            )


class JobManager:
    def __init__(self, settings: Settings, s3_client_factory: Callable[[Settings], Any] = make_s3_client) -> None:
        self.settings = settings
        self.jobs: dict[str, Job] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._s3_client_factory = s3_client_factory

    def create(self, input_file: str, webhook_url: str | None = None) -> Job:
        path = resolve_input_path(self.settings.data_dir, input_file)
        job = Job(uuid.uuid4().hex, path, self.settings, webhook_url, self._s3_client_factory)
        job.save()
        self.jobs[job.id] = job
        self._start(job)
        return job

    def get(self, job_id: str) -> Job | None:
        return self.jobs.get(job_id)

    def recover(self) -> int:
        """Load every job from disk; restart the ones that were still running. Returns #resumed."""
        resumed = 0
        root = self.settings.output_dir
        if not root.exists():
            return 0
        for meta_path in root.glob("*/meta.json"):
            try:
                meta = JobStore(root, meta_path.parent.name).read_meta()
                job = Job.from_meta(meta, self.settings, s3_client_factory=self._s3_client_factory)
            except Exception:
                log.exception("skipping unreadable job at %s", meta_path.parent)
                continue
            self.jobs[job.id] = job
            if not job.status.finished:
                log.info("resuming job %s after restart", job.id)
                self._start(job)
                resumed += 1
        return resumed

    def _start(self, job: Job) -> None:
        task = asyncio.create_task(job.run(), name=f"job-{job.id}")
        self._tasks[job.id] = task
        task.add_done_callback(lambda _: self._tasks.pop(job.id, None))

    async def wait(self, job_id: str) -> None:
        """Wait for a job's background task (used by tests)."""
        task = self._tasks.get(job_id)
        if task:
            await task

    async def shutdown(self) -> None:
        """Cancel running jobs; they stay 'running' on disk and resume on next start."""
        tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
