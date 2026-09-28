"""HTTP API: create a job, poll its status, download its results.

Run with:  uvicorn app.main:app
"""

import json
import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

from app.config import Settings, get_settings
from app.jobs import InputPathError, Job, JobManager
from app.schemas import CreateJobRequest, CreateJobResponse, JobStatus
from app.storage import iter_jsonl

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is too noisy
log = logging.getLogger("app")


def create_app(settings: Settings | None = None, manager: JobManager | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.manager = manager or JobManager(settings)
        resumed = app.state.manager.recover()
        if resumed:
            log.info("resumed %d unfinished job(s) from disk", resumed)
        if not settings.model_access_key:
            log.warning("MODEL_ACCESS_KEY is not set; jobs will fail with an auth error")
        yield
        await app.state.manager.shutdown()

    app = FastAPI(title="Batch Inference Engine", version="1.0.0", lifespan=lifespan)

    def get_job(request: Request, job_id: str) -> Job:
        job = request.app.state.manager.get(job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"job {job_id} not found")
        return job

    @app.post("/job", status_code=status.HTTP_202_ACCEPTED, response_model=CreateJobResponse)
    async def create_job(request: Request, body: CreateJobRequest | None = None) -> CreateJobResponse:
        """Start a batch job in the background and return its ID immediately."""
        body = body or CreateJobRequest()
        try:
            job = request.app.state.manager.create(
                body.input_file, str(body.webhook_url) if body.webhook_url else None
            )
        except InputPathError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from None
        return CreateJobResponse(
            job_id=job.id,
            status=job.status,
            status_url=f"/job/{job.id}/status",
            download_url=f"/job/{job.id}/download",
        )

    @app.get("/job/{job_id}/status")
    async def job_status(job_id: str, request: Request) -> dict:
        """Progress, failures, retries, 429s, throughput, tokens and cost."""
        return get_job(request, job_id).snapshot()

    @app.get("/job/{job_id}/download")
    async def download(
        job_id: str,
        request: Request,
        kind: Literal["results", "errors"] = Query("results", description="results (default) or errors"),
    ) -> StreamingResponse:
        """Stream the job's results (or errors) as one JSON array.

        Records are in completion order; each carries its input `index` so the
        client can sort. Sorting server-side would mean loading everything into
        memory, which is exactly what this design avoids.
        """
        job = get_job(request, job_id)
        if not job.status.finished:
            raise HTTPException(status.HTTP_409_CONFLICT, f"job is {job.status.value}; download when it finishes")
        path = job.store.results_path if kind == "results" else job.store.errors_path
        return StreamingResponse(
            _stream_json_array(path),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{job_id}-{kind}.json"'},
        )

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    return app


def _stream_json_array(path) -> Iterator[str]:
    """Build a JSON array one line at a time, so memory stays flat for any size."""
    yield "["
    first = True
    for record in iter_jsonl(path):
        yield ("" if first else ",") + json.dumps(record, ensure_ascii=False)
        first = False
    yield "]"


app = create_app()
