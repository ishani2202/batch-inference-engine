"""Request/response models and the job status enum."""

from enum import StrEnum

from pydantic import BaseModel, Field, HttpUrl


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"  # finished; individual items may still have failed
    FAILED = "failed"  # the job itself could not run (bad file, bad API key, ...)

    @property
    def finished(self) -> bool:
        return self in (JobStatus.COMPLETED, JobStatus.FAILED)


class CreateJobRequest(BaseModel):
    input_file: str = Field(
        default="sample_batch.json",
        description="File name inside DATA_DIR containing a JSON array of prompt items.",
    )
    webhook_url: HttpUrl | None = Field(
        default=None, description="Optional URL that receives a POST with the job summary when it finishes."
    )


class CreateJobResponse(BaseModel):
    job_id: str
    status: JobStatus
    status_url: str
    download_url: str
