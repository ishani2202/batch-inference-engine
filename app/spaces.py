"""Progressive upload of results to a DigitalOcean Spaces bucket (S3-compatible).

S3 objects can't be appended to, so we upload results.jsonl in numbered parts:
every `part_size` new results, the bytes written since the last upload become
`results/part-00001.jsonl`, `part-00002.jsonl`, ... If the machine dies, all
completed parts are safe in the bucket. Concatenating the parts in order
reproduces results.jsonl exactly (stale parts left by a crash are removed at the
end, see _delete_stale_parts).

boto3 is synchronous, so every call runs in a worker thread (asyncio.to_thread)
to keep the event loop free. Upload failures are logged and retried on the next
part; they never fail the job.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from app.config import Settings
from app.storage import JobStore

log = logging.getLogger(__name__)


def make_s3_client(settings: Settings) -> Any:
    import boto3  # imported lazily: only needed when Spaces is configured

    endpoint = settings.spaces_endpoint or f"https://{settings.spaces_region}.digitaloceanspaces.com"
    return boto3.client(
        "s3",
        region_name=settings.spaces_region,
        endpoint_url=endpoint,
        aws_access_key_id=settings.spaces_key,
        aws_secret_access_key=settings.spaces_secret,
    )


class SpacesUploader:
    def __init__(
        self,
        client: Any,
        bucket: str,
        prefix: str,
        job_id: str,
        store: JobStore,
        part_size: int,
        uploaded_offset: int = 0,
        next_part: int = 1,
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.base = f"{prefix.strip('/')}/{job_id}"
        self.store = store
        self.part_size = part_size
        # Resume state (persisted in meta.json): bytes of results.jsonl already
        # uploaded, and the next part number.
        self.uploaded_offset = uploaded_offset
        self.next_part = next_part
        self._pending = 0
        self._lock = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()

    def state(self) -> dict[str, int]:
        return {"uploaded_offset": self.uploaded_offset, "next_part": self.next_part}

    def on_result(self) -> None:
        """Called after each result is appended; starts a part upload when due."""
        self._pending += 1
        if self._pending >= self.part_size:
            self._pending = 0
            task = asyncio.create_task(self.flush_part())
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def flush_part(self) -> None:
        """Upload everything written since the last upload as the next part."""
        async with self._lock:  # parts must go up one at a time, in order
            end = self.store.results_size()
            if end <= self.uploaded_offset:
                return
            key = f"{self.base}/results/part-{self.next_part:05d}.jsonl"
            try:
                data = await asyncio.to_thread(_read_range, self.store.results_path, self.uploaded_offset, end)
                await asyncio.to_thread(self._put, key, data)
            except Exception:
                log.exception("Spaces upload of %s failed; will retry with the next part", key)
                return
            log.info("uploaded %s (%d bytes) to Spaces", key, len(data))
            self.uploaded_offset = end
            self.next_part += 1

    async def finalize(self, meta: dict[str, Any]) -> None:
        """At job end: upload the last partial part, errors.jsonl, and meta.json."""
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.flush_part()
        try:
            await asyncio.to_thread(self._delete_stale_parts)
            if self.store.errors_path.exists():
                data = await asyncio.to_thread(self.store.errors_path.read_bytes)
                await asyncio.to_thread(self._put, f"{self.base}/errors.jsonl", data)
            await asyncio.to_thread(self._put, f"{self.base}/meta.json", json.dumps(meta, indent=2).encode())
        except Exception:
            log.exception("Spaces upload of final job files failed")

    def _delete_stale_parts(self) -> None:
        """Remove parts numbered at or beyond next_part, left over from before a crash.

        meta.json (which holds our upload state) is saved every few seconds, so after
        a kill -9 it can be behind the bucket. The resumed job then re-uploads from
        the older offset under the older part numbers; any higher-numbered part it
        never reaches again would duplicate data. Parts go up in order, so everything
        from next_part on is stale.
        """
        prefix = f"{self.base}/results/"
        start_after = f"{prefix}part-{self.next_part - 1:05d}.jsonl"
        while True:
            resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, StartAfter=start_after)
            keys = [obj["Key"] for obj in resp.get("Contents", [])]
            for key in keys:
                self.client.delete_object(Bucket=self.bucket, Key=key)
                log.info("deleted stale Spaces part %s", key)
            if not resp.get("IsTruncated") or not keys:
                return
            start_after = keys[-1]

    def _put(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)


def _read_range(path: Path, start: int, end: int) -> bytes:
    with open(path, "rb") as f:
        f.seek(start)
        return f.read(end - start)
