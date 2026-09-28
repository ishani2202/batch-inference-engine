"""On-disk job state: output/{job_id}/meta.json, results.jsonl, errors.jsonl.

Each finished item is appended as one JSON line the moment it completes, so:
* memory stays flat (results never accumulate in RAM), and
* a crash never loses finished work: on restart we skip indexes already on disk.
"""

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any


class DoneSet:
    """Which input indexes are already finished, one byte per item.

    A Python set of 500,000 ints costs ~30 MB; this costs 500 KB.
    """

    def __init__(self, size: int) -> None:
        self._bits = bytearray(size)
        self.count = 0

    def add(self, index: int) -> bool:
        """Mark done. Returns False if it was already marked (a duplicate line)."""
        if not isinstance(index, int) or not 0 <= index < len(self._bits) or self._bits[index]:
            return False
        self._bits[index] = 1
        self.count += 1
        return True

    def __contains__(self, index: object) -> bool:
        return isinstance(index, int) and 0 <= index < len(self._bits) and self._bits[index] == 1


class JobStore:
    def __init__(self, root: Path, job_id: str) -> None:
        self.dir = root / job_id
        self.meta_path = self.dir / "meta.json"
        self.results_path = self.dir / "results.jsonl"
        self.errors_path = self.dir / "errors.jsonl"
        self._results_f = None
        self._errors_f = None

    # --- meta.json ---

    def write_meta(self, meta: dict[str, Any]) -> None:
        """Write atomically (temp file + rename) so a crash never leaves half a file."""
        self.dir.mkdir(parents=True, exist_ok=True)
        tmp = self.meta_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(meta, indent=2))
        os.replace(tmp, self.meta_path)

    def read_meta(self) -> dict[str, Any]:
        return json.loads(self.meta_path.read_text())

    # --- appending results ---

    def open(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        for path in (self.results_path, self.errors_path):
            _truncate_partial_last_line(path)
        self._results_f = open(self.results_path, "a", encoding="utf-8")
        self._errors_f = open(self.errors_path, "a", encoding="utf-8")

    def close(self) -> None:
        for f in (self._results_f, self._errors_f):
            if f is not None:
                f.close()
        self._results_f = self._errors_f = None

    def append_result(self, record: dict[str, Any]) -> None:
        _append(self._results_f, record)

    def append_error(self, record: dict[str, Any]) -> None:
        _append(self._errors_f, record)

    def results_size(self) -> int:
        return self.results_path.stat().st_size if self.results_path.exists() else 0

    # --- reading back ---

    def iter_results(self) -> Iterator[dict[str, Any]]:
        return iter_jsonl(self.results_path)

    def iter_errors(self) -> Iterator[dict[str, Any]]:
        return iter_jsonl(self.errors_path)


def _append(f, record: dict[str, Any]) -> None:
    if f is None:
        raise RuntimeError("JobStore.open() must be called before appending")
    f.write(json.dumps(record, ensure_ascii=False) + "\n")
    # flush() hands the line to the OS, so it survives a process crash. We skip
    # fsync (expensive per line): after a power loss the last few lines may be
    # lost and those items simply get re-run on resume (at-least-once).
    f.flush()


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Yield records from a JSONL file, skipping a torn (crash-truncated) line."""
    if not path.exists():
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.endswith("\n"):
                break  # torn last line from a crash
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _truncate_partial_last_line(path: Path) -> None:
    """If a crash left half a line at the end, cut it off before appending more.

    Otherwise the next record would be glued onto the broken one.
    """
    if not path.exists():
        return
    with open(path, "rb+") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size == 0:
            return
        f.seek(size - 1)
        if f.read(1) == b"\n":
            return
        # Walk back to the last newline (in blocks, to stay cheap on big files).
        pos = size
        block = 4096
        while pos > 0:
            start = max(0, pos - block)
            f.seek(start)
            chunk = f.read(pos - start)
            nl = chunk.rfind(b"\n")
            if nl != -1:
                f.truncate(start + nl + 1)
                return
            pos = start
        f.truncate(0)
