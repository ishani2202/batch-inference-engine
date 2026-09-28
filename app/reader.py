"""Streaming reader for a JSON array of prompt items.

Uses ijson so only one item is in memory at a time: reading a 500,000-item file
costs the same memory as reading a 10-item file.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import ijson


class InputFileError(Exception):
    """The input file is missing, not a JSON array, or not valid JSON."""


def _check_starts_with_array(f) -> None:
    # ijson.items(f, "item") silently yields nothing for a top-level object,
    # so check the first non-whitespace byte ourselves.
    while True:
        ch = f.read(1)
        if not ch:
            raise InputFileError("input file is empty")
        if not ch.isspace():
            break
    if ch != b"[":
        raise InputFileError("input file must contain a JSON array of prompt items")
    f.seek(0)


def iter_items(path: Path) -> Iterator[tuple[int, Any]]:
    """Yield (index, item) for each element of the top-level JSON array."""
    try:
        with open(path, "rb") as f:
            _check_starts_with_array(f)
            for index, item in enumerate(ijson.items(f, "item", use_float=True)):
                yield index, item
    except ijson.JSONError as exc:
        raise InputFileError(f"invalid JSON in input file: {exc}") from None
    except OSError as exc:
        raise InputFileError(f"cannot read input file: {exc}") from None


def count_items(path: Path) -> int:
    """Stream the whole file once to validate it and count items (flat memory).

    Doing this before any API call means a corrupt file fails the job before we
    spend money, and gives the status endpoint a real total for progress.
    """
    return sum(1 for _ in iter_items(path))
