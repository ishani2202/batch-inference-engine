import json

import pytest

from app.reader import InputFileError, count_items, iter_items
from app.storage import DoneSet, JobStore, iter_jsonl


# ---------- reader ----------


def test_streams_items_with_indexes(tmp_path):
    path = tmp_path / "b.json"
    path.write_text(json.dumps([{"prompt": "a"}, None, 3, {"prompt": "d", "score": 1.5}]))
    assert list(iter_items(path)) == [(0, {"prompt": "a"}), (1, None), (2, 3), (3, {"prompt": "d", "score": 1.5})]
    assert count_items(path) == 4


def test_empty_array(tmp_path):
    path = tmp_path / "b.json"
    path.write_text("  []  ")
    assert list(iter_items(path)) == []


@pytest.mark.parametrize(
    "content",
    [
        '[{"prompt": "a"}, {"prompt": ',  # truncated
        '[{"prompt": "a"} {"prompt": "b"}]',  # missing comma
        '{"prompt": "not an array"}',
        "",
    ],
)
def test_bad_files_raise_input_error(tmp_path, content):
    path = tmp_path / "b.json"
    path.write_text(content)
    with pytest.raises(InputFileError):
        count_items(path)


def test_missing_file(tmp_path):
    with pytest.raises(InputFileError):
        count_items(tmp_path / "nope.json")


# ---------- storage ----------


def test_append_and_read_back(tmp_path):
    store = JobStore(tmp_path, "job1")
    store.open()
    store.append_result({"index": 0, "response": "a"})
    store.append_result({"index": 2, "response": "c"})
    store.append_error({"index": 1, "error_type": "invalid_input"})
    store.close()
    assert [r["index"] for r in store.iter_results()] == [0, 2]
    assert [r["index"] for r in store.iter_errors()] == [1]


def test_torn_last_line_is_skipped_and_truncated(tmp_path):
    store = JobStore(tmp_path, "job1")
    store.dir.mkdir(parents=True)
    store.results_path.write_text('{"index": 0}\n{"index": 1}\n{"index": 2, "resp')  # crash mid-write
    assert [r["index"] for r in iter_jsonl(store.results_path)] == [0, 1]

    store.open()  # must cut the torn line so the next record isn't glued onto it
    store.append_result({"index": 5})
    store.close()
    assert [r["index"] for r in store.iter_results()] == [0, 1, 5]


def test_meta_roundtrip(tmp_path):
    store = JobStore(tmp_path, "job1")
    store.write_meta({"status": "running"})
    store.write_meta({"status": "completed"})
    assert store.read_meta() == {"status": "completed"}
    assert not list(store.dir.glob("*.tmp"))


def test_done_set():
    done = DoneSet(5)
    assert done.add(1) and done.add(4)
    assert not done.add(1)  # duplicate
    assert not done.add(99) and not done.add("x")  # out of range / wrong type
    assert 1 in done and 4 in done and 0 not in done and 99 not in done
    assert done.count == 2
