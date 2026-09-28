# 500,000-item benchmark: raw evidence

Raw output from the run summarized in the main README's [Scaling and memory](../../../README.md#scaling-and-memory) section. The run used 500,000 items (46 MB, made with `scripts/generate_batch.py -n 500000`) against `scripts/fake_inference_server.py` (`FAKE_CAPACITY=200`, 1–5 ms latency, 1% random 500s), with `MAX_CONCURRENCY=64`, `START_CONCURRENCY=32`, on a laptop.

The 46 MB input and the 129 MB `results.jsonl` are not committed. Regenerate them with the commands in the main README.

| File | Contents |
|---|---|
| `rss.log` | One line every 5 s during the run: `unix_time rss_kb=<service RSS in KB> <status> <done> <total> <items_per_second> <concurrency_limit> <rate_limited_429s>` |
| `dl_rss.log` | Service RSS in KB, sampled every 0.2 s while the 129 MB `/download` streamed (after the service restarted and reloaded the finished job) |
| `meta.json` | The job's final `meta.json`, as written by the service |
| `errors.jsonl` | The job's `errors.jsonl`: the 7 deliberately broken input items |
