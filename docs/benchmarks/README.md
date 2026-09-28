# Evidence

Raw output behind every number in the main README. The large regenerable files (inputs and big result sets) are not included.

| Folder | What it backs up | Key files |
|---|---|---|
| [`real-do-run/`](real-do-run/) | The real 1,000-prompt run on DigitalOcean (`mistral-3-14B`): 993 ok, 7 bad rows isolated, 765 real 429s, $0.016 | `meta.json` (final status and metrics), `results.jsonl` (all 993 real model answers), `errors.jsonl` (the 7 bad rows), `progress.log` (status every 3 s), `controller.log` (every concurrency-limit change), `rate-limit-headers.txt` |
| [`adaptive-vs-fixed/`](adaptive-vs-fixed/) | Adaptive controller vs fixed concurrency 64, same fake API with a hidden capacity of 20 | `adaptive/meta.json` (10.4 s, 6 × 429), `fixed64/meta.json` (71.6 s, 820 × 429), `adaptive/controller.log` (the AIMD sawtooth) |
| [`crash-recovery/`](crash-recovery/) | `kill -9` of the service at 159 of 1,000 items, then an automatic resume on restart | `server_before_kill.log`, `server_after_restart.log` (the "resuming job" lines), `meta.json` (`resumed: true`), `verification.txt` (1,000 records, 1,000 unique, exactly 0..999) |
| [`500k/`](500k/) | Memory stays flat on 500,000 items | `rss.log` (memory every 5 s), `dl_rss.log`, `meta.json`, see its README |

How the comparison and crash runs were made: start `scripts/fake_inference_server.py` (with `FAKE_CAPACITY=20` for the comparison, `10` for the crash test), point the service at it with `INFERENCE_URL=http://127.0.0.1:9000/v1 MODEL_ACCESS_KEY=fake`, and run `scripts/run_job.py`. For the baseline, also set `MIN_CONCURRENCY=MAX_CONCURRENCY=START_CONCURRENCY=64`. For the crash test, `kill -9` the service about 4 s into the job, then start it again.
