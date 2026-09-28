# Architecture

```mermaid
flowchart TD
    Client([Client]) -->|"POST /job {input_file, webhook_url?}"| API[FastAPI]
    API -.->|"202 + job_id, instantly"| Client
    API -->|"start background asyncio task"| JM["Job Manager<br/>status + metrics per job"]

    subgraph ING["① Ingestion"]
        F[("sample_batch.json")] --> PF["Pre-flight pass<br/>validate JSON + count items<br/>(streams, no API calls)"]
        PF --> R["Streaming reader (ijson)<br/>yields (index, item) one at a time"]
        DS["Done-set from JSONL<br/>(resume after crash)"] -.->|"skip finished indexes"| R
    end

    subgraph SCAT["② Scatter"]
        R -->|"await put() blocks when full"| Q[["Bounded queue<br/>QUEUE_SIZE = 100"]]
        Q --> W1[Worker 1]
        Q --> W2[Worker 2]
        Q --> WN["Worker N<br/>(N = MAX_CONCURRENCY)"]
        W1 & W2 & WN --> V{"valid item?<br/>object, non-empty prompt,<br/>≤ MAX_PROMPT_CHARS"}
    end

    subgraph BP["③ Backpressure + throttling"]
        V -->|yes| L["Shared AIMD controller<br/>acquire slot (limit adapts)<br/>pause on Retry-After"]
        L --> C["Inference client<br/>1 slot per in-flight request"]
        C -->|HTTPS| DO[("DigitalOcean<br/>Serverless Inference")]
        DO -->|"429"| L2["halve limit (once per burst)<br/>+ full-jitter backoff<br/>(slot released while sleeping)"]
        DO -->|"5xx / timeout"| B["full-jitter backoff<br/>≤ MAX_RETRIES"]
        DO -->|"401 / 403 / 402 / 404"| X["stop the whole job<br/>(bad key, no billing, unknown model)"]
        DO -->|"200"| S["success: +1 slot<br/>(slow start, then additive)"]
        L2 --> L
        B --> L
    end

    subgraph GATH["④ Gather"]
        S --> RJ[("results.jsonl<br/>append + flush per item")]
        V -->|no| EJ[("errors.jsonl")]
        DO -->|"other 4xx / retries exhausted"| EJ
        RJ -->|"every SPACES_FLUSH_SECONDS, if anything new<br/>(boto3 in a thread)"| SP[("DO Spaces<br/>results/part-00001.jsonl …")]
        JM -->|"every 2s + at end"| MJ[("meta.json")]
    end

    JM -->|"job finished"| WH["Webhook POST<br/>summary + retries"]
    Client -->|"GET /job/{id}/status"| JM
    Client -->|"GET /job/{id}/download"| DL["StreamingResponse<br/>JSON array built line by line"]
    RJ --> DL
```

## Walkthrough

1. **Ingestion.** `POST /job` checks that the input file sits inside `DATA_DIR`, writes `meta.json`, starts a background task, and returns the job ID. The task first streams the file once to validate it and count items. A corrupt file fails here, before any money is spent. It then streams the file again, handing out `(index, item)` pairs one at a time. After a restart, indexes already in the JSONL files are skipped.
2. **Scatter.** The reader puts items on a bounded queue. When the queue is full, the reader waits, so it can never run ahead of the workers. That is the backpressure that keeps memory flat. A fixed pool of worker tasks pulls from the queue. Invalid items go straight to `errors.jsonl` without an API call.
3. **Backpressure and throttling.** Before each HTTP request a worker takes a slot from the shared controller, and it gives the slot back as soon as the response arrives, including before it sleeps for a retry. A 429 halves the number of slots, once per burst, and `Retry-After` pauses every worker. Successes add slots back. 5xx responses and timeouts are retried with full-jitter backoff. Other 4xx errors are not retried. A 401 or 403 (bad key), 402 (no billing) or 404 (unknown model or wrong URL) stops the whole job at once, because every item would fail the same way.
4. **Gather.** Each outcome is appended to `results.jsonl` or `errors.jsonl` as soon as it happens. Optionally, every `SPACES_FLUSH_SECONDS` (default 30) the newly written bytes are uploaded to Spaces as the next numbered part. Nothing is uploaded if nothing new was written. `meta.json` is written every 2 seconds and at the end. When the job finishes, the webhook fires. `download` streams the JSONL back as one JSON array, never holding it all in memory.
