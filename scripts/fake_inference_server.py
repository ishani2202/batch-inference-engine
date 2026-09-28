"""A stand-in inference API for load testing, with a hidden capacity limit.

Behaves like an OpenAI-compatible /v1/chat/completions endpoint, but:
* if more than FAKE_CAPACITY requests are in flight, it answers 429 (sometimes
  with Retry-After), like a real provider under load;
* FAKE_ERROR_RATE of requests fail with a 500;
* each request takes a random FAKE_LATENCY_MIN..MAX milliseconds.

The engine doesn't know the capacity. The point is to watch the adaptive
controller find it. Used for the 500,000-item memory benchmark too.

    FAKE_CAPACITY=20 uvicorn scripts.fake_inference_server:app --port 9000
"""

import asyncio
import os
import random

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

CAPACITY = int(os.getenv("FAKE_CAPACITY", "20"))
ERROR_RATE = float(os.getenv("FAKE_ERROR_RATE", "0.02"))
LATENCY_MIN = float(os.getenv("FAKE_LATENCY_MIN", "50")) / 1000
LATENCY_MAX = float(os.getenv("FAKE_LATENCY_MAX", "250")) / 1000
RETRY_AFTER_RATE = float(os.getenv("FAKE_RETRY_AFTER_RATE", "0.2"))

app = FastAPI(title="Fake inference API")
state = {"in_flight": 0, "requests": 0, "rate_limited": 0, "errors": 0}


@app.post("/v1/chat/completions")
async def chat(request: Request):
    body = await request.json()
    state["requests"] += 1
    if state["in_flight"] >= CAPACITY:
        state["rate_limited"] += 1
        headers = {"Retry-After": "1"} if random.random() < RETRY_AFTER_RATE else {}
        return JSONResponse({"error": "rate limit exceeded"}, status_code=429, headers=headers)
    state["in_flight"] += 1
    try:
        await asyncio.sleep(random.uniform(LATENCY_MIN, LATENCY_MAX))
        if random.random() < ERROR_RATE:
            state["errors"] += 1
            return JSONResponse({"error": "internal error"}, status_code=500)
        prompt = body["messages"][0]["content"]
        return {
            "id": "fake",
            "object": "chat.completion",
            "model": body.get("model"),
            "choices": [{"index": 0, "message": {"role": "assistant", "content": f"Fake answer to: {prompt[:60]}"}}],
            "usage": {"prompt_tokens": len(prompt) // 4 + 1, "completion_tokens": 12},
        }
    finally:
        state["in_flight"] -= 1


@app.get("/stats")
async def stats():
    return state
