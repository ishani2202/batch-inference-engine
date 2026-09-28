import json
from pathlib import Path

import httpx
import pytest

from app.config import Settings

BASE_URL = "https://inference.test/v1"
CHAT_URL = f"{BASE_URL}/chat/completions"


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Fast, isolated settings: tiny backoffs, temp dirs, never the real API."""
    data = tmp_path / "data"
    data.mkdir()
    return Settings(
        _env_file=None,
        inference_url=BASE_URL,
        model_access_key="test-key",
        model="test-model",
        max_retries=3,
        backoff_base=0.001,
        backoff_cap=0.005,
        max_concurrency=8,
        min_concurrency=1,
        start_concurrency=4,
        queue_size=10,
        data_dir=data,
        output_dir=tmp_path / "output",
        meta_flush_interval=0.05,
        price_input_per_m=1.0,
        price_output_per_m=2.0,
        spaces_bucket="",
        spaces_key="",
        spaces_secret="",
    )


def ok(content: str = "an answer", prompt_tokens: int = 10, completion_tokens: int = 5) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "choices": [{"message": {"role": "assistant", "content": content}}],
            "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens},
        },
    )


def write_batch(settings: Settings, items: list, name: str = "batch.json") -> Path:
    path = settings.data_dir / name
    path.write_text(json.dumps(items))
    return path


def prompts(n: int) -> list[dict]:
    return [{"id": f"p{i}", "prompt": f"question {i}"} for i in range(n)]
