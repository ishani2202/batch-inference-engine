"""All runtime settings, read from environment variables (or a local .env file).

Keeping every knob here means a reviewer can run the service with their own key,
model, and limits without touching code.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # `protected_namespaces=()` lets us use field names that start with "model_".
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", protected_namespaces=())

    # --- Inference provider (DigitalOcean Serverless Inference, OpenAI-compatible) ---
    inference_url: str = "https://inference.do-ai.run/v1"
    model_access_key: str = ""
    model: str = "mistral-3-14B"  # small, non-reasoning, $0.20/$0.20 per 1M tokens on DO
    max_tokens: int = 128
    temperature: float = 0.2
    request_timeout: float = 60.0

    # --- Retries: exponential backoff with full jitter ---
    max_retries: int = 6  # retries for errors (5xx, timeouts, connection)
    max_rate_limit_retries: int = 20  # separate, larger budget for 429s: they are backpressure, not faults
    backoff_base: float = 1.0  # seconds
    backoff_cap: float = 30.0  # seconds
    max_retry_after: float = 60.0  # never trust a server asking us to wait longer than this

    # --- Concurrency: adaptive (AIMD) limit shared by all workers ---
    max_concurrency: int = 32  # also the number of worker tasks
    min_concurrency: int = 1
    start_concurrency: int = 8
    queue_size: int = 100  # items buffered between reader and workers

    # --- Input / output ---
    data_dir: Path = Path("data")
    output_dir: Path = Path("output")
    max_prompt_chars: int = 32_000
    meta_flush_interval: float = 2.0  # seconds between meta.json snapshots

    # --- Cost estimate (USD per 1M tokens; fill from the DO pricing page) ---
    price_input_per_m: float = 0.0
    price_output_per_m: float = 0.0

    # --- DigitalOcean Spaces (optional extension) ---
    spaces_bucket: str = ""
    spaces_region: str = "nyc3"
    spaces_endpoint: str = ""  # defaults to https://{region}.digitaloceanspaces.com
    spaces_key: str = ""
    spaces_secret: str = ""
    spaces_prefix: str = "batch-jobs"
    spaces_flush_seconds: float = 30.0  # upload new results this often (skipped if nothing new)

    # --- Webhook (optional extension) ---
    webhook_retries: int = 3
    webhook_timeout: float = 10.0

    @property
    def spaces_enabled(self) -> bool:
        return bool(self.spaces_bucket and self.spaces_key and self.spaces_secret)


@lru_cache
def get_settings() -> Settings:
    return Settings()
