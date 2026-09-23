import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


@dataclass(frozen=True)
class Config:
    broker_type: str = os.getenv("BROKER_TYPE", "memory").lower()
    broker_connection_url: str = os.getenv("BROKER_CONNECTION_URL", "localhost:9092")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "saga.dead_letter")

    state_store_type: str = os.getenv("STATE_STORE_TYPE", "memory").lower()
    database_url: str = os.getenv("DATABASE_URL", "")

    saga_timeout_ms: int = _env_int("SAGA_TIMEOUT_MS", 30000)
    max_compensation_retries: int = _env_int("MAX_COMPENSATION_RETRIES", 3)
    step_retry_backoff_ms: int = _env_int("STEP_RETRY_BACKOFF_MS", 500)

    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()


config = Config()
