from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

_TRUE_VALUES = frozenset({"1", "true", "yes", "t", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "f", "off"})


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be a boolean, got {raw!r}")


@dataclass(frozen=True, slots=True)
class Settings:
    bootstrap_servers: str
    topic: str
    total_messages: int
    http_port: int
    flush_timeout_s: float
    log_level: str

    linger_ms: int
    batch_size: int
    compression_type: str
    queue_buffering_max_messages: int
    message_timeout_ms: int
    acks: str
    enable_idempotence: bool
    message_template: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            bootstrap_servers=_env_str("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9094,kafka3:9096"),
            topic=_env_str("KAFKA_TOPIC", "load-test"),
            total_messages=_env_int("TOTAL_MESSAGES", 100_000),
            http_port=_env_int("HTTP_PORT", 8001),
            flush_timeout_s=_env_float("PRODUCER_FLUSH_TIMEOUT_S", 30.0),
            log_level=_env_str("LOG_LEVEL", "INFO"),
            linger_ms=_env_int("PRODUCER_LINGER_MS", 50),
            batch_size=_env_int("PRODUCER_BATCH_SIZE", 1_048_576),
            compression_type=_env_str("PRODUCER_COMPRESSION_TYPE", "snappy"),
            queue_buffering_max_messages=_env_int("PRODUCER_QUEUE_BUFFERING_MAX_MESSAGES", 200_000),
            message_timeout_ms=_env_int("PRODUCER_MESSAGE_TIMEOUT_MS", 60_000),
            acks=_env_str("PRODUCER_ACKS", "1"),
            enable_idempotence=_env_bool("PRODUCER_ENABLE_IDEMPOTENCE", False),
            message_template=_env_str("PRODUCER_MESSAGE_TEMPLATE", "message-{n}"),
        )

    def kafka_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "linger.ms": self.linger_ms,
            "batch.size": self.batch_size,
            "compression.type": self.compression_type,
            "queue.buffering.max.messages": self.queue_buffering_max_messages,
            "message.timeout.ms": self.message_timeout_ms,
            "acks": self.acks,
            "enable.idempotence": self.enable_idempotence,
        }
