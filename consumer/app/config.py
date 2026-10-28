from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


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


@dataclass(frozen=True, slots=True)
class Settings:
    bootstrap_servers: str
    topic: str
    group_id: str
    metrics_port: int
    poll_timeout_s: float
    shutdown_timeout_s: float
    log_level: str
    batch_size: int
    auto_offset_reset: str
    session_timeout_ms: int
    heartbeat_interval_ms: int
    max_poll_interval_ms: int

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            bootstrap_servers=_env_str("KAFKA_BOOTSTRAP_SERVERS", "kafka1:19092,kafka2:19092,kafka3:19092"),
            topic=_env_str("KAFKA_TOPIC", "load-test"),
            group_id=_env_str("CONSUMER_GROUP_ID", "load-test-group"),
            metrics_port=_env_int("METRICS_PORT", 8082),
            poll_timeout_s=_env_float("CONSUMER_POLL_TIMEOUT_S", 1.0),
            shutdown_timeout_s=_env_float("CONSUMER_SHUTDOWN_TIMEOUT_S", 5.0),
            log_level=_env_str("LOG_LEVEL", "INFO"),
            batch_size=_env_int("CONSUMER_BATCH_SIZE", 1000),
            auto_offset_reset=_env_str("CONSUMER_AUTO_OFFSET_RESET", "earliest"),
            session_timeout_ms=_env_int("CONSUMER_SESSION_TIMEOUT_MS", 45_000),
            heartbeat_interval_ms=_env_int("CONSUMER_HEARTBEAT_INTERVAL_MS", 3_000),
            max_poll_interval_ms=_env_int("CONSUMER_MAX_POLL_INTERVAL_MS", 300_000),
        )

    def kafka_config(self, group_id: str | None = None) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id if group_id is not None else self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "session.timeout.ms": self.session_timeout_ms,
            "heartbeat.interval.ms": self.heartbeat_interval_ms,
            "max.poll.interval.ms": self.max_poll_interval_ms,
            "partition.assignment.strategy": "cooperative-sticky",
        }
