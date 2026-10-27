from __future__ import annotations

import logging
import threading
from dataclasses import replace

import pytest
from confluent_kafka import KafkaError
from prometheus_client import CollectorRegistry

from app.config import Settings
from app.metrics import ProducerMetrics
from app.producer import LoadProducer, render_message

_ENV_NAMES = (
    "TOTAL_MESSAGES",
    "PRODUCER_LINGER_MS",
    "PRODUCER_FLUSH_TIMEOUT_S",
    "PRODUCER_ACKS",
    "PRODUCER_ENABLE_IDEMPOTENCE",
    "PRODUCER_QUEUE_BUFFERING_MAX_MESSAGES",
    "PRODUCER_MESSAGE_TIMEOUT_MS",
    "PRODUCER_MESSAGE_TEMPLATE",
    "PRODUCER_BATCH_SIZE",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def _settings(**overrides: object) -> Settings:
    return replace(Settings.from_env(), **overrides)


class FakeMessage:
    def __init__(self, topic: str, offset: int) -> None:
        self._topic = topic
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def latency(self) -> float:
        return 0.01


class FakeProducer:
    def __init__(
        self,
        *,
        buffer_at: int | None = None,
        fail_delivery: bool = False,
        drop_last: int = 0,
        flush_remaining: int = 0,
        stop_after: int | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.buffer_at = buffer_at
        self.fail_delivery = fail_delivery
        self.drop_last = drop_last
        self.flush_remaining = flush_remaining
        self.stop_after = stop_after
        self.stop_event = stop_event
        self.pending: list[tuple[str, bytes, object]] = []
        self.attempts = 0
        self.queued = 0
        self.poll_timeouts: list[float] = []
        self.flush_calls = 0
        self.close_calls = 0
        self._len = 0

    def produce(self, topic: str, value: bytes | None = None, on_delivery: object = None) -> None:
        self.attempts += 1
        if self.buffer_at is not None and self.attempts == self.buffer_at:
            raise BufferError("queue full")
        self.queued += 1
        self.pending.append((topic, value or b"", on_delivery))
        self._len += 1
        if self.stop_event is not None and self.stop_after is not None and self.queued == self.stop_after:
            self.stop_event.set()

    def poll(self, timeout: float) -> None:
        self.poll_timeouts.append(timeout)

    def flush(self, timeout: float) -> int:
        self.flush_calls += 1
        pending = self.pending
        self.pending = []
        if self.flush_remaining:
            deliver = pending[: max(0, len(pending) - self.flush_remaining)]
            self._deliver(deliver)
            self._len = self.flush_remaining
            return self.flush_remaining
        if self.drop_last:
            deliver = pending[: max(0, len(pending) - self.drop_last)]
            self._deliver(deliver)
            self._len = 0
            return 0
        self._deliver(pending)
        self._len = 0
        return 0

    def close(self) -> None:
        self.close_calls += 1

    def __len__(self) -> int:
        return self._len

    def _deliver(self, items: list[tuple[str, bytes, object]]) -> None:
        for index, (topic, _value, callback) in enumerate(items):
            message = FakeMessage(topic, index)
            if self.fail_delivery:
                callback(KafkaError(KafkaError._MSG_TIMED_OUT, "timed out"), message)
            else:
                callback(None, message)


def _run(total: int = 10, **fake_kwargs: object) -> tuple[FakeProducer, object]:
    fake = FakeProducer(**fake_kwargs)
    metrics = ProducerMetrics(CollectorRegistry())
    load = LoadProducer(_settings(total_messages=total), threading.Event(), producer=fake, metrics=metrics)
    return fake, load.run()


def test_success_acks_all_messages() -> None:
    _fake, report = _run(10)
    assert report.delivered == 10
    assert report.failed == 0
    assert report.is_success


def test_delivery_errors_increment_failed() -> None:
    _fake, report = _run(10, fail_delivery=True)
    assert report.delivered == 0
    assert report.failed == 10
    assert report.delivered + report.failed == report.produced
    assert not report.is_success


def test_missing_callbacks_are_not_success() -> None:
    _fake, report = _run(10, drop_last=2)
    assert report.delivered == 8
    assert not report.is_success


def test_flush_remaining_is_an_error(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR)
    _fake, report = _run(10, flush_remaining=5)
    assert not report.is_success
    assert any("remaining=5" in record.getMessage() for record in caplog.records)


def test_poll_zero_inside_send_loop() -> None:
    fake, report = _run(10)
    zeros = [timeout for timeout in fake.poll_timeouts if timeout == 0]
    assert len(zeros) >= report.produced - 1


def test_buffer_error_does_not_abort_the_run() -> None:
    _fake, report = _run(10, buffer_at=5)
    assert report.produced == 10
    assert report.failed == 1
    assert report.delivered == 9
    assert report.delivered + report.failed == report.produced
    assert not report.is_success


def test_template_with_percent_and_braces() -> None:
    assert render_message("100% - сообщение #%d", 7) == "100% - сообщение #7".encode()
    assert render_message("100% - {n} {ok}", 3) == b"100% - 3 {ok}"


@pytest.mark.parametrize("raw", ["true", "TRUE", "True", "t", "1", "yes"])
def test_bool_true(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("PRODUCER_ENABLE_IDEMPOTENCE", raw)
    assert Settings.from_env().enable_idempotence is True


@pytest.mark.parametrize("raw", ["false", "FALSE", "f", "0", "no", "off"])
def test_bool_false(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("PRODUCER_ENABLE_IDEMPOTENCE", raw)
    assert Settings.from_env().enable_idempotence is False


def test_bool_garbage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRODUCER_ENABLE_IDEMPOTENCE", "maybe")
    with pytest.raises(ValueError, match="PRODUCER_ENABLE_IDEMPOTENCE"):
        Settings.from_env()


def test_invalid_linger_mentions_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRODUCER_LINGER_MS", "not-a-duration")
    with pytest.raises(ValueError, match="PRODUCER_LINGER_MS"):
        Settings.from_env()


def test_default_message_count_is_100k() -> None:
    settings = Settings.from_env()
    assert settings.total_messages == 100_000
    assert settings.acks == "1"
    assert settings.enable_idempotence is False
    assert settings.flush_timeout_s * 1000 < settings.message_timeout_ms
    config = settings.kafka_config()
    assert isinstance(config["bootstrap.servers"], str)
    assert "," in config["bootstrap.servers"]
    assert config["acks"] == "1"
    assert config["enable.idempotence"] is False


def test_progress_logged_for_100k(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    _run(100_000)
    progress = [record for record in caplog.records if record.getMessage().startswith("progress ")]
    assert len(progress) >= 5


def test_close_is_idempotent() -> None:
    fake = FakeProducer()
    load = LoadProducer(_settings(total_messages=1), threading.Event(), producer=fake, metrics=ProducerMetrics(CollectorRegistry()))
    load.close()
    load.close()
    assert fake.flush_calls == 1
    assert fake.close_calls == 1


def test_shutdown_flushes_messages_already_queued() -> None:
    stop = threading.Event()
    fake = FakeProducer(stop_after=5, stop_event=stop)
    load = LoadProducer(
        _settings(total_messages=10),
        stop,
        producer=fake,
        metrics=ProducerMetrics(CollectorRegistry()),
    )
    report = load.run()
    assert report.produced == 5
    assert report.delivered == 5
    assert report.failed == 0
    assert fake.flush_calls == 1
    assert fake.close_calls == 1
    assert not report.is_success
