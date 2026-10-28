from __future__ import annotations

import threading

from confluent_kafka import KafkaError, KafkaException, TopicPartition
from prometheus_client import CollectorRegistry

from app.config import Settings
from app.metrics import ConsumerMetrics
from app.worker import ConsumerWorker

TOPIC = "load-test"


class FakeMessage:
    def __init__(self, offset: int, partition: int = 0, error: KafkaError | None = None) -> None:
        self._offset = offset
        self._partition = partition
        self._error = error

    def error(self) -> KafkaError | None:
        return self._error

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage], fail_commits: int = 0) -> None:
        self._messages = list(messages)
        self._fail_commits = fail_commits
        self._stored: dict[tuple[str, int], int] = {}
        self.commits: list[list[TopicPartition]] = []

    def poll(self, _timeout: float) -> FakeMessage | None:
        if not self._messages:
            return None
        return self._messages.pop(0)

    def commit(self, offsets: list[TopicPartition], asynchronous: bool = True) -> None:
        if asynchronous:
            raise AssertionError("commit must be synchronous")
        if self._fail_commits:
            self._fail_commits -= 1
            raise KafkaException(KafkaError(KafkaError._MSG_TIMED_OUT, "commit failed"))
        self.commits.append(offsets)
        for tp in offsets:
            self._stored[(tp.topic, tp.partition)] = tp.offset

    def committed(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        return [
            TopicPartition(tp.topic, tp.partition, self._stored.get((tp.topic, tp.partition), -1001))
            for tp in partitions
        ]


def _settings(batch_size: int = 1000) -> Settings:
    return Settings(
        bootstrap_servers="localhost:9092",
        topic=TOPIC,
        group_id="load-test-group",
        metrics_port=0,
        poll_timeout_s=0.01,
        shutdown_timeout_s=1,
        log_level="INFO",
        batch_size=batch_size,
        auto_offset_reset="earliest",
        session_timeout_ms=45_000,
        heartbeat_interval_ms=3_000,
        max_poll_interval_ms=300_000,
    )


def _worker(handler=None, batch_size: int = 1000, stop: threading.Event | None = None) -> ConsumerWorker:
    return ConsumerWorker(
        "load-test-group",
        _settings(batch_size),
        stop or threading.Event(),
        handler=handler,
        metrics=ConsumerMetrics(CollectorRegistry()),
    )


def _committed(consumer: FakeConsumer, partition: int = 0) -> int:
    return consumer.committed([TopicPartition(TOPIC, partition)])[0].offset


def _messages(count: int, start: int = 0) -> list[FakeMessage]:
    return [FakeMessage(offset) for offset in range(start, start + count)]


def test_commit_only_processed_prefix() -> None:
    def handler(msg: FakeMessage) -> None:
        if msg.offset() == 1499:
            raise RuntimeError("handler failed")

    consumer = FakeConsumer(_messages(2500))
    _worker(handler).consume_available(consumer)
    assert _committed(consumer) == 1499


def test_hole_in_batch_is_not_skipped() -> None:
    def handler(msg: FakeMessage) -> None:
        if msg.offset() == 500:
            raise RuntimeError("hole")

    consumer = FakeConsumer(_messages(1000))
    _worker(handler).consume_available(consumer)
    assert _committed(consumer) == 500


def test_shutdown_before_start_commits_nothing() -> None:
    stop = threading.Event()
    stop.set()
    consumer = FakeConsumer(_messages(10))
    _worker(stop=stop).consume_available(consumer)
    assert consumer.commits == []
    assert _committed(consumer) == -1001


def test_shutdown_mid_batch() -> None:
    stop = threading.Event()

    def handler(msg: FakeMessage) -> None:
        if msg.offset() == 499:
            stop.set()

    consumer = FakeConsumer(_messages(1000))
    _worker(handler, stop=stop).consume_available(consumer)
    assert _committed(consumer) == 500


def test_empty_poll_does_not_commit() -> None:
    consumer = FakeConsumer([])
    worker = _worker()
    worker.consume_available(consumer)
    assert consumer.commits == []
    assert worker.processed == 0


def test_partition_eof_is_not_a_commit_error() -> None:
    eof = FakeMessage(0, error=KafkaError(KafkaError._PARTITION_EOF))
    consumer = FakeConsumer([eof, FakeMessage(0)])
    worker = _worker()
    worker.consume_available(consumer)
    assert worker._commit_errors._value.get() == 0
    assert worker._errors._value.get() == 0
    assert _committed(consumer) == 1


def test_commit_error_is_counted_and_next_batch_continues() -> None:
    consumer = FakeConsumer(_messages(2), fail_commits=1)
    worker = _worker(batch_size=1)
    worker.consume_available(consumer)
    assert worker.processed == 2
    assert worker._commit_errors._value.get() == 1
    assert _committed(consumer) == 2


def test_commit_offset_is_next_to_read() -> None:
    consumer = FakeConsumer([FakeMessage(99)])
    _worker().consume_available(consumer)
    assert _committed(consumer) == 100


def test_on_revoke_commits_processed_prefix() -> None:
    consumer = FakeConsumer(_messages(500))
    worker = _worker()
    worker.consume_available(consumer)
    consumer._stored.clear()
    consumer.commits.clear()
    worker._on_revoke(consumer, [TopicPartition(TOPIC, 0)])
    assert _committed(consumer) == 500


def test_auto_commit_is_disabled() -> None:
    config = _settings().kafka_config()
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["max.poll.interval.ms"] == 300_000
    assert config["heartbeat.interval.ms"] < config["session.timeout.ms"] / 3


def test_kafka_config_has_no_message_count() -> None:
    assert "message_count" not in _settings().kafka_config()
    assert not hasattr(_settings(), "total_messages")
