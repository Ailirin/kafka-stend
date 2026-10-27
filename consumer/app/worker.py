from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from app.config import Settings
from app.metrics import ConsumerMetrics
from app.offsets import OffsetPrefix

logger = logging.getLogger(__name__)

_PROGRESS_INTERVAL_S = 5.0
Handler = Callable[[Message], None]


class ConsumerWorker(threading.Thread):
    """Читает топик в рамках одной consumer group в отдельном потоке."""

    def __init__(
        self,
        group_id: str,
        settings: Settings,
        stop_event: threading.Event,
        handler: Handler | None = None,
        metrics: ConsumerMetrics | None = None,
    ) -> None:
        super().__init__(name=f"consumer-{group_id}", daemon=True)
        self.group_id = group_id
        self.failed = False
        self.processed = 0
        self._settings = settings
        self._stop_event = stop_event
        self._handler = handler if handler is not None else _noop
        self._metrics = metrics if metrics is not None else ConsumerMetrics()
        self._prefix = OffsetPrefix()
        self._consumed = self._metrics.consumed.labels(group_id=group_id)
        self._errors = self._metrics.errors.labels(group_id=group_id)
        self._commit_errors = self._metrics.commit_errors.labels(group_id=group_id, reason="sync_exception")
        self._batch_size = self._metrics.batch_size.labels(group_id=group_id)
        self._duration = self._metrics.duration.labels(group_id=group_id)
        self._last_progress = time.monotonic()

    def run(self) -> None:
        consumer = Consumer(self._settings.kafka_config(self.group_id))
        try:
            consumer.subscribe(
                [self._settings.topic],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
            self._log_start()
            while not self._stop_event.is_set():
                batch = self._collect_batch(consumer)
                if batch:
                    self._process_batch(consumer, batch)
            self._commit(consumer, batch_size=0)
        except Exception:
            self.failed = True
            logger.exception("group=%s consumer crashed, stopping the service", self.group_id)
            self._stop_event.set()
        finally:
            consumer.close()
            logger.info("group=%s stop processed=%d", self.group_id, self.processed)

    def consume_available(self, consumer: Any) -> None:
        """Дочитывает уже лежащие в клиенте сообщения. Для тестов и ревокации."""
        while not self._stop_event.is_set():
            batch = self._collect_batch(consumer)
            if not batch:
                return
            self._process_batch(consumer, batch)

    def _log_start(self) -> None:
        logger.info(
            "group=%s start topic=%s batch_size=%d enable.auto.commit=False max.poll.interval.ms=%d",
            self.group_id,
            self._settings.topic,
            self._settings.batch_size,
            self._settings.max_poll_interval_ms,
        )

    def _collect_batch(self, consumer: Any) -> list[Message]:
        if self._stop_event.is_set():
            return []
        batch: list[Message] = []
        timeout = self._settings.poll_timeout_s
        while len(batch) < self._settings.batch_size and not self._stop_event.is_set():
            msg = consumer.poll(timeout if not batch else 0)
            if msg is None:
                break
            err = msg.error()
            if err is not None:
                self._handle_error(err)
                continue
            batch.append(msg)
        return batch

    def _process_batch(self, consumer: Any, batch: list[Message]) -> None:
        started = time.perf_counter()
        handled = 0
        for msg in batch:
            if self._stop_event.is_set():
                break
            try:
                self._handler(msg)
            except Exception:
                logger.exception(
                    "group=%s handler failed topic=%s partition=%s offset=%s",
                    self.group_id,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                break
            if not self._prefix.observe_success(msg.topic(), msg.partition(), msg.offset()):
                logger.error(
                    "group=%s offset gap topic=%s partition=%s offset=%s",
                    self.group_id,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                break
            self.processed += 1
            handled += 1
            self._consumed.inc()
            self._metrics.processed.labels(
                group_id=self.group_id,
                topic=msg.topic(),
                partition=str(msg.partition()),
            ).inc()
            self._maybe_progress()
        self._batch_size.observe(handled)
        self._duration.observe(time.perf_counter() - started)
        self._commit(consumer, batch_size=len(batch))

    def _commit(self, consumer: Any, only: list[TopicPartition] | None = None, batch_size: int = 0) -> None:
        offsets = self._prefix.commit_offsets(only)
        if not offsets:
            return
        try:
            consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException:
            self._commit_errors.inc()
            logger.exception(
                "group=%s commit failed count=%d batch_size=%d partitions=%s",
                self.group_id,
                len(offsets),
                batch_size,
                [(tp.topic, tp.partition, tp.offset) for tp in offsets],
            )

    def _maybe_progress(self) -> None:
        now = time.monotonic()
        if now - self._last_progress < _PROGRESS_INTERVAL_S:
            return
        self._last_progress = now
        logger.info("group=%s progress processed=%d", self.group_id, self.processed)

    def _handle_error(self, err: KafkaError) -> None:
        if err.code() == KafkaError._PARTITION_EOF:
            return
        self._errors.inc()
        if err.fatal():
            raise RuntimeError(f"Fatal consumer error: {err}")
        logger.error("group=%s consumer error: %s", self.group_id, err)

    def _on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        logger.info(
            "group=%s assigned partitions: %s",
            self.group_id,
            sorted(p.partition for p in partitions),
        )

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        logger.info(
            "group=%s revoked partitions: %s",
            self.group_id,
            sorted(p.partition for p in partitions),
        )
        self._commit(consumer, only=partitions)


def _noop(_msg: Message) -> None:
    return None
