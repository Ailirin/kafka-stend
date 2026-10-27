from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass

from confluent_kafka import KafkaError, Message, Producer

from app.config import Settings
from app.metrics import ProducerMetrics

logger = logging.getLogger(__name__)

_BUFFER_FULL_POLL_TIMEOUT_S = 0.1


def render_message(template: str, n: int) -> bytes:
    """Подставляет номер без оператора % и без str.format.

    Шаблон вроде ``100% - сообщение #%d`` или ``{n}`` не должен падать.
    """
    number = str(n)
    return template.replace("{n}", number).replace("%d", number).encode()


def _failure_reason(err: KafkaError) -> str:
    code = err.code()
    if code == KafkaError._MSG_TIMED_OUT:
        return "timeout"
    if code in (KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN):
        return "transport"
    return "unknown"


@dataclass(frozen=True, slots=True)
class RunReport:
    expected: int
    produced: int
    delivered: int
    failed: int
    not_flushed: int
    elapsed_s: float
    interrupted: bool

    @property
    def rate(self) -> float:
        return self.delivered / self.elapsed_s if self.elapsed_s > 0 else 0.0

    @property
    def is_success(self) -> bool:
        return (
            not self.interrupted
            and self.failed == 0
            and self.not_flushed == 0
            and self.produced == self.expected
            and self.delivered == self.expected
        )


class LoadProducer:
    """Один прогон нагрузки. На каждый прогон создаётся новый экземпляр."""

    def __init__(
        self,
        settings: Settings,
        stop_event: threading.Event,
        producer: Producer | None = None,
        metrics: ProducerMetrics | None = None,
    ) -> None:
        self._settings = settings
        self._stop_event = stop_event
        self._metrics = metrics if metrics is not None else ProducerMetrics()
        self._produced = 0
        self._delivered = 0
        self._failed = 0
        self._closed = False
        self._flushed = False
        self._not_flushed = 0
        if producer is None:
            config = settings.kafka_config()
            config["error_cb"] = self._on_client_error
            producer = Producer(config)
        self._producer = producer

    @property
    def produced(self) -> int:
        return self._produced

    @property
    def delivered(self) -> int:
        return self._delivered

    @property
    def failed(self) -> int:
        return self._failed

    def run(self) -> RunReport:
        try:
            return self._run()
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._flush_once()
        closer = getattr(self._producer, "close", None)
        if closer is not None:
            closer()

    def _run(self) -> RunReport:
        total = self._settings.total_messages
        topic = self._settings.topic
        step = max(total // 10, 1)
        logger.info(
            "Start producing topic=%s message_count=%d acks=%s enable.idempotence=%s linger.ms=%s batch.size=%s",
            topic,
            total,
            self._settings.acks,
            self._settings.enable_idempotence,
            self._settings.linger_ms,
            self._settings.batch_size,
        )

        start = time.monotonic()
        for i in range(total):
            if self._stop_event.is_set():
                logger.warning("Stop requested, interrupting after %d messages", self._produced)
                break
            queued = self._produce(topic, render_message(self._settings.message_template, i))
            if queued is None:
                break
            self._produced += 1
            if queued:
                self._metrics.produced.labels(topic=topic).inc()
            self._metrics.inflight.set(len(self._producer))
            if self._produced % step == 0:
                logger.info("progress produced=%d of %d", self._produced, total)

        not_flushed = self._flush_once()
        elapsed = time.monotonic() - start
        if self._delivered + self._failed != self._produced:
            logger.error(
                "acked + failed != produced: acked=%d failed=%d produced=%d",
                self._delivered,
                self._failed,
                self._produced,
            )

        logger.info(
            "Stop producing produced=%d acked=%d failed=%d elapsed=%.1fs",
            self._produced,
            self._delivered,
            self._failed,
            elapsed,
        )
        return RunReport(
            expected=total,
            produced=self._produced,
            delivered=self._delivered,
            failed=self._failed,
            not_flushed=not_flushed,
            elapsed_s=elapsed,
            interrupted=self._stop_event.is_set(),
        )

    def _flush_once(self) -> int:
        if self._flushed:
            return self._not_flushed
        self._flushed = True
        logger.info("Flushing, timeout %.0f s", self._settings.flush_timeout_s)
        not_flushed = self._producer.flush(self._settings.flush_timeout_s)
        self._not_flushed = not_flushed
        self._metrics.flush_remaining.set(not_flushed)
        if not_flushed:
            logger.error(
                "flush timed out, remaining=%d flushed=%d",
                not_flushed,
                self._produced - not_flushed,
            )
        return not_flushed

    def _produce(self, topic: str, value: bytes) -> bool | None:
        """True — сообщение в очереди, False — очередь переполнена, None — остановка."""
        if self._stop_event.is_set():
            return None
        try:
            self._producer.produce(topic, value=value, on_delivery=self._on_delivery)
        except BufferError:
            self._failed += 1
            self._metrics.failed.labels(topic=topic, reason="buffer_full").inc()
            self._metrics.errors.inc()
            self._metrics.buffer_full.inc()
            logger.error("Local producer queue is full, message was not queued")
            self._producer.poll(_BUFFER_FULL_POLL_TIMEOUT_S)
            return False
        self._producer.poll(0)
        return True

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        topic = msg.topic() or self._settings.topic
        if err is not None:
            reason = _failure_reason(err)
            self._failed += 1
            self._metrics.failed.labels(topic=topic, reason=reason).inc()
            self._metrics.errors.inc()
            logger.error(
                "Delivery failed code=%s name=%s topic=%s partition=%s offset=%s reason=%s: %s",
                err.code(),
                err.name(),
                topic,
                msg.partition(),
                msg.offset(),
                reason,
                err,
            )
            return

        self._delivered += 1
        self._metrics.acked.labels(topic=topic).inc()
        self._metrics.sent.inc()
        latency = msg.latency()
        if latency is not None:
            self._metrics.duration.observe(latency)
            self._metrics.latency.observe(latency)

    def _on_client_error(self, err: KafkaError) -> None:
        logger.error("Kafka client error code=%s name=%s: %s", err.code(), err.name(), err)
