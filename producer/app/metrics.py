from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

_LATENCY_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)


class ProducerMetrics:
    """Метрики одной копии продюсера. В тестах передаётся отдельный registry."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        opts = {} if registry is None else {"registry": registry}
        self.produced = Counter(
            "producer_messages_produced_total",
            "Сообщения, принятые в очередь librdkafka",
            ["topic"],
            **opts,
        )
        self.acked = Counter(
            "producer_messages_acked_total",
            "Сообщения, подтверждённые брокером",
            ["topic"],
            **opts,
        )
        self.failed = Counter(
            "producer_messages_failed_total",
            "Сообщения, которые не удалось доставить",
            ["topic", "reason"],
            **opts,
        )
        # Старые имена: на них уже смотрит дашборд.
        self.sent = Counter(
            "producer_messages_sent_total",
            "Сообщения, подтверждённые брокером",
            **opts,
        )
        self.errors = Counter(
            "producer_errors_total",
            "Ошибки отправки",
            **opts,
        )
        self.buffer_full = Counter(
            "producer_buffer_full_total",
            "Сколько раз produce() упёрся в локальную очередь",
            **opts,
        )
        self.inflight = Gauge(
            "producer_inflight",
            "Сообщения в локальной очереди librdkafka",
            **opts,
        )
        self.flush_remaining = Gauge(
            "producer_flush_remaining",
            "Сколько сообщений осталось в очереди после flush",
            **opts,
        )
        self.duration = Histogram(
            "producer_send_duration_seconds",
            "Время от produce() до delivery callback",
            buckets=_LATENCY_BUCKETS,
            **opts,
        )
        self.latency = Histogram(
            "producer_send_latency_seconds",
            "Время от produce() до delivery callback",
            buckets=_LATENCY_BUCKETS,
            **opts,
        )
