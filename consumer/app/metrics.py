from prometheus_client import CollectorRegistry, Counter, Histogram


class ConsumerMetrics:
    """Метрики одной копии сервиса. В тестах передаётся отдельный registry."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        opts = {} if registry is None else {"registry": registry}
        self.processed = Counter(
            "consumer_messages_processed_total",
            "Успешно обработанные сообщения",
            ["group_id", "topic", "partition"],
            **opts,
        )
        self.consumed = Counter(
            "consumer_messages_consumed_total",
            "Успешно обработанные сообщения",
            ["group_id"],
            **opts,
        )
        self.errors = Counter(
            "consumer_errors_total",
            "Ошибки консьюмера, кроме конца партиции",
            ["group_id"],
            **opts,
        )
        self.commit_errors = Counter(
            "consumer_commit_errors_total",
            "Ошибки коммита офсетов",
            ["group_id", "reason"],
            **opts,
        )
        self.batch_size = Histogram(
            "consumer_batch_size",
            "Сколько сообщений обработано в одном батче",
            ["group_id"],
            buckets=(1, 10, 50, 100, 500, 1000, 5000),
            **opts,
        )
        self.duration = Histogram(
            "consumer_processing_duration_seconds",
            "Время обработки одного батча",
            ["group_id"],
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
            **opts,
        )
