from __future__ import annotations

from confluent_kafka import TopicPartition


class OffsetPrefix:
    """Непрерывный префикс успешно обработанных офсетов по партициям.

    В брокер уходит next-to-read: последний успешный offset + 1.
    Дырка префикс не перепрыгивает.
    """

    def __init__(self) -> None:
        self._next: dict[tuple[str, int], int] = {}

    def observe_success(self, topic: str, partition: int, offset: int) -> bool:
        key = (topic, partition)
        expected = self._next.get(key)
        if expected is None or offset == expected:
            self._next[key] = offset + 1
            return True
        return offset < expected

    def commit_offsets(self, only: list[TopicPartition] | None = None) -> list[TopicPartition]:
        if only is None:
            keys = list(self._next)
        else:
            keys = [(tp.topic, tp.partition) for tp in only if (tp.topic, tp.partition) in self._next]
        return [TopicPartition(topic, partition, self._next[(topic, partition)]) for topic, partition in keys]
