from __future__ import annotations

import logging
import signal
import sys
import threading
from types import FrameType

from prometheus_client import start_http_server

from app.config import Settings
from app.metrics import ConsumerMetrics
from app.worker import ConsumerWorker

logger = logging.getLogger("consumer")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )


def install_signal_handlers(stop_event: threading.Event) -> None:
    def handle(signum: int, _frame: FrameType | None) -> None:
        logger.info("Received %s, stopping", signal.Signals(signum).name)
        stop_event.set()

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)


def main() -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    stop_event = threading.Event()
    install_signal_handlers(stop_event)

    start_http_server(settings.metrics_port)
    logger.info("Metrics: http://localhost:%d/metrics", settings.metrics_port)

    worker = ConsumerWorker(settings.group_id, settings, stop_event, metrics=ConsumerMetrics())
    worker.start()
    logger.info("Started consumer group=%s", settings.group_id)

    stop_event.wait()

    worker.join(timeout=settings.shutdown_timeout_s)
    if worker.is_alive():
        logger.warning("Worker %s did not stop in %.0f s", worker.name, settings.shutdown_timeout_s)

    return 1 if worker.failed else 0


if __name__ == "__main__":
    sys.exit(main())
