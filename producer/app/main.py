from __future__ import annotations

import logging

import uvicorn

from app.api import create_app
from app.config import Settings

logger = logging.getLogger("producer")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    logger.info(
        "Producer started topic=%s acks=%s enable.idempotence=%s linger.ms=%s port=%s",
        settings.topic,
        settings.acks,
        settings.enable_idempotence,
        settings.linger_ms,
        settings.http_port,
    )

    # Один процесс без workers: состояние прогона хранится в памяти RunManager.
    # SIGINT/SIGTERM обрабатывает uvicorn, остановка прогона — в lifespan приложения.
    uvicorn.run(
        create_app(settings),
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
