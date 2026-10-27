from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict

from app.config import Settings
from app.runner import RunAlreadyActiveError, RunManager, RunStatus

# Запас поверх flush_timeout на завершение цикла отправки при остановке сервиса.
_SHUTDOWN_EXTRA_S = 5.0


class RunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: RunStatus
    total_messages: int
    started_at: datetime
    finished_at: datetime | None
    produced: int
    delivered: int
    failed: int
    not_flushed: int | None
    elapsed_s: float
    rate: float
    error: str | None


def create_app(settings: Settings) -> FastAPI:
    manager = RunManager(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        # join() блокирующий, поэтому выносим его из event loop.
        await asyncio.to_thread(manager.shutdown, settings.flush_timeout_s + _SHUTDOWN_EXTRA_S)

    app = FastAPI(title="Kafka load producer", lifespan=lifespan)

    @app.post("/runs", status_code=status.HTTP_202_ACCEPTED, response_model=RunResponse)
    def start_run() -> RunResponse:
        try:
            snapshot = manager.start()
        except RunAlreadyActiveError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return RunResponse.model_validate(snapshot)

    @app.get("/runs/current", response_model=RunResponse)
    def current_run() -> RunResponse:
        snapshot = manager.current()
        if snapshot is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No runs yet")
        return RunResponse.model_validate(snapshot)

    @app.get("/health", include_in_schema=False)
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready", include_in_schema=False)
    def ready() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
