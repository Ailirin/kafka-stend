from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum

from app.config import Settings
from app.metrics import ProducerMetrics
from app.producer import LoadProducer, RunReport

logger = logging.getLogger(__name__)


class RunStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


class RunAlreadyActiveError(Exception):
    def __init__(self, run_id: int) -> None:
        super().__init__(f"Run {run_id} is already running")
        self.run_id = run_id


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    """Состояние прогона на момент запроса; безопасно отдавать в другой поток."""

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


@dataclass(slots=True)
class _RunState:
    id: int
    total_messages: int
    started_at: datetime
    started_monotonic: float
    status: RunStatus = RunStatus.RUNNING
    producer: LoadProducer | None = None
    report: RunReport | None = None
    finished_at: datetime | None = None
    error: str | None = None


class RunManager:
    """Запускает прогоны в фоновом потоке и не допускает двух прогонов одновременно."""

    def __init__(self, settings: Settings, metrics: ProducerMetrics | None = None) -> None:
        self._settings = settings
        self._metrics = metrics if metrics is not None else ProducerMetrics()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._current: _RunState | None = None
        self._thread: threading.Thread | None = None
        self._last_id = 0

    def start(self) -> RunSnapshot:
        with self._lock:
            if self._current is not None and self._current.status is RunStatus.RUNNING:
                raise RunAlreadyActiveError(self._current.id)

            self._last_id += 1
            state = _RunState(
                id=self._last_id,
                total_messages=self._settings.total_messages,
                started_at=datetime.now(timezone.utc),
                started_monotonic=time.monotonic(),
            )
            self._current = state
            self._thread = threading.Thread(
                target=self._execute, args=(state,), name=f"run-{state.id}", daemon=True
            )
            self._thread.start()
            logger.info("Run %d started", state.id)
            return self._snapshot(state)

    def current(self) -> RunSnapshot | None:
        with self._lock:
            return self._snapshot(self._current) if self._current is not None else None

    def shutdown(self, timeout_s: float) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            logger.info("Waiting for the active run to flush")
            thread.join(timeout_s)
            if thread.is_alive():
                logger.warning("Active run did not finish in %.0f s", timeout_s)

    def _execute(self, state: _RunState) -> None:
        try:
            # Producer создаётся в потоке прогона и используется только в нём.
            producer = LoadProducer(self._settings, self._stop_event, metrics=self._metrics)
            with self._lock:
                state.producer = producer
            report = producer.run()
        except Exception as exc:
            logger.exception("Run %d failed", state.id)
            with self._lock:
                state.status = RunStatus.FAILED
                state.error = str(exc)
                state.finished_at = datetime.now(timezone.utc)
            return

        with self._lock:
            state.report = report
            state.finished_at = datetime.now(timezone.utc)
            if report.is_success:
                state.status = RunStatus.COMPLETED
            elif report.interrupted:
                state.status = RunStatus.INTERRUPTED
            else:
                state.status = RunStatus.FAILED
                state.error = (
                    f"delivered {report.delivered} of {report.expected}, "
                    f"failed={report.failed}, not_flushed={report.not_flushed}"
                )

        logger.info(
            "Run %d %s: produced=%d delivered=%d failed=%d not_flushed=%d elapsed=%.1fs rate=%.0f msg/sec",
            state.id,
            state.status,
            report.produced,
            report.delivered,
            report.failed,
            report.not_flushed,
            report.elapsed_s,
            report.rate,
        )

    @staticmethod
    def _snapshot(state: _RunState) -> RunSnapshot:
        report = state.report
        if report is not None:
            produced, delivered, failed = report.produced, report.delivered, report.failed
            not_flushed: int | None = report.not_flushed
            elapsed = report.elapsed_s
        else:
            producer = state.producer
            produced = producer.produced if producer else 0
            delivered = producer.delivered if producer else 0
            failed = producer.failed if producer else 0
            not_flushed = None
            if state.finished_at is not None:
                elapsed = (state.finished_at - state.started_at).total_seconds()
            else:
                elapsed = time.monotonic() - state.started_monotonic

        return RunSnapshot(
            id=state.id,
            status=state.status,
            total_messages=state.total_messages,
            started_at=state.started_at,
            finished_at=state.finished_at,
            produced=produced,
            delivered=delivered,
            failed=failed,
            not_flushed=not_flushed,
            elapsed_s=round(elapsed, 3),
            rate=round(delivered / elapsed, 1) if elapsed > 0 else 0.0,
            error=state.error,
        )
