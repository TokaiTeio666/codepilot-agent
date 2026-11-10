"""Consumers for the GitHub ingress stream."""

import asyncio
import logging
import random
import time
import uuid
from collections.abc import Mapping
from contextlib import suppress

from agent.github.ingress.config import IngressSettings
from agent.github.ingress.handlers import HANDLERS, IngressHandler
from agent.github.ingress.models import IngressJob
from agent.github.ingress.queue import (
    GithubIngressQueue,
    IngressUnavailable,
    QueuedMessage,
)

logger = logging.getLogger(__name__)

_READ_BLOCK_MS = 500
_MAINTENANCE_INTERVAL_SECONDS = 5.0
_SLOT_REAP_INTERVAL_SECONDS = 60.0
_RECLAIM_INTERVAL_SECONDS = 60.0
_JITTER_FRACTION = 0.2


def retry_delay(attempt: int, *, base_seconds: float, max_seconds: float) -> float:
    """Exponential backoff with jitter; ``attempt`` is 1 for the first retry."""
    exponential = min(max_seconds, base_seconds * (2 ** (attempt - 1)))
    return exponential * random.uniform(1 - _JITTER_FRACTION, 1 + _JITTER_FRACTION)


class GithubIngressWorker:
    """Runs ``settings.local_concurrency`` consumers against one stream."""

    def __init__(
        self,
        queue: GithubIngressQueue,
        settings: IngressSettings,
        *,
        worker_id: str,
        stop: asyncio.Event,
        handlers: Mapping[str, IngressHandler] | None = None,
    ) -> None:
        self._queue = queue
        self._settings = settings
        self._worker_id = worker_id
        self._stop = stop
        self._handlers: Mapping[str, IngressHandler] = (
            handlers if handlers is not None else HANDLERS
        )
        self._next_reclaim_at = 0.0

    @property
    def stale_after_seconds(self) -> float:
        return self._settings.handler_timeout_seconds + _RECLAIM_INTERVAL_SECONDS

    async def run(self) -> None:
        consumers = [
            asyncio.create_task(
                self._consume(f"{self._worker_id}-{index}"),
                name=f"github-ingress-consumer-{self._worker_id}-{index}",
            )
            for index in range(self._settings.local_concurrency)
        ]
        maintenance = asyncio.create_task(
            self._maintain(), name=f"github-ingress-maintenance-{self._worker_id}"
        )
        try:
            await asyncio.gather(maintenance, *consumers)
        finally:
            for task in (maintenance, *consumers):
                task.cancel()
            await asyncio.gather(maintenance, *consumers, return_exceptions=True)

    async def _maintain(self) -> None:
        last_reap = 0.0
        while not self._stop.is_set():
            try:
                await self._queue.promote_due_retries()
                now = time.monotonic()
                if now - last_reap >= _SLOT_REAP_INTERVAL_SECONDS:
                    last_reap = now
                    await self._queue.reap_stale_slots(older_than_seconds=self.stale_after_seconds)
            except IngressUnavailable:
                logger.warning("GitHub ingress maintenance iteration failed", exc_info=True)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("GitHub ingress maintenance iteration failed")
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop.wait(), timeout=_MAINTENANCE_INTERVAL_SECONDS
                )

    async def process_one(self, consumer: str) -> bool:
        """Run at most one ready message; returns whether one ran."""
        token = uuid.uuid4().hex
        if not await self._queue.acquire_slot(token):
            return False
        try:
            message = await self._queue.read(consumer, block_ms=_READ_BLOCK_MS)
            if message is None:
                return False
            await self._execute(message)
            return True
        finally:
            await self._queue.release_slot(token)

    async def _consume(self, consumer: str) -> None:
        while not self._stop.is_set():
            await self._maybe_reclaim(consumer)
            try:
                ran = await self.process_one(consumer)
            except IngressUnavailable:
                logger.warning("GitHub ingress consumer poll failed", exc_info=True)
                await self._sleep(1.0)
                continue
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("GitHub ingress consumer iteration failed")
                await self._sleep(1.0)
                continue
            if not ran:
                await self._sleep(0.2)

    async def _maybe_reclaim(self, consumer: str) -> None:
        now = time.monotonic()
        if now < self._next_reclaim_at:
            return
        self._next_reclaim_at = now + _RECLAIM_INTERVAL_SECONDS
        token = uuid.uuid4().hex
        try:
            if not await self._queue.acquire_slot(token):
                return
        except IngressUnavailable:
            logger.warning("Could not reserve a slot to reclaim abandoned deliveries", exc_info=True)
            return
        try:
            reclaimed = await self._queue.reclaim_stale(
                consumer, min_idle_ms=int(self.stale_after_seconds * 1000)
            )
            for message in reclaimed:
                await self._execute(message)
        except IngressUnavailable:
            logger.warning("Could not reclaim abandoned GitHub deliveries", exc_info=True)
        finally:
            await self._queue.release_slot(token)

    async def _execute(self, message: QueuedMessage) -> None:
        job = message.job
        handler = self._handlers.get(job.handler)
        if handler is None:
            await self._dead_letter(message, f"unknown handler {job.handler!r}")
            return

        heartbeat = asyncio.create_task(self._heartbeat(job))
        try:
            await asyncio.wait_for(
                handler(job.payload, job.event_type, job.delivery_id),
                timeout=self._settings.handler_timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            await self._fail(message, exc)
        else:
            await self._queue.ack(message.message_id)
            logger.info(
                "Handled a GitHub ingress delivery",
                extra={
                    "github_delivery": job.delivery_id,
                    "github_handler": job.handler,
                    "github_event": job.event_type,
                },
            )
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

    async def _heartbeat(self, job: IngressJob) -> None:
        interval = max(1.0, self._settings.handler_timeout_seconds / 3)
        while True:
            await self._queue.heartbeat(job)
            await asyncio.sleep(interval)

    async def _fail(self, message: QueuedMessage, exc: BaseException) -> None:
        failed = message.job.next_attempt()
        error = f"{type(exc).__name__}: {exc}"
        extra = {
            "github_delivery": failed.delivery_id,
            "github_handler": failed.handler,
            "github_attempt": failed.attempt,
        }
        if failed.attempt >= self._settings.max_attempts:
            await self._dead_letter(message, error)
            return
        delay = retry_delay(
            failed.attempt,
            base_seconds=self._settings.retry_base_seconds,
            max_seconds=self._settings.retry_max_seconds,
        )
        await self._queue.schedule_retry(failed, delay)
        await self._queue.ack(message.message_id)
        logger.warning(
            "GitHub ingress handler failed; retry scheduled",
            extra={**extra, "retry_in_seconds": round(delay, 1), "error": error},
        )

    async def _dead_letter(self, message: QueuedMessage, error: str) -> None:
        failed = message.job.next_attempt()
        await self._queue.dead_letter(failed, error)
        await self._queue.ack(message.message_id)
        logger.error(
            "Dead-lettered a GitHub ingress delivery",
            extra={
                "github_delivery": failed.delivery_id,
                "github_handler": failed.handler,
                "github_attempt": failed.attempt,
                "error": error,
            },
        )

    async def _sleep(self, seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
