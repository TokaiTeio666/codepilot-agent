"""Facade the webhook route and the app lifespan use for GitHub ingress.

``submit`` is the only path a web request takes: when the queue is enabled it
reserves the delivery id, appends the job and returns; the request never waits
for an agent to start. When it is disabled (the default) the route keeps its
in-process background task, so enabling the queue is opt-in.
"""

import asyncio
import logging
import os
import socket

from redis.asyncio import Redis

from agent.github.ingress.config import IngressSettings
from agent.github.ingress.models import HandlerKey, IngressJob, SubmitOutcome
from agent.github.ingress.queue import DeliveryLedger, GithubIngressQueue, IngressUnavailable
from agent.github.ingress.worker import GithubIngressWorker
from agent.utils.json_types import JsonObject

logger = logging.getLogger(__name__)

_CLIENTS: dict[str, Redis] = {}
_WORKERS: list[asyncio.Task[None]] = []
_STOP = asyncio.Event()
_SHUTDOWN_GRACE_SECONDS = 10.0


def _client(settings: IngressSettings) -> Redis:
    """Return the process-wide client for this URL, creating it on first use."""
    client = _CLIENTS.get(settings.redis_url)
    if client is None:
        client = Redis.from_url(settings.redis_url, decode_responses=True)
        _CLIENTS[settings.redis_url] = client
    return client


async def submit(
    handler: HandlerKey,
    *,
    event_type: str,
    delivery_id: str,
    payload: JsonObject,
) -> SubmitOutcome:
    """Queue a verified delivery, or report that the caller must run it inline."""
    settings = IngressSettings.from_env()
    if not settings.enabled:
        return "disabled"

    redis = _client(settings)
    ledger = DeliveryLedger(redis, settings)
    if not await ledger.claim(delivery_id, handler):
        logger.info(
            "Ignored a duplicate GitHub delivery",
            extra={"github_delivery": delivery_id, "github_handler": handler},
        )
        return "duplicate"

    job = IngressJob.new(
        handler=handler,
        event_type=event_type,
        delivery_id=delivery_id,
        payload=payload,
    )
    try:
        await GithubIngressQueue(redis, settings).enqueue(job)
    except IngressUnavailable:
        await ledger.release(delivery_id, handler)
        raise
    logger.info(
        "Queued a GitHub delivery",
        extra={
            "github_delivery": delivery_id,
            "github_handler": handler,
            "github_event": event_type,
        },
    )
    return "queued"


async def start_worker() -> None:
    """Start the configured consumers; a no-op unless the worker flag is on."""
    if _WORKERS:
        return
    settings = IngressSettings.from_env()
    if not settings.worker_enabled:
        return

    redis = _client(settings)
    queue = GithubIngressQueue(redis, settings)
    await queue.ensure_group()
    _STOP.clear()
    identity = f"{socket.gethostname()}-{os.getpid()}"
    for index in range(settings.worker_count):
        worker = GithubIngressWorker(
            queue,
            settings,
            worker_id=f"{identity}-{index}",
            stop=_STOP,
        )
        _WORKERS.append(
            asyncio.create_task(worker.run(), name=f"github-ingress-worker-{index}")
        )
    logger.info(
        "Started GitHub ingress workers",
        extra={
            "github_workers": settings.worker_count,
            "github_local_concurrency": settings.local_concurrency,
            "github_concurrency": settings.concurrency,
        },
    )


async def stop_worker() -> None:
    """Stop consumers and close Redis clients this process opened."""
    _STOP.set()
    if _WORKERS:
        done, pending = await asyncio.wait(_WORKERS, timeout=_SHUTDOWN_GRACE_SECONDS)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            if task.cancelled():
                continue
            error = task.exception()
            if error is not None:
                logger.warning(
                    "GitHub ingress worker stopped with an error",
                    exc_info=(type(error), error, error.__traceback__),
                )
        _WORKERS.clear()

    for client in _CLIENTS.values():
        await client.aclose()
    _CLIENTS.clear()


def reset_for_tests() -> None:
    """Forget process-wide state between tests."""
    _WORKERS.clear()
    _CLIENTS.clear()
    _STOP.set()
