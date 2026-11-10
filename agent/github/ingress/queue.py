"""Redis Stream transport for GitHub webhook deliveries.

Three structures carry a delivery's whole life:

- the stream, which holds work that is ready to run;
- a delayed sorted set, which holds retries until their backoff elapses;
- a dead stream, which holds deliveries that exhausted their attempts.

A delivery id claim in Redis (``SET NX EX``) makes GitHub's at-least-once
redelivery safe: a duplicate never reaches a handler.
"""

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from agent.github.ingress.config import IngressSettings
from agent.github.ingress.models import IngressJob

logger = logging.getLogger(__name__)

_JOB_FIELD = "job"
_ERROR_FIELD = "error"
_DEAD_AT_FIELD = "dead_at"


class IngressUnavailable(RuntimeError):
    """The ingress queue cannot accept or deliver work right now."""


@dataclass(frozen=True)
class QueuedMessage:
    """A job plus the stream entry that must be acknowledged after it runs."""

    message_id: str
    job: IngressJob


def _text(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return None


def _text_list(value: object) -> list[str]:
    """Normalize a reply whose element type Redis types only as ``bytes | str``."""
    if not isinstance(value, (list, tuple)):
        return []
    return [text for item in value if (text := _text(item)) is not None]


def _stream_entries(value: object) -> list[tuple[str, dict[str, str]]]:
    """Normalize stream entries, dropping shapes this queue cannot decode."""
    if not isinstance(value, (list, tuple)):
        return []
    entries: list[tuple[str, dict[str, str]]] = []
    for entry in value:
        if not isinstance(entry, (list, tuple)) or len(entry) != 2:
            continue
        message_id, raw_fields = entry
        if not isinstance(message_id, str) or not isinstance(raw_fields, dict):
            continue
        fields = {
            key: text
            for key, value in raw_fields.items()
            if isinstance(key, str) and (text := _text(value)) is not None
        }
        entries.append((message_id, fields))
    return entries


def _decode_message(fields: dict[str, str]) -> IngressJob | None:
    raw = fields.get(_JOB_FIELD)
    return IngressJob.decode(raw) if isinstance(raw, str) else None


class DeliveryLedger:
    """Deduplicates ``X-GitHub-Delivery`` ids for the delivery TTL."""

    def __init__(self, redis: Redis, settings: IngressSettings) -> None:
        self._redis = redis
        self._settings = settings

    def _key(self, delivery_id: str, handler: str) -> str:
        # Keyed by handler as well: one delivery can legitimately trigger more
        # than one handler (an expedited-review watch and the PR comment that
        # followed it), and deduplicating the whole delivery would drop the
        # second one.
        return f"{self._settings.delivery_prefix}{delivery_id}:{handler}"

    async def claim(self, delivery_id: str, handler: str) -> bool:
        """Reserve a delivery id, returning False when it was already seen."""
        if not delivery_id:
            return True
        try:
            claimed = await self._redis.set(
                self._key(delivery_id, handler),
                "1",
                nx=True,
                ex=self._settings.delivery_ttl_seconds,
            )
        except RedisError as exc:
            raise IngressUnavailable("could not reserve the GitHub delivery id") from exc
        return bool(claimed)

    async def release(self, delivery_id: str, handler: str) -> None:
        """Release a reservation whose enqueue failed, so GitHub's retry fits."""
        if not delivery_id:
            return
        try:
            await self._redis.delete(self._key(delivery_id, handler))
        except RedisError:
            logger.warning(
                "Could not release an unqueued GitHub delivery claim",
                extra={"github_delivery": delivery_id, "github_handler": handler},
                exc_info=True,
            )


class GithubIngressQueue:
    """Stream, retry and concurrency primitives over one Redis connection."""

    def __init__(self, redis: Redis, settings: IngressSettings) -> None:
        self._redis = redis
        self._settings = settings

    async def ensure_group(self) -> None:
        """Create the consumer group; a pre-existing group is fine."""
        try:
            await self._redis.xgroup_create(
                self._settings.stream, self._settings.group, id="0", mkstream=True
            )
        except RedisError as exc:
            if "BUSYGROUP" not in str(exc):
                raise IngressUnavailable("could not create the GitHub ingress group") from exc

    async def enqueue(self, job: IngressJob) -> None:
        try:
            await self._redis.xadd(self._settings.stream, {_JOB_FIELD: job.encode()})
        except RedisError as exc:
            raise IngressUnavailable("could not append to the GitHub ingress stream") from exc

    async def read(self, consumer: str, *, block_ms: int) -> QueuedMessage | None:
        """Read one never-delivered entry; returns None on an empty poll."""
        try:
            response = await self._redis.xreadgroup(
                self._settings.group,
                consumer,
                {self._settings.stream: ">"},
                count=1,
                block=block_ms,
            )
        except RedisError as exc:
            raise IngressUnavailable("could not read the GitHub ingress stream") from exc
        for stream_reply in response or []:
            if not isinstance(stream_reply, (list, tuple)) or len(stream_reply) != 2:
                continue
            for message_id, fields in _stream_entries(stream_reply[1]):
                job = _decode_message(fields)
                if job is None:
                    await self.ack(message_id)
                    logger.error(
                        "Discarded a malformed GitHub ingress message",
                        extra={"redis_message_id": message_id},
                    )
                    continue
                return QueuedMessage(message_id=message_id, job=job)
        return None

    async def reclaim_stale(self, consumer: str, *, min_idle_ms: int) -> list[QueuedMessage]:
        """Take over entries whose consumer stopped heartbeating."""
        try:
            response = await self._redis.xautoclaim(
                self._settings.stream,
                self._settings.group,
                consumer,
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=1,
            )
        except RedisError as exc:
            raise IngressUnavailable("could not reclaim the GitHub ingress stream") from exc
        reclaimed: list[QueuedMessage] = []
        entries = response[1] if len(response) > 1 else None
        for message_id, fields in _stream_entries(entries):
            job = _decode_message(fields)
            if job is None:
                await self.ack(message_id)
                logger.error(
                    "Discarded a malformed reclaimed GitHub ingress message",
                    extra={"redis_message_id": message_id},
                )
                continue
            logger.warning(
                "Reclaimed an abandoned GitHub ingress delivery",
                extra={"github_delivery": job.delivery_id, "redis_message_id": message_id},
            )
            reclaimed.append(QueuedMessage(message_id=message_id, job=job))
        return reclaimed

    async def ack(self, message_id: str) -> None:
        try:
            await self._redis.xack(self._settings.stream, self._settings.group, message_id)
        except RedisError as exc:
            raise IngressUnavailable("could not acknowledge a GitHub ingress message") from exc

    async def schedule_retry(self, job: IngressJob, delay_seconds: float) -> None:
        score = time.time() + delay_seconds
        try:
            await self._redis.zadd(self._settings.delayed_key, {job.encode(): score})
        except RedisError as exc:
            raise IngressUnavailable("could not schedule a GitHub ingress retry") from exc

    async def promote_due_retries(self, *, limit: int = 50) -> int:
        """Move due retries back onto the stream; returns how many moved."""
        due_before = time.time()
        try:
            due = await self._redis.zrangebyscore(
                self._settings.delayed_key, "-inf", due_before, start=0, num=limit
            )
        except RedisError as exc:
            raise IngressUnavailable("could not read the GitHub ingress retry set") from exc

        promoted = 0
        for raw in _text_list(due):
            removed = await self._redis.zrem(self._settings.delayed_key, raw)
            if not removed:
                # Another worker promoted it first.
                continue
            job = IngressJob.decode(raw)
            if job is None:
                logger.error("Discarded a malformed GitHub ingress retry entry")
                continue
            try:
                await self._redis.xadd(self._settings.stream, {_JOB_FIELD: raw})
            except RedisError:
                # Put it back rather than losing a delivery that was already
                # removed from the retry set.
                await self._redis.zadd(self._settings.delayed_key, {raw: time.time() + 5})
                logger.warning(
                    "Could not re-enqueue a due GitHub retry; rescheduled",
                    extra={"github_delivery": job.delivery_id},
                    exc_info=True,
                )
                continue
            promoted += 1
        return promoted

    async def dead_letter(self, job: IngressJob, error: str) -> None:
        try:
            await self._redis.xadd(
                self._settings.dead_stream,
                {
                    _JOB_FIELD: job.encode(),
                    _ERROR_FIELD: error,
                    _DEAD_AT_FIELD: datetime.now(UTC).isoformat(),
                },
            )
        except RedisError as exc:
            raise IngressUnavailable("could not dead-letter a GitHub ingress job") from exc

    async def acquire_slot(self, token: str) -> bool:
        """Reserve one global concurrency slot, atomically against other workers.

        ``WATCH``/``MULTI`` keeps the count at the ceiling without a Lua script,
        and the sorted-set score lets a reaper free slots leaked by a crashed
        worker.
        """
        for _ in range(5):
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(self._settings.running_key)
                    running = await pipe.zcard(self._settings.running_key)
                    if running >= self._settings.concurrency:
                        await pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.zadd(self._settings.running_key, {token: time.time()})
                    await pipe.execute()
                    return True
            except WatchError:
                continue
            except RedisError as exc:
                raise IngressUnavailable("could not reserve a GitHub ingress slot") from exc
        return False

    async def release_slot(self, token: str) -> None:
        try:
            await self._redis.zrem(self._settings.running_key, token)
        except RedisError:
            logger.warning("Could not release a GitHub ingress concurrency slot", exc_info=True)

    async def reap_stale_slots(self, *, older_than_seconds: float) -> int:
        """Free slots whose holder died without releasing them."""
        cutoff = time.time() - older_than_seconds
        try:
            removed = await self._redis.zremrangebyscore(self._settings.running_key, "-inf", cutoff)
        except RedisError:
            logger.warning("Could not reap stale GitHub ingress slots", exc_info=True)
            return 0
        if removed:
            logger.warning(
                "Freed GitHub ingress slots leaked by an abandoned worker",
                extra={"slots": removed},
            )
        return int(removed)

    async def heartbeat(self, job: IngressJob) -> None:
        try:
            await self._redis.set(
                f"{self._settings.heartbeat_prefix}{job.job_id}",
                "1",
                ex=max(1, int(self._settings.handler_timeout_seconds)),
            )
        except RedisError:
            # A failed heartbeat must not fail the job it is watching.
            logger.warning(
                "Could not write a GitHub ingress heartbeat",
                extra={"github_delivery": job.delivery_id},
                exc_info=True,
            )
