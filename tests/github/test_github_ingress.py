import asyncio
from dataclasses import replace
from typing import cast

from fakeredis.aioredis import FakeRedis

from agent.github import routes as github_routes
from agent.github.ingress import service as ingress_service
from agent.github.ingress.config import IngressSettings
from agent.github.ingress.models import HandlerKey, IngressJob, SubmitOutcome
from agent.github.ingress.queue import DeliveryLedger, GithubIngressQueue
from agent.github.ingress.worker import GithubIngressWorker
from agent.utils.json_types import JsonObject
from agent.webhooks import common as webhook_common
from tests.conftest import post_signed_github_webhook

_TEST_WEBHOOK_SECRET = "ingress-test-secret"


def _settings(**overrides: object) -> IngressSettings:
    base = IngressSettings(
        enabled=True,
        worker_enabled=False,
        redis_url="redis://test/0",
        stream="test:stream",
        group="test-group",
        concurrency=2,
        worker_count=1,
        local_concurrency=1,
        max_attempts=2,
        retry_base_seconds=0.0,
        retry_max_seconds=0.0,
        handler_timeout_seconds=1.0,
        delivery_ttl_seconds=60,
    )
    return replace(base, **overrides)


def _job(handler: HandlerKey = "github_issue") -> IngressJob:
    return IngressJob.new(
        handler=handler,
        event_type="issues",
        delivery_id="delivery-1",
        payload={"action": "reopened"},
    )


async def test_delivery_ledger_deduplicates_per_handler() -> None:
    redis = FakeRedis(decode_responses=True)
    ledger = DeliveryLedger(redis, _settings())

    assert await ledger.claim("delivery-1", "github_issue") is True
    assert await ledger.claim("delivery-1", "github_issue") is False
    # One delivery may trigger more than one handler; only the repeated handler
    # is a duplicate.
    assert await ledger.claim("delivery-1", "github_pr_comment") is True

    await ledger.release("delivery-1", "github_issue")
    assert await ledger.claim("delivery-1", "github_issue") is True


async def test_concurrency_slots_are_capped() -> None:
    redis = FakeRedis(decode_responses=True)
    queue = GithubIngressQueue(redis, _settings(concurrency=2))

    assert await queue.acquire_slot("a") is True
    assert await queue.acquire_slot("b") is True
    assert await queue.acquire_slot("c") is False

    await queue.release_slot("a")
    assert await queue.acquire_slot("c") is True

    assert await queue.reap_stale_slots(older_than_seconds=-1) == 2
    assert await queue.acquire_slot("d") is True


async def test_invalid_job_envelope_is_rejected() -> None:
    assert IngressJob.decode("not json") is None
    assert IngressJob.decode('{"handler": "not-a-handler"}') is None


async def test_worker_acks_a_successful_delivery() -> None:
    redis = FakeRedis(decode_responses=True)
    settings = _settings()
    queue = GithubIngressQueue(redis, settings)
    await queue.ensure_group()

    calls: list[str] = []

    async def handler(payload: JsonObject, event_type: str, delivery_id: str) -> None:
        calls.append(delivery_id)

    worker = GithubIngressWorker(
        queue,
        settings,
        worker_id="w",
        stop=asyncio.Event(),
        handlers={"github_issue": handler},
    )
    await queue.enqueue(_job())

    assert await worker.process_one("consumer-1") is True
    assert calls == ["delivery-1"]
    pending = await redis.xpending(settings.stream, settings.group)
    assert pending["pending"] == 0
    assert await redis.zcard(settings.delayed_key) == 0
    assert await redis.xlen(settings.dead_stream) == 0


async def test_worker_retries_then_dead_letters() -> None:
    redis = FakeRedis(decode_responses=True)
    settings = _settings(max_attempts=2)
    queue = GithubIngressQueue(redis, settings)
    await queue.ensure_group()

    attempts: list[int] = []

    async def handler(payload: JsonObject, event_type: str, delivery_id: str) -> None:
        attempts.append(len(attempts) + 1)
        raise RuntimeError("handler exploded")

    worker = GithubIngressWorker(
        queue,
        settings,
        worker_id="w",
        stop=asyncio.Event(),
        handlers={"github_issue": handler},
    )
    await queue.enqueue(_job())

    # First failure schedules a retry instead of dead-lettering.
    assert await worker.process_one("consumer-1") is True
    assert len(attempts) == 1
    assert await redis.zcard(settings.delayed_key) == 1
    assert await redis.xlen(settings.dead_stream) == 0

    # The retry is due immediately (zero base delay) and exhausts the budget.
    assert await queue.promote_due_retries() == 1
    assert await worker.process_one("consumer-1") is True
    assert len(attempts) == 2
    assert await redis.xlen(settings.dead_stream) == 1
    pending = await redis.xpending(settings.stream, settings.group)
    assert pending["pending"] == 0


async def test_webhook_queues_and_deduplicates_deliveries(monkeypatch) -> None:
    submitted: list[tuple[str, str]] = []
    outcomes = iter(["queued", "duplicate"])

    async def routable(owner: str, name: str) -> bool:
        return True

    async def allow(*args: object, **kwargs: object) -> None:
        return None

    async def fake_submit(
        handler: HandlerKey,
        *,
        event_type: str,
        delivery_id: str,
        payload: JsonObject,
    ) -> SubmitOutcome:
        submitted.append((handler, delivery_id))
        return cast(SubmitOutcome, next(outcomes))

    async def inline_handler(*args: object, **kwargs: object) -> None:
        raise AssertionError("handler must not run in the web process when queued")

    monkeypatch.setattr(ingress_service, "submit", fake_submit)
    monkeypatch.setattr(github_routes.service, "process_github_issue", inline_handler)
    monkeypatch.setattr(github_routes, "repo_is_routable", routable)
    monkeypatch.setattr(webhook_common, "enforce_public_repo_org_gate", allow)
    monkeypatch.setattr(webhook_common, "GITHUB_WEBHOOK_SECRET", _TEST_WEBHOOK_SECRET)

    payload = {
        "action": "reopened",
        "issue": {
            "id": 12345,
            "number": 42,
            "title": "@openswe fix the flaky test",
            "body": "The test is failing intermittently.",
        },
        "repository": {"owner": {"login": "langchain-ai"}, "name": "open-swe"},
        "sender": {"login": "octocat"},
    }

    first = await post_signed_github_webhook(
        "issues", payload, secret=_TEST_WEBHOOK_SECRET, delivery_id="delivery-1"
    )
    assert first.json() == {"status": "accepted", "message": "Queued for processing"}

    second = await post_signed_github_webhook(
        "issues", payload, secret=_TEST_WEBHOOK_SECRET, delivery_id="delivery-1"
    )
    assert second.json() == {"status": "ignored", "reason": "duplicate delivery"}
    assert submitted == [("github_issue", "delivery-1"), ("github_issue", "delivery-1")]
