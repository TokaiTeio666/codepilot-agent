# GitHub pipeline

How a GitHub event becomes an agent run, and where every stage can fail.

```
GitHub
  │ POST /webhooks/github   (raw body + X-Hub-Signature-256)
  ▼
agent/github/routes.py
  │ 1. HMAC-SHA256 verification            → 401 on mismatch
  │ 2. event/action/mention/allowlist gates → 200 "ignored"
  │ 3. repo → workspace routing             → 503 so GitHub retries
  │ 4. delivery id claim (Redis SET NX EX)  → 200 "duplicate"
  │ 5. XADD job                             → 202/200 immediately
  ▼
Redis Stream `github:ingress:stream`
  │ consumer group `github-ingress`, one PEL entry per delivery
  ▼
agent/github/ingress/worker.py
  │ 6. global slot (ZSET, ceiling 20) + local consumers
  │ 7. handler runs outside the request
  │ 8. success → XACK
  │    failure → ZADD delayed (30s, ×2, cap 10m, jitter) → back to the stream
  │    attempts exhausted → XADD dead stream
  ▼
agent/github/webhook.py
  │ 9. thread id, prompt, durable LangGraph run, PR comment writeback
  ▼
agent/github/pull_requests.py + comments.py
```

The queue is opt-in. With neither `GITHUB_INGRESS_QUEUE_ENABLED` nor
`GITHUB_INGRESS_WORKER_ENABLED` set, the route keeps its original behavior:
`background_tasks.add_task(...)` in the web process. Enable the queue when the
web process must answer GitHub in milliseconds and agent runs must survive a
web restart.

## 1. Signature verification

`agent/github/comments.py::verify_github_signature` recomputes
`sha256=<hmac(secret, raw_body)>` and compares with `hmac.compare_digest`. The
route reads `request.body()` before parsing: JSON-decoding first would change
the bytes and invalidate every signature. An unset secret rejects the request
(fail closed), it does not skip verification.

## 2. Gates before work

The route rejects cheaply and in this order:

| Gate | Rejection |
| --- | --- |
| unsupported `X-GitHub-Event` | `200 ignored` |
| repository belongs to no workspace | `200 ignored` (retryable lookup errors are `503`) |
| repository not in `ALLOWED_GITHUB_REPOS` | `200 ignored` |
| public repo / sender outside the org gate | `200 ignored` |
| event or comment does not mention a tag in `OPEN_SWE_MENTION_TAGS` | `200 ignored` |
| `pull_request` action not supported | `200 ignored` |

`503` is reserved for cases where dropping the delivery would drop real work
(workspace ownership unreadable, queue unavailable). GitHub retries 5xx, not 2xx
— an `ignored` answer must mean "this event can never produce work".

## 3. Delivery idempotency

GitHub delivers at least once. `DeliveryLedger` reserves
`<stream>:delivery:<delivery-id>:<handler>` with `SET NX EX`
(`GITHUB_INGRESS_DELIVERY_TTL_SECONDS`, default 24h).

The handler is part of the key on purpose: one delivery can legitimately
trigger two handlers (an expedited-review watch and the PR comment that followed
it). Deduplicating the whole delivery would silently drop the second. The claim
is released if the `XADD` fails, so a Redis hiccup does not consume the id and
the redelivery still does work.

## 4. Queue structures

| Key | Type | Purpose |
| --- | --- | --- |
| `github:ingress:stream` | Stream | ready work |
| `github:ingress:stream:delayed` | ZSET | retries waiting for backoff (score = due epoch) |
| `github:ingress:stream:dead` | Stream | attempts exhausted, needs a human |
| `github:ingress:stream:running` | ZSET | global concurrency slots (score = start epoch) |
| `github:ingress:stream:heartbeat:<job>` | String TTL | job is alive |
| `github:ingress:stream:delivery:<delivery>:<handler>` | String TTL | idempotency claim |

A Stream rather than a List because the consumer group supplies the pending
entries list (PEL), `XACK` and `XAUTOCLAIM`. `BRPOP` loses a message when the
worker dies between pop and completion, and rebuilding that guarantee by hand is
just reimplementing the Stream.

Job envelope (`agent/github/ingress/models.py`):

```json
{
  "job_id": "…",
  "handler": "github_issue",
  "event_type": "issues",
  "delivery_id": "…",
  "attempt": 0,
  "payload": { "…": "raw webhook payload" }
}
```

`handler` is validated against a closed `HandlerKey` set, so a malformed or
forged entry can never name an arbitrary coroutine. A payload that fails
validation is acknowledged and discarded — retrying cannot change its shape.

## 5. Concurrency

Two limits:

- **local**: `GITHUB_INGRESS_WORKER_COUNT` × `GITHUB_INGRESS_LOCAL_CONCURRENCY`
  consumer coroutines in one process (defaults 2 × 10 = 20).
- **global**: `GITHUB_INGRESS_CONCURRENCY` (default 20) slots in a Redis ZSET,
  so two replicas cannot overshoot the ceiling together.

A slot is taken with `WATCH` + `MULTI`: read `ZCARD`, and only when it is below
the ceiling add this worker's token. The slot is released in a `finally`; a
worker that dies leaks its slot, and `reap_stale_slots` removes entries older
than the handler timeout plus the reclaim interval.

A consumer takes a slot before reading, so a message never sits unowned while it
waits for capacity. If no slot is free the consumer sleeps and retries instead
of piling up in-flight messages.

## 6. Retries and abandonment

```
attempt 1 fails → delayed +30s (±20% jitter)
attempt 2 fails → delayed +60s
attempt 3 fails → dead stream (max_attempts default 3)
```

- `GITHUB_INGRESS_RETRY_BASE_SECONDS` and `_MAX_SECONDS` bound the exponential
  delay; jitter keeps a GitHub outage from retrying every delivery in lockstep.
- Only handler exceptions are retried. Signature failures and gate rejections
  never enter the queue.
- A due retry is promoted by `ZREM` then `XADD`; if the `XADD` fails the entry
  is put back, so a delivery is not lost between the two structures.
- Acknowledgements happen after the state transition (ack, schedule-retry, or
  dead-letter). A crash between the two can at worst redeliver — at-least-once,
  which is why handlers must tolerate a repeated delivery.
- Every consumer calls `XAUTOCLAIM` for entries idle longer than the handler
  timeout plus a minute, so a killed worker's in-flight jobs come back.
- A heartbeat key is refreshed every third of the handler timeout; it is
  diagnostic, not the reclaim trigger.

Timeouts are layered: the per-attempt `asyncio.wait_for` is
`GITHUB_INGRESS_HANDLER_TIMEOUT_SECONDS` (default 10m), while the agent run's
own budget is enforced by LangGraph after the run is created.

## 7. Handler registry

`agent/github/ingress/handlers.py` maps each `HandlerKey` to a coroutine and
imports the webhook service lazily. That keeps a web process that only enqueues
from loading sandbox/reviewer code, and avoids an import cycle with
`agent.github.webhook`.

| Handler | Entry point |
| --- | --- |
| `github_issue` | `webhook.process_github_issue` |
| `github_issue_automations` | `schedules.launch_github_issue_automations` |
| `github_pr_comment` | `webhook.process_github_pr_comment` |
| `github_pr_ready` | `webhook.process_github_pr_ready` |
| `github_pr_close` | `webhook.process_github_pr_close` |
| `github_push` | `webhook.process_github_push_event` |
| `github_ci` | `webhook.process_github_ci_event` |
| `github_expedited_review` | `webhook.process_expedited_review_event` |
| `github_review_finding_reply` | `webhook.process_github_review_finding_reply` |

## 8. GitHub App credentials

`agent/github/app.py` implements the three-layer credential:

1. Sign a ≤10-minute RS256 JWT (`iss` = App ID, `iat` 60s in the past for clock
   skew).
2. Exchange it for an installation token, optionally restricted with
   `repository_ids` / `repositories` and `permissions`. Those can only narrow
   the App's own permissions, never widen them.
3. Cache the token in-process until 10 minutes before expiry, keyed by
   installation + repository scope + permission set.

`GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY` and `GITHUB_APP_INSTALLATION_ID` are
the minimum configuration. The private key is never written to disk by the
agent; tokens are only ever held in memory.

## 9. PR writeback

Handlers end in `agent/github/pull_requests.py` (branch/PR creation) and
`agent/github/comments.py` (comment/react/update). Results are written back to
the issue or PR that triggered the run; `agent/thread_ids.py` derives a stable
thread id per issue/PR so a follow-up comment joins the same conversation.

## Failure matrix

| Failure | What happens |
| --- | --- |
| bad signature | `401`, nothing queued |
| Redis unreachable while enqueueing | claim released, `503`, GitHub retries |
| duplicate delivery | `200 ignored`, handler not run twice |
| worker crashes mid-handler | PEL entry reclaimed after timeout; job retried |
| handler raises | delayed retry with jitter, then dead letter |
| handler exceeds its timeout | treated as a failure; sandbox/run cleanup is the handler's job |
| malformed job envelope | acked and logged, never retried |
| dead stream non-empty | `XLEN` and log `Dead-lettered a GitHub ingress delivery` |

## Configuration

| Variable | Default | Effect |
| --- | --- | --- |
| `GITHUB_INGRESS_QUEUE_ENABLED` | off | route enqueues instead of running inline |
| `GITHUB_INGRESS_WORKER_ENABLED` | off | this process consumes the stream |
| `GITHUB_INGRESS_REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `GITHUB_INGRESS_STREAM` | `github:ingress:stream` | stream/key prefix |
| `GITHUB_INGRESS_GROUP` | `github-ingress` | consumer group |
| `GITHUB_INGRESS_CONCURRENCY` | `20` | global slots |
| `GITHUB_INGRESS_WORKER_COUNT` | `2` | worker loops per process |
| `GITHUB_INGRESS_LOCAL_CONCURRENCY` | `10` | consumers per loop |
| `GITHUB_INGRESS_MAX_ATTEMPTS` | `3` | attempts before dead-lettering |
| `GITHUB_INGRESS_RETRY_BASE_SECONDS` | `30` | first retry delay |
| `GITHUB_INGRESS_RETRY_MAX_SECONDS` | `600` | delay ceiling |
| `GITHUB_INGRESS_HANDLER_TIMEOUT_SECONDS` | `600` | one attempt's deadline |
| `GITHUB_INGRESS_DELIVERY_TTL_SECONDS` | `86400` | idempotency window |

## Operating it

- Watch `xpending` for the group: a rising pending count means handlers are
  slower than the arrival rate or a consumer keeps dying.
- Watch `xlen` on the dead stream: it should stay at zero; entries there need a
  replay decision, they are never retried automatically.
- `ZRANGE github:ingress:stream:running 0 -1 WITHSCORES` shows in-flight jobs.
- Logs carry `github_delivery`, `github_handler` and `github_attempt` on every
  stage, so one delivery can be followed end to end by `extra`.
