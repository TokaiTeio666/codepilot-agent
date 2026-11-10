"""Job envelope carried through the GitHub ingress stream."""

import json
import uuid
from dataclasses import dataclass, replace
from typing import Literal, cast, get_args

from agent.utils.json_types import JsonObject

# One key per webhook handler the queue may run. Kept as a closed set so a
# corrupted or forged message can never name an arbitrary coroutine.
type HandlerKey = Literal[
    "github_issue",
    "github_issue_automations",
    "github_pr_comment",
    "github_pr_ready",
    "github_pr_close",
    "github_push",
    "github_ci",
    "github_expedited_review",
    "github_review_finding_reply",
]

type SubmitOutcome = Literal["queued", "duplicate", "disabled"]

_JOB_ID_FIELD = "job_id"
_HANDLER_FIELD = "handler"
_EVENT_TYPE_FIELD = "event_type"
_DELIVERY_ID_FIELD = "delivery_id"
_ATTEMPT_FIELD = "attempt"
_PAYLOAD_FIELD = "payload"


@dataclass(frozen=True)
class IngressJob:
    """A webhook delivery waiting for a handler."""

    job_id: str
    handler: HandlerKey
    event_type: str
    delivery_id: str
    attempt: int
    payload: JsonObject

    @classmethod
    def new(
        cls,
        *,
        handler: HandlerKey,
        event_type: str,
        delivery_id: str,
        payload: JsonObject,
    ) -> IngressJob:
        return cls(
            job_id=uuid.uuid4().hex,
            handler=handler,
            event_type=event_type,
            delivery_id=delivery_id,
            attempt=0,
            payload=payload,
        )

    def next_attempt(self) -> IngressJob:
        return replace(self, attempt=self.attempt + 1)

    def encode(self) -> str:
        return json.dumps(
            {
                _JOB_ID_FIELD: self.job_id,
                _HANDLER_FIELD: self.handler,
                _EVENT_TYPE_FIELD: self.event_type,
                _DELIVERY_ID_FIELD: self.delivery_id,
                _ATTEMPT_FIELD: self.attempt,
                _PAYLOAD_FIELD: self.payload,
            },
            separators=(",", ":"),
        )

    @classmethod
    def decode(cls, raw: str) -> IngressJob | None:
        """Parse a message, returning None when it is not a valid job envelope.

        ``None`` is the dead-letter signal: a malformed message is never
        retried, because retrying cannot change its shape.
        """
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(value, dict):
            return None

        job_id = value.get(_JOB_ID_FIELD)
        handler = value.get(_HANDLER_FIELD)
        event_type = value.get(_EVENT_TYPE_FIELD)
        delivery_id = value.get(_DELIVERY_ID_FIELD)
        attempt = value.get(_ATTEMPT_FIELD)
        payload = value.get(_PAYLOAD_FIELD)
        if (
            not isinstance(job_id, str)
            or not isinstance(handler, str)
            or handler not in _HANDLER_KEYS
            or not isinstance(event_type, str)
            or not isinstance(delivery_id, str)
            or not isinstance(attempt, int)
            or attempt < 0
            or not isinstance(payload, dict)
        ):
            return None
        return cls(
            job_id=job_id,
            handler=cast(HandlerKey, handler),
            event_type=event_type,
            delivery_id=delivery_id,
            attempt=attempt,
            payload=cast(JsonObject, payload),
        )


# ``get_args`` on a PEP 695 alias returns () on this runtime; unwrap the alias
# it wraps to reach the Literal's members.
_HANDLER_KEYS: frozenset[str] = frozenset(
    key for key in get_args(getattr(HandlerKey, "__value__", HandlerKey)) if isinstance(key, str)
)
