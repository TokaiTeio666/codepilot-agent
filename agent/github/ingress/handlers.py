"""The closed set of webhook handlers the ingress worker may run.

Imports are lazy on purpose: this module is imported by the worker bootstrap,
while each handler pulls in the whole webhook service stack. Resolving them at
call time keeps the import graph acyclic and keeps a process that only enqueues
(no worker) from loading sandbox and reviewer code.
"""

from collections.abc import Awaitable, Callable, Mapping

from agent.utils.json_types import JsonObject

type IngressHandler = Callable[[JsonObject, str, str], Awaitable[None]]


async def _github_issue(payload: JsonObject, event_type: str, _delivery_id: str) -> None:
    from agent.github import webhook

    await webhook.process_github_issue(payload, event_type)


async def _github_issue_automations(
    payload: JsonObject, _event_type: str, delivery_id: str
) -> None:
    from agent.schedules import store as schedules

    await schedules.launch_github_issue_automations(payload, delivery_id)


async def _github_pr_comment(payload: JsonObject, event_type: str, _delivery_id: str) -> None:
    from agent.github import webhook

    await webhook.process_github_pr_comment(payload, event_type)


async def _github_pr_ready(payload: JsonObject, _event_type: str, _delivery_id: str) -> None:
    from agent.github import webhook

    await webhook.process_github_pr_ready(payload)


async def _github_pr_close(payload: JsonObject, _event_type: str, _delivery_id: str) -> None:
    from agent.github import webhook

    await webhook.process_github_pr_close(payload)


async def _github_push(payload: JsonObject, _event_type: str, _delivery_id: str) -> None:
    from agent.github import webhook

    await webhook.process_github_push_event(payload)


async def _github_ci(payload: JsonObject, event_type: str, delivery_id: str) -> None:
    from agent.github import webhook

    await webhook.process_github_ci_event(payload, event_type, delivery_id)


async def _github_expedited_review(
    payload: JsonObject, event_type: str, _delivery_id: str
) -> None:
    from agent.github import webhook

    await webhook.process_expedited_review_event(payload, event_type)


async def _github_review_finding_reply(
    payload: JsonObject, _event_type: str, _delivery_id: str
) -> None:
    from agent.github import webhook

    await webhook.process_github_review_finding_reply(payload)


HANDLERS: Mapping[str, IngressHandler] = {
    "github_issue": _github_issue,
    "github_issue_automations": _github_issue_automations,
    "github_pr_comment": _github_pr_comment,
    "github_pr_ready": _github_pr_ready,
    "github_pr_close": _github_pr_close,
    "github_push": _github_push,
    "github_ci": _github_ci,
    "github_expedited_review": _github_expedited_review,
    "github_review_finding_reply": _github_review_finding_reply,
}
