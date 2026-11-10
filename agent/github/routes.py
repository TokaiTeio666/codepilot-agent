"""Github webhook HTTP routes."""

from collections.abc import Awaitable, Callable
from typing import cast

from fastapi import APIRouter, Response

from agent.expedited_review.watch import WATCHED_GITHUB_EVENTS as EXPEDITED_REVIEW_EVENTS
from agent.github import webhook as service
from agent.github.ingress import service as ingress
from agent.github.ingress.models import HandlerKey
from agent.github.ingress.queue import IngressUnavailable
from agent.schedules import store as schedules
from agent.utils.json_types import JsonObject
from agent.webhooks import common
from agent.workspaces.routing import WorkspaceLookupError, repo_is_routable

router = APIRouter()

_QUEUED_RESPONSE: dict[str, str] = {"status": "accepted", "message": "Queued for processing"}
_DUPLICATE_RESPONSE: dict[str, str] = {
    "status": "ignored",
    "reason": "duplicate delivery",
}


async def _launch_issue_automations(payload: JsonObject, delivery_id: str) -> None:
    try:
        await schedules.launch_github_issue_automations(payload, delivery_id)
    except Exception:  # noqa: BLE001
        common.logger.exception(
            "Failed to launch GitHub issue automations",
            extra={"github_delivery": delivery_id},
        )


async def _schedule(
    background_tasks: common.BackgroundTasks,
    handler: HandlerKey,
    function: Callable[..., Awaitable[object]],
    *args: object,
    payload: JsonObject,
    event_type: str,
    delivery_id: str,
) -> dict[str, str] | None:
    """Queue a verified delivery, or run it inline when the queue is disabled.

    Returns the HTTP response the queue already settled, or None when the
    caller should keep its existing in-process behavior.
    """
    try:
        outcome = await ingress.submit(
            handler,
            event_type=event_type,
            delivery_id=delivery_id,
            payload=payload,
        )
    except IngressUnavailable:
        # A 5xx is the only answer GitHub retries, and dropping this delivery
        # would drop real work.
        common.logger.error(
            "GitHub ingress queue unavailable; asking GitHub to retry",
            extra={"github_delivery": delivery_id, "github_handler": handler},
            exc_info=True,
        )
        raise common.HTTPException(
            status_code=503, detail="GitHub ingress queue unavailable"
        ) from None
    if outcome == "duplicate":
        return _DUPLICATE_RESPONSE
    if outcome == "queued":
        return _QUEUED_RESPONSE
    background_tasks.add_task(function, *args)
    return None


@router.post("/webhooks/github")
async def github_webhook(
    request: common.Request, response: Response, background_tasks: common.BackgroundTasks
) -> dict[str, str]:
    """Handle GitHub webhooks for issue and PR events that tag @open-swe."""
    body = await request.body()

    signature = request.headers.get("X-Hub-Signature-256", "")
    if not common.verify_github_signature(body, signature, secret=common.GITHUB_WEBHOOK_SECRET):
        common.logger.warning("Invalid GitHub webhook signature")
        raise common.HTTPException(status_code=401, detail="Invalid signature")

    event_type = request.headers.get("X-GitHub-Event", "")
    delivery_id = request.headers.get("X-GitHub-Delivery", "")
    common.logger.info(
        "GitHub webhook received",
        extra={
            "github_event": event_type,
            "github_delivery": delivery_id,
            "github_payload": body.decode("utf-8", errors="replace"),
        },
    )

    if event_type not in common.SUPPORTED_GH_EVENTS:
        common.logger.info("Ignoring unsupported GitHub event type: %s", event_type)
        return {"status": "ignored", "reason": f"Unsupported event type: {event_type}"}

    try:
        parsed = common.json.loads(body)
    except common.json.JSONDecodeError:
        common.logger.exception("Failed to parse GitHub webhook JSON")
        return {"status": "error", "message": "Invalid JSON"}
    if not isinstance(parsed, dict):
        return {"status": "error", "message": "Invalid JSON"}
    payload = cast(JsonObject, parsed)

    webhook_repo = cast(JsonObject, payload.get("repository") or {})
    webhook_repo_config = {
        "owner": cast(JsonObject, webhook_repo.get("owner") or {}).get("login", ""),
        "name": webhook_repo.get("name", ""),
    }

    if webhook_repo_config["owner"] and webhook_repo_config["name"]:
        repository = f"{webhook_repo_config['owner']}/{webhook_repo_config['name']}"
        try:
            routable = await repo_is_routable(
                cast(str, webhook_repo_config["owner"]),
                cast(str, webhook_repo_config["name"]),
            )
        except WorkspaceLookupError:
            # Ownership is unknown, so dropping the delivery may drop real work.
            # GitHub retries a 5xx and nothing else, so answer 503 and let it.
            common.logger.error(
                "Workspace lookup failed for a GitHub delivery; asking GitHub to retry",
                extra={"repository": repository, "github_delivery": delivery_id},
                exc_info=True,
            )
            response.status_code = 503
            return {"status": "error", "reason": "workspace ownership is temporarily unreadable"}
        if not routable:
            common.logger.info(
                "Ignoring GitHub event for a repository no workspace owns",
                extra={"repository": repository},
            )
            return {"status": "ignored", "reason": "repository is not assigned to a workspace"}

    # Ahead of the per-event branches below, several of which return early, but
    # behind both gates they answer to: the repository must belong to a
    # workspace and be allowlisted.
    if event_type in EXPEDITED_REVIEW_EVENTS and common.is_repo_allowed(webhook_repo_config):
        scheduled = await _schedule(
            background_tasks,
            "github_expedited_review",
            service.process_expedited_review_event,
            payload,
            event_type,
            payload=payload,
            event_type=event_type,
            delivery_id=delivery_id,
        )
        if scheduled is not None and scheduled is _DUPLICATE_RESPONSE:
            return scheduled

    issue = cast(JsonObject, payload.get("issue") or {})
    is_pull_request_comment = bool(event_type == "issue_comment" and issue.get("pull_request"))
    is_issue_comment = bool(event_type == "issue_comment" and not issue.get("pull_request"))
    is_issue_event = event_type == "issues"
    is_pull_request_event = event_type == "pull_request"

    if is_pull_request_event:
        action = payload.get("action", "")
        if action not in common.SUPPORTED_GH_PULL_REQUEST_ACTIONS:
            common.logger.info("Ignoring unsupported GitHub pull_request action: %s", action)
            return {
                "status": "ignored",
                "reason": f"Unsupported GitHub pull_request action: {action}",
            }
        if action in common.GH_PR_AGENT_STATE_ACTIONS:
            background_tasks.add_task(common.update_agent_thread_pr_state, payload)
            try:
                await common.update_agent_pr_usage_from_webhook(payload)
            except Exception:  # noqa: BLE001
                common.logger.debug("Failed to update Agent PR usage", exc_info=True)
        if action in common.GH_PR_WATCH_TOGGLE_ACTIONS:
            common.logger.info(
                "Accepted GitHub PR %s webhook, scheduling reviewer watch update", action
            )
            scheduled = await _schedule(
                background_tasks,
                "github_pr_close",
                service.process_github_pr_close,
                payload,
                payload=payload,
                event_type=event_type,
                delivery_id=delivery_id,
            )
            if scheduled is not None:
                return scheduled
            return {"status": "accepted", "message": f"Processing PR {action} for reviewer watch"}
        if action in common.GH_PR_FIRST_REVIEW_ACTIONS:
            if not await common.is_repo_auto_review_enabled(webhook_repo_config):
                return {"status": "ignored", "reason": "Automatic review disabled for repository"}
            gate_rejection = await common.enforce_public_repo_org_gate(payload, "pull_request")
            if gate_rejection is not None:
                return gate_rejection
            common.logger.info("Accepted GitHub PR %s webhook, scheduling auto-review task", action)
            scheduled = await _schedule(
                background_tasks,
                "github_pr_ready",
                service.process_github_pr_ready,
                payload,
                payload=payload,
                event_type=event_type,
                delivery_id=delivery_id,
            )
            if scheduled is not None:
                return scheduled
            return {"status": "accepted", "message": f"Processing PR {action} for auto-review"}
        if action in common.GH_PR_AGENT_STATE_ACTIONS:
            return {"status": "accepted", "message": f"Processing PR {action} state"}
        common.logger.info("Ignoring unsupported GitHub pull_request action: %s", action)
        return {
            "status": "ignored",
            "reason": f"Unsupported GitHub pull_request action: {action}",
        }

    if event_type == "push":
        if not await common.is_repo_auto_review_enabled(webhook_repo_config):
            return {"status": "ignored", "reason": "Automatic review disabled for repository"}
        common.logger.info("Accepted GitHub push webhook, scheduling reviewer watch evaluation")
        scheduled = await _schedule(
            background_tasks,
            "github_push",
            service.process_github_push_event,
            payload,
            payload=payload,
            event_type=event_type,
            delivery_id=delivery_id,
        )
        if scheduled is not None:
            return scheduled
        return {"status": "accepted", "message": "Processing GitHub push for reviewer watch"}

    if not common.is_repo_allowed(webhook_repo_config):
        common.logger.debug(
            "Rejecting GitHub webhook: repo '%s/%s' not in allowlist",
            webhook_repo_config.get("owner"),
            webhook_repo_config.get("name"),
        )
        return {"status": "ignored", "reason": "Repository not in allowlist"}

    if event_type in common.GITHUB_CI_EVENTS:
        scheduled = await _schedule(
            background_tasks,
            "github_ci",
            service.process_github_ci_event,
            payload,
            event_type,
            delivery_id,
            payload=payload,
            event_type=event_type,
            delivery_id=delivery_id,
        )
        if scheduled is not None:
            return scheduled
        return {"status": "accepted", "message": "Processing GitHub CI event"}

    if is_issue_event:
        action = payload.get("action", "")
        if action not in common.SUPPORTED_GH_ISSUE_ACTIONS:
            common.logger.info("Ignoring unsupported GitHub issue action: %s", action)
            return {"status": "ignored", "reason": f"Unsupported GitHub issue action: {action}"}
        if action == "edited":
            changes = payload.get("changes", {})
            if not any(field in changes for field in ("body", "title")):
                common.logger.info("Ignoring GitHub issue edit without title/body changes")
                return {"status": "ignored", "reason": "Issue edit did not change title or body"}
        if action == "opened":
            scheduled = await _schedule(
                background_tasks,
                "github_issue_automations",
                _launch_issue_automations,
                payload,
                delivery_id,
                payload=payload,
                event_type=event_type,
                delivery_id=delivery_id,
            )
            if scheduled is not None and scheduled is _DUPLICATE_RESPONSE:
                return scheduled

        issue_text = f"{issue.get('title', '')}\n\n{issue.get('body', '')}"
        if not common.mentions_open_swe(issue_text):
            tags = common.describe_open_swe_tags()
            common.logger.info("Ignoring issue that does not mention %s", tags)
            return {"status": "ignored", "reason": f"Issue does not mention {tags}"}

        gate_rejection = await common.enforce_public_repo_org_gate(payload, event_type)
        if gate_rejection is not None:
            return gate_rejection

        scheduled = await _schedule(
            background_tasks,
            "github_issue",
            service.process_github_issue,
            payload,
            event_type,
            payload=payload,
            event_type=event_type,
            delivery_id=delivery_id,
        )
        if scheduled is not None:
            return scheduled
        common.logger.info("Accepted GitHub issue webhook, scheduling background task")
        return {"status": "accepted", "message": "Processing GitHub issue event"}

    action = payload.get("action", "")
    supported_comment_actions = common.SUPPORTED_GH_COMMENT_ACTIONS.get(event_type)
    if supported_comment_actions is None:
        common.logger.info("Ignoring unsupported GitHub payload shape for event=%s", event_type)
        return {"status": "ignored", "reason": f"Unsupported payload for event type: {event_type}"}
    if action and action not in supported_comment_actions:
        common.logger.debug("Ignoring unsupported GitHub %s action: %s", event_type, action)
        return {"status": "ignored", "reason": f"Unsupported GitHub {event_type} action: {action}"}

    comment = payload.get("comment") or payload.get("review") or {}
    comment_body = (comment.get("body") or "") if isinstance(comment, dict) else ""

    if (
        event_type == "pull_request_review_comment"
        and common.review_comment_reply_parent_id(payload) is not None
    ):
        gate_rejection = await common.enforce_public_repo_org_gate(payload, event_type)
        if gate_rejection is not None:
            return gate_rejection
        scheduled = await _schedule(
            background_tasks,
            "github_review_finding_reply",
            service.process_github_review_finding_reply,
            payload,
            payload=payload,
            event_type=event_type,
            delivery_id=delivery_id,
        )
        if scheduled is not None:
            return scheduled
        return {"status": "accepted", "message": "Processing review finding reply"}

    if not common.mentions_open_swe(comment_body):
        tags = common.describe_open_swe_tags()
        common.logger.debug(
            "Ignoring GitHub %s%s that does not mention %s",
            event_type,
            f" action={action}" if action else "",
            tags,
        )
        return {"status": "ignored", "reason": f"Comment does not mention {tags}"}

    gate_rejection = await common.enforce_public_repo_org_gate(payload, event_type)
    if gate_rejection is not None:
        return gate_rejection

    common.logger.info("Accepted GitHub webhook: event=%s, scheduling background task", event_type)
    if is_pull_request_comment or event_type in {
        "pull_request_review_comment",
        "pull_request_review",
    }:
        scheduled = await _schedule(
            background_tasks,
            "github_pr_comment",
            service.process_github_pr_comment,
            payload,
            event_type,
            payload=payload,
            event_type=event_type,
            delivery_id=delivery_id,
        )
        if scheduled is not None:
            return scheduled
        return {"status": "accepted", "message": f"Processing {event_type} event"}

    if is_issue_comment:
        scheduled = await _schedule(
            background_tasks,
            "github_issue",
            service.process_github_issue,
            payload,
            event_type,
            payload=payload,
            event_type=event_type,
            delivery_id=delivery_id,
        )
        if scheduled is not None:
            return scheduled
        return {"status": "accepted", "message": "Processing GitHub issue comment event"}

    common.logger.info("Ignoring unsupported GitHub payload shape for event=%s", event_type)
    return {"status": "ignored", "reason": f"Unsupported payload for event type: {event_type}"}
