# Agent tooling: patch-based writes, command approval, model routing

Three pieces of the coding loop that are easy to get subtly wrong: how a change
is written, when a destructive command needs a human, and when a cheap model
must give up.

## 1. Writes are patches

`apply_patch` (`agent/tools/apply_patch.py`) applies a git-style unified diff in
the sandbox. It sits beside Deep Agents' `write_file` and `edit_file`; the tool
description tells the model to prefer it for multi-line and multi-file changes.

Why a patch rather than a whole-file write:

- the diff is the reviewable unit, so a PR review sees exactly what changed;
- an ambiguous or stale hunk fails before anything is written, instead of
  silently dropping the parts of the file the model did not quote;
- one patch can touch several files and is applied in two phases: every file is
  read and every hunk is matched first, then all writes happen. A matching
  failure leaves every file untouched.

Strictness is deliberate:

| Input | Result |
| --- | --- |
| hunk context does not match | error naming the hunk; no writes |
| hunk counts disagree with the body | parse error |
| `..` or `~` in a path | parse error |
| rename or binary section | parse error |
| up to 3 lines of line drift | accepted |
| `\ No newline at end of file` | preserved |

`apply_file_patch` / `apply_hunks` are pure functions, so the parser and applier
are tested without a sandbox (`tests/agent/test_apply_patch.py`).

## 2. Destructive commands need a human

`DangerousCommandApprovalMiddleware`
(`agent/middleware/dangerous_commands.py`) wraps `execute` and
`background_execute`. It inspects the command before the tool runs and, when the
command's purpose is destruction, calls LangGraph's `interrupt()` with:

```json
{
  "type": "dangerous_command",
  "command": "rm -rf build",
  "reason": "deletes files",
  "tool": "execute",
  "tool_call_id": "...",
  "prompt": "A command is waiting for human approval ..."
}
```

The interrupt checkpoints the run. Resume it with
`Command(resume={"approved": true})` to continue from the checkpoint; any other
answer returns an error `ToolMessage` that the agent cannot retry unchanged.

Gated commands include `rm`/`del`/`rmdir`/`shred`, `git push --force` (including
`+refspec`), `git reset --hard`, `git clean -f`, `sudo`/`doas`, disk writers
(`dd`, `mkfs*`), recursive `chmod 777`, machine shutdown, downloads piped into a
shell, PowerShell `Remove-Item -Recurse/-Force`, and fork bombs. Wrappers
(`env`, `timeout`, `xargs`, assignments) and `bash -c "..."` are unwrapped before
the check. A command that cannot be tokenized is gated rather than assumed safe.

Modes (`OPEN_SWE_DANGEROUS_COMMAND_APPROVAL`):

| Mode | Behavior |
| --- | --- |
| `off` (default) | no gating; existing behavior is unchanged |
| `interactive` | interrupt for `web`/`desktop`/`dashboard` runs; deny on Slack, GitHub, Linear and schedules, where nobody is attached to approve |
| `always` | interrupt on every surface |

When `SANDBOX_TYPE=local`, the middleware defaults to `interactive` unless
`OPEN_SWE_DANGEROUS_COMMAND_APPROVAL` is set explicitly: the local provider runs
commands directly on the host, so a destructive command there has no container
boundary to contain it.

## 3. Model routing escalates

`ModelSelectionMiddleware` (`agent/middleware/model_selection.py`) classifies a
turn into `fast`, `balanced`, or `performance` once per run and keeps that route
in graph state. Cheap routes need an escape hatch, so two signals escalate the
route to `performance` for the rest of the run:

| Signal | Threshold | Reason |
| --- | --- | --- |
| consecutive failed tool results | 2 | `repeated_tool_failures` |
| tool calls with no file write | 15 | `steps_without_edit` |

The escalation is sticky (a failure does not un-escalate later), emits a
`model_route_escalated` stream event, and is covered by
`tests/middleware/test_model_selection.py`. Plan mode always uses
`performance`, so it is never escalated twice.

Escalation is intentionally rule-based, not a second model call: it is cheap,
deterministic, and its rate can be reported. A run that never escalates suggests
the classifier is too conservative; a run that escalates immediately suggests
the cheap route was the wrong choice for that task shape.

## Where this sits

```
execute / background_execute
  └─ DangerousCommandApprovalMiddleware   (destructive command gate)
  └─ PullRequestCreationGuardMiddleware   (no shell fallback for PR creation)
  └─ WorkflowPushGuardMiddleware          (workflow-file push approval)

apply_patch / edit_file / write_file
  └─ sandbox backend (per-thread workspace)

before each model call
  └─ ModelSelectionMiddleware             (route, then escalate on failure)
  └─ ConversationOffloadingMiddleware     (compaction at the context ceiling)
```
