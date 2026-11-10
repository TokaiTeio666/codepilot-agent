"""Require human approval before a destructive shell command runs.

The agent executes model-authored commands. Narrowing every command with a
regex would break real work, so this gate only stops commands whose whole point
is destruction (``rm``, force-push, ``git reset --hard``, elevated execution,
downloads piped into a shell) and asks a human to resume the run.

Three modes, from `OPEN_SWE_DANGEROUS_COMMAND_APPROVAL`:

- ``off``: no gating (default; behavior is unchanged).
- ``interactive``: interrupt for dashboard/desktop runs; deny elsewhere, where
  no human is attached to resume the run.
- ``always``: interrupt for every surface.
"""

import json
import logging
import os
import re
import shlex
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Literal

from langchain.agents.middleware.types import AgentState
from langchain_core.messages import ToolMessage
from langgraph.config import get_config
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command, interrupt

from agent.middleware.trace import OpenSWEMiddleware
from agent.prompts import render_prompt
from agent.run_config import RunConfig

logger = logging.getLogger(__name__)

type ApprovalMode = Literal["off", "interactive", "always"]
type ToolResult = ToolMessage | Command

_MODES: tuple[ApprovalMode, ...] = ("off", "interactive", "always")
_INTERACTIVE_SURFACES = frozenset({"web", "desktop", "dashboard"})
_SHELL_TOOLS = frozenset({"execute", "background_execute"})
_SHELLS = frozenset({"bash", "sh", "zsh", "dash", "ksh"})
_WRAPPERS = frozenset({"command", "env", "nohup", "time", "timeout", "xargs"})
_OPERATORS = frozenset({"&&", "||", ";", "|", "&", "\n"})
_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
_NUMERIC = re.compile(r"^\d+$")
_FORK_BOMB = re.compile(r":\s*\(\s*\)\s*\{")
_DOWNLOAD_TO_SHELL = re.compile(
    r"(?i)\b(?:curl|wget)\b[^|\n]*\|\s*(?:sudo\s+)?(?:bash|sh|zsh|dash)\b"
)
_POWERSHELL_REMOVE = re.compile(r"(?i)\bRemove-Item\b[^\n]*(?:-Recurse|-Force)")
_WINDOWS_DELETE = re.compile(r"(?i)\b(?:del|erase)\b[^\n]*/[sf]\b")
_WINDOWS_RMDIR = re.compile(r"(?i)\b(?:rmdir|rd)\b[^\n]*/s\b")


@dataclass(frozen=True)
class DangerousCommand:
    reason: str


def _tool_name(request: ToolCallRequest) -> str | None:
    tool_call = getattr(request, "tool_call", None)
    if isinstance(tool_call, Mapping):
        name = tool_call.get("name")
        return name if isinstance(name, str) else None
    return None


def _tool_args(request: ToolCallRequest) -> Mapping[str, object]:
    tool_call = getattr(request, "tool_call", None)
    args = tool_call.get("args") if isinstance(tool_call, Mapping) else None
    return args if isinstance(args, Mapping) else {}


def _tool_call_id(request: ToolCallRequest) -> str | None:
    tool_call = getattr(request, "tool_call", None)
    if isinstance(tool_call, Mapping):
        value = tool_call.get("id")
        return value if isinstance(value, str) else None
    return None


def _runtime_config(request: ToolCallRequest) -> Mapping[str, object]:
    runtime_config = getattr(getattr(request, "runtime", None), "config", None)
    if isinstance(runtime_config, Mapping):
        return runtime_config
    try:
        config = get_config()
    except Exception:
        return {}
    return config if isinstance(config, Mapping) else {}


def _surface(request: ToolCallRequest) -> str:
    try:
        return (RunConfig.from_config(_runtime_config(request)).source or "").lower()
    except Exception:
        logger.debug("Could not resolve the run surface for command approval", exc_info=True)
        return ""


def _segments(command: str) -> list[list[str]] | None:
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError:
        return None
    segments: list[list[str]] = []
    current: list[str] = []
    for token in tokens:
        if token in _OPERATORS:
            if current:
                segments.append(current)
                current = []
            continue
        current.append(token)
    if current:
        segments.append(current)
    return segments


def _unwrap(tokens: list[str]) -> list[str]:
    remaining = list(tokens)
    while remaining:
        head = os.path.basename(remaining[0].strip("'\""))
        if _ASSIGNMENT.match(remaining[0]):
            remaining.pop(0)
            continue
        if head not in _WRAPPERS:
            return remaining
        remaining.pop(0)
        while remaining and (
            remaining[0].startswith("-")
            or _NUMERIC.match(remaining[0])
            or _ASSIGNMENT.match(remaining[0])
        ):
            remaining.pop(0)
    return remaining


def _shell_inline_command(tokens: list[str]) -> str | None:
    for index, token in enumerate(tokens):
        if token == "-c" or (
            token.startswith("-") and not token.startswith("--") and "c" in token[1:]
        ):
            return tokens[index + 1] if index + 1 < len(tokens) else None
    return None


def _git_subcommand(tokens: list[str]) -> tuple[str, list[str]]:
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in {"-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path"}:
            index += 2
            continue
        if token.startswith("-"):
            index += 1
            continue
        return token, tokens[index + 1 :]
    return "", []


def _git_danger(tokens: list[str]) -> DangerousCommand | None:
    subcommand, rest = _git_subcommand(tokens)
    if subcommand == "push":
        forced = any(
            token in {"-f", "--force", "--force-with-lease", "--force-if-includes", "--mirror"}
            or token.startswith("--force-with-lease=")
            or token.startswith("+")
            for token in rest
        )
        if forced:
            return DangerousCommand("force-pushes a branch")
        if any(token in {"-d", "--delete"} for token in rest):
            return DangerousCommand("deletes a remote branch")
    if subcommand == "reset" and any(token == "--hard" for token in rest):
        return DangerousCommand("discards uncommitted work")
    if subcommand == "clean":
        forced = any(
            token == "--force"
            or (token.startswith("-") and not token.startswith("--") and "f" in token[1:])
            for token in rest
        )
        if forced:
            return DangerousCommand("deletes untracked files")
    return None


def _program_danger(program: str, tokens: list[str]) -> DangerousCommand | None:
    if program in {"rm", "rmdir", "rd", "del", "erase", "unlink", "shred", "wipefs"}:
        return DangerousCommand("deletes files")
    if program == "dd" or program.startswith("mkfs") or program == "format":
        return DangerousCommand("overwrites a disk or device")
    if program in {"shutdown", "reboot", "poweroff", "halt"}:
        return DangerousCommand("stops the machine")
    if program in {"sudo", "doas"}:
        return DangerousCommand("runs with elevated privileges")
    if program == "git":
        return _git_danger(tokens[1:])
    if program == "chmod":
        recursive = any(
            token == "--recursive"
            or (token.startswith("-") and not token.startswith("--") and "R" in token[1:])
            for token in tokens[1:]
        )
        world_writable = any("777" in token or "a+rwx" in token for token in tokens[1:])
        if recursive and world_writable:
            return DangerousCommand("makes a tree world-writable")
    if program in _SHELLS:
        inline = _shell_inline_command(tokens)
        if inline is not None:
            return is_dangerous_command(inline)
    return None


def _raw_danger(command: str) -> DangerousCommand | None:
    if _FORK_BOMB.search(command):
        return DangerousCommand("is a fork bomb")
    if _DOWNLOAD_TO_SHELL.search(command):
        return DangerousCommand("pipes a download into a shell")
    if _POWERSHELL_REMOVE.search(command):
        return DangerousCommand("recursively removes items")
    if _WINDOWS_DELETE.search(command) or _WINDOWS_RMDIR.search(command):
        return DangerousCommand("recursively deletes files")
    return None


def is_dangerous_command(command: str) -> DangerousCommand | None:
    """Return why ``command`` needs approval, or None when it is safe to run."""
    raw = _raw_danger(command)
    if raw is not None:
        return raw
    segments = _segments(command)
    if segments is None:
        # An unparseable command is not provably safe; treat it as gated.
        return DangerousCommand("could not be parsed for a safety check")
    for segment in segments:
        tokens = _unwrap(segment)
        if not tokens:
            continue
        program = os.path.basename(tokens[0].strip("'\""))
        danger = _program_danger(program, tokens)
        if danger is not None:
            return danger
    return None


def approval_mode(value: str) -> ApprovalMode:
    normalized = value.strip().lower()
    if normalized not in _MODES:
        raise ValueError(
            f"OPEN_SWE_DANGEROUS_COMMAND_APPROVAL must be one of {sorted(_MODES)}, "
            f"got {value!r}"
        )
    return normalized


def _approved(value: object) -> bool:
    if value is True:
        return True
    if isinstance(value, Mapping):
        return value.get("approved") is True or value.get("decision") == "approve"
    return False


class DangerousCommandApprovalMiddleware(OpenSWEMiddleware):
    """Pause a run before a destructive command until a human approves it."""

    state_schema = AgentState

    def __init__(self, *, mode: ApprovalMode = "off") -> None:
        self._mode = mode

    def _denied(
        self,
        request: ToolCallRequest,
        *,
        command: str,
        reason: str,
    ) -> ToolMessage:
        content = {
            "status": "error",
            "error_type": "DangerousCommandNotApproved",
            "code": "dangerous_command_not_approved",
            "recoverable_by_agent": False,
            "error": render_prompt(
                "security/dangerous-command-denied.md", command=command, reason=reason
            ),
            "blocked_command": command,
        }
        return ToolMessage(
            content=json.dumps(content),
            tool_call_id=_tool_call_id(request),
            status="error",
        )

    def _gate(self, request: ToolCallRequest) -> ToolMessage | None:
        if self._mode == "off" or _tool_name(request) not in _SHELL_TOOLS:
            return None
        command = _tool_args(request).get("command")
        if not isinstance(command, str) or not command.strip():
            return None
        danger = is_dangerous_command(command)
        if danger is None:
            return None

        surface = _surface(request)
        if self._mode == "interactive" and surface not in _INTERACTIVE_SURFACES:
            return self._denied(request, command=command, reason=danger.reason)

        decision = interrupt(
            {
                "type": "dangerous_command",
                "command": command,
                "reason": danger.reason,
                "tool": _tool_name(request),
                "tool_call_id": _tool_call_id(request),
                "prompt": render_prompt(
                    "security/dangerous-command-approval.md",
                    command=command,
                    reason=danger.reason,
                ),
            }
        )
        if not _approved(decision):
            return self._denied(request, command=command, reason=danger.reason)
        return None

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolResult],
    ) -> ToolResult:
        denied = self._gate(request)
        if denied is not None:
            return denied
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolResult]],
    ) -> ToolResult:
        denied = self._gate(request)
        if denied is not None:
            return denied
        return await handler(request)
