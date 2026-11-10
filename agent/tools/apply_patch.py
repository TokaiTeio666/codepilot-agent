"""Tool: apply a unified diff inside the sandbox.

Writes are expressed as a patch rather than a whole-file replacement so every
change is reviewable before it lands and a partially wrong edit cannot silently
truncate a file. The parser is deliberately strict: an ambiguous hunk is an
error, never a best-effort guess.
"""

import re
from dataclasses import dataclass
from typing import Literal

from agent.run_config import RunConfig
from agent.runtime import get_cached_sandbox_backend
from agent.sandboxes.state import SandboxBackendProxy

_HUNK_HEADER = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
_NO_NEWLINE = "\\ No newline at end of file"
_READ_PAGE_LINES = 5_000
_MAX_HUNK_DRIFT = 3

type HunkLineKind = Literal["context", "add", "remove"]
type FileStatus = Literal["added", "modified", "deleted"]


class PatchError(ValueError):
    """The patch cannot be parsed or does not apply cleanly."""


@dataclass(frozen=True)
class HunkLine:
    kind: HunkLineKind
    text: str
    no_newline: bool = False


@dataclass(frozen=True)
class Hunk:
    old_start: int
    old_count: int
    new_start: int
    new_count: int
    lines: tuple[HunkLine, ...]


@dataclass(frozen=True)
class FilePatch:
    old_path: str | None
    new_path: str | None
    hunks: tuple[Hunk, ...]

    @property
    def target_path(self) -> str:
        path = self.new_path or self.old_path
        if path is None:
            raise PatchError("patch file has neither an old nor a new path")
        return path


@dataclass(frozen=True)
class FilePlan:
    path: str
    status: FileStatus
    content: str
    additions: int
    deletions: int


def _header_path(raw: str, *, prefix: str) -> str | None:
    value = raw.split("\t", 1)[0].strip()
    if value == "/dev/null":
        return None
    if value.startswith(prefix):
        value = value[len(prefix) :]
    return _safe_path(value)


def _safe_path(path: str) -> str:
    value = path.strip()
    if not value or "\x00" in value:
        raise PatchError("patch contains an empty file path")
    if value.startswith("~"):
        raise PatchError(f"patch path must not be home-relative: {value}")
    parts = re.split(r"[\\/]+", value)
    if ".." in parts:
        raise PatchError(f"patch path must not contain '..': {value}")
    return value


def _parse_hunk(lines: list[str], start: int) -> tuple[Hunk, int]:
    match = _HUNK_HEADER.match(lines[start])
    if match is None:
        raise PatchError(f"invalid hunk header: {lines[start]!r}")
    old_start = int(match.group(1))
    old_count = int(match.group(2)) if match.group(2) is not None else 1
    new_start = int(match.group(3))
    new_count = int(match.group(4)) if match.group(4) is not None else 1

    entries: list[HunkLine] = []
    index = start + 1
    while index < len(lines):
        line = lines[index]
        if line.startswith(_NO_NEWLINE):
            if not entries:
                raise PatchError("no-newline marker without a preceding line")
            entries[-1] = HunkLine(
                kind=entries[-1].kind, text=entries[-1].text, no_newline=True
            )
            index += 1
            continue
        if line.startswith("@@") or line.startswith("--- ") or line.startswith("diff --git "):
            break
        if not line:
            break
        prefix, text = line[0], line[1:]
        if prefix == " ":
            entries.append(HunkLine("context", text))
        elif prefix == "+":
            entries.append(HunkLine("add", text))
        elif prefix == "-":
            entries.append(HunkLine("remove", text))
        else:
            raise PatchError(f"invalid hunk line: {line!r}")
        index += 1

    expected_old = sum(1 for entry in entries if entry.kind != "add")
    expected_new = sum(1 for entry in entries if entry.kind != "remove")
    if expected_old != old_count or expected_new != new_count:
        raise PatchError(
            f"hunk @@ -{old_start},{old_count} +{new_start},{new_count} @@ has "
            f"{expected_old} old and {expected_new} new lines"
        )
    return (
        Hunk(
            old_start=old_start,
            old_count=old_count,
            new_start=new_start,
            new_count=new_count,
            lines=tuple(entries),
        ),
        index,
    )


def parse_unified_diff(patch: str) -> list[FilePatch]:
    """Parse a unified diff into one entry per file, rejecting renames."""
    lines = patch.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    files: list[FilePatch] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.startswith("Binary files ") or line.startswith("GIT binary patch"):
            raise PatchError("binary patches are not supported")
        if line.startswith("rename from ") or line.startswith("rename to "):
            raise PatchError("renames are not supported; add and delete instead")
        if not line.startswith("--- "):
            index += 1
            continue

        old_path = _header_path(line[4:], prefix="a/")
        if index + 1 >= len(lines) or not lines[index + 1].startswith("+++ "):
            raise PatchError(f"missing +++ header after {line!r}")
        new_path = _header_path(lines[index + 1][4:], prefix="b/")
        if old_path is None and new_path is None:
            raise PatchError("patch file deletes and creates nothing")
        if old_path is not None and new_path is not None and old_path != new_path:
            raise PatchError("renames are not supported; add and delete instead")

        index += 2
        hunks: list[Hunk] = []
        while index < len(lines):
            current = lines[index]
            if current.startswith("--- ") or current.startswith("diff --git "):
                break
            if current.startswith("@@"):
                hunk, index = _parse_hunk(lines, index)
                hunks.append(hunk)
                continue
            if current.startswith(("index ", "new file mode ", "deleted file mode ")):
                index += 1
                continue
            if not current:
                index += 1
                continue
            raise PatchError(f"unexpected line outside a hunk: {current!r}")
        if not hunks and old_path is not None:
            raise PatchError(f"file {old_path} has no hunks")
        files.append(FilePatch(old_path=old_path, new_path=new_path, hunks=tuple(hunks)))

    if not files:
        raise PatchError("patch contains no file changes")
    return files


def _matches(lines: list[str], start: int, hunk: Hunk) -> bool:
    cursor = start
    for entry in hunk.lines:
        if entry.kind == "add":
            continue
        if cursor >= len(lines) or lines[cursor] != entry.text:
            return False
        cursor += 1
    return True


def _find_start(lines: list[str], hunk: Hunk, expected: int) -> int:
    candidates = [expected]
    for drift in range(1, _MAX_HUNK_DRIFT + 1):
        candidates.extend((expected - drift, expected + drift))
    for candidate in candidates:
        if 0 <= candidate <= len(lines) and _matches(lines, candidate, hunk):
            return candidate
    raise PatchError(
        f"hunk @@ -{hunk.old_start},{hunk.old_count} "
        f"+{hunk.new_start},{hunk.new_count} @@ does not match the file"
    )


def apply_hunks(original: str, hunks: tuple[Hunk, ...]) -> tuple[str, int, int]:
    """Apply hunks to ``original``; returns (text, additions, deletions)."""
    lines = original.split("\n")
    offset = 0
    additions = 0
    deletions = 0
    for hunk in hunks:
        expected = (hunk.old_start - 1 if hunk.old_count else hunk.old_start) + offset
        start = _find_start(lines, hunk, expected)
        produced = [entry.text for entry in hunk.lines if entry.kind != "remove"]
        lines = [*lines[:start], *produced, *lines[start + hunk.old_count :]]
        offset += len(produced) - hunk.old_count
        additions += sum(1 for entry in hunk.lines if entry.kind == "add")
        deletions += sum(1 for entry in hunk.lines if entry.kind == "remove")

    text = "\n".join(lines)
    if hunks and hunks[-1].lines and hunks[-1].lines[-1].no_newline and text.endswith("\n"):
        text = text[:-1]
    return text, additions, deletions


async def _read_all(backend: SandboxBackendProxy, path: str) -> str:
    parts: list[str] = []
    offset = 0
    while True:
        result = await backend.aread(path, offset=offset, limit=_READ_PAGE_LINES)
        if result.error:
            raise PatchError(f"{path}: {result.error}")
        if result.file_data is not None:
            parts.append(str(result.file_data.get("content", "")))
        next_offset = result.next_offset
        if not isinstance(next_offset, int) or next_offset <= offset:
            break
        offset = next_offset
    return "".join(parts)


def _plan_file(patch_file: FilePatch, original: str) -> FilePlan:
    if patch_file.new_path is None:
        return FilePlan(
            path=patch_file.target_path,
            status="deleted",
            content="",
            additions=0,
            deletions=sum(1 for hunk in patch_file.hunks for entry in hunk.lines if entry.kind == "remove"),
        )
    text, additions, deletions = apply_hunks(original, patch_file.hunks)
    return FilePlan(
        path=patch_file.target_path,
        status="added" if patch_file.old_path is None else "modified",
        content=text,
        additions=additions,
        deletions=deletions,
    )


async def apply_patch(patch: str) -> dict[str, object]:
    """Implement the `apply_patch` tool."""
    if not isinstance(patch, str) or not patch.strip():
        return {"success": False, "error": "patch must be a non-empty unified diff"}

    try:
        file_patches = parse_unified_diff(patch)
    except PatchError as exc:
        return {"success": False, "error": str(exc)}

    thread_id = RunConfig.from_runtime().thread_id
    if not thread_id:
        return {"success": False, "error": "no thread_id in run config"}
    backend = get_cached_sandbox_backend(thread_id)

    plans: list[FilePlan] = []
    for patch_file in file_patches:
        try:
            # A deletion is idempotent: deleting an already-missing file is the
            # state the patch asks for, and reading it first would turn a retry
            # into an error.
            original = (
                ""
                if patch_file.old_path is None or patch_file.new_path is None
                else await _read_all(backend, patch_file.old_path)
            )
            plans.append(_plan_file(patch_file, original))
        except PatchError as exc:
            return {"success": False, "error": str(exc)}

    errors: list[str] = []
    for plan in plans:
        if plan.status == "deleted":
            result = await backend.adelete(plan.path)
        else:
            result = await backend.awrite(plan.path, plan.content)
        if result.error:
            errors.append(f"{plan.path}: {result.error}")
    if errors:
        return {"success": False, "error": "; ".join(errors)}

    return {
        "success": True,
        "files": [
            {
                "path": plan.path,
                "status": plan.status,
                "additions": plan.additions,
                "deletions": plan.deletions,
            }
            for plan in plans
        ],
        "additions": sum(plan.additions for plan in plans),
        "deletions": sum(plan.deletions for plan in plans),
    }
