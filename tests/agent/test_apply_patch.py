import importlib
from types import SimpleNamespace

import pytest
from deepagents.backends.protocol import DeleteResult, ReadResult, WriteResult

from agent.tools.apply_patch import (
    PatchError,
    apply_hunks,
    parse_unified_diff,
)

apply_patch_module = importlib.import_module("agent.tools.apply_patch")


class FakeBackend:
    def __init__(self, files: dict[str, str]) -> None:
        self.files = dict(files)
        self.writes: list[tuple[str, str]] = []
        self.deletes: list[str] = []

    async def aread(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        content = self.files.get(file_path)
        if content is None:
            return ReadResult(error="not found")
        total_lines = content.count("\n") + (0 if content.endswith("\n") else 1)
        total_lines = max(1, total_lines)
        return ReadResult(
            file_data={"content": content, "encoding": "utf-8"},
            total_lines=total_lines,
            start_line=1,
            end_line=total_lines,
        )

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        self.files[file_path] = content
        self.writes.append((file_path, content))
        return WriteResult(path=file_path)

    async def adelete(self, file_path: str) -> DeleteResult:
        self.files.pop(file_path, None)
        self.deletes.append(file_path)
        return DeleteResult(path=file_path)


def _wire_backend(monkeypatch: pytest.MonkeyPatch, backend: FakeBackend) -> None:
    monkeypatch.setattr(
        apply_patch_module, "get_cached_sandbox_backend", lambda thread_id: backend
    )
    monkeypatch.setattr(
        apply_patch_module,
        "RunConfig",
        SimpleNamespace(from_runtime=lambda: SimpleNamespace(thread_id="thread-1")),
    )


def _apply(patch: str, original: str) -> tuple[str, int, int]:
    files = parse_unified_diff(patch)
    assert len(files) == 1
    return apply_hunks(original, files[0].hunks)


def test_apply_modifies_matching_context() -> None:
    patch = """\
--- a/f.txt
+++ b/f.txt
@@ -1,3 +1,3 @@
 a
-b
+B
 c
"""
    text, additions, deletions = _apply(patch, "a\nb\nc\n")

    assert text == "a\nB\nc\n"
    assert (additions, deletions) == (1, 1)


def test_apply_adds_a_new_file() -> None:
    patch = """\
--- /dev/null
+++ b/new.txt
@@ -0,0 +1,2 @@
+hello
+world
"""
    files = parse_unified_diff(patch)
    assert files[0].old_path is None
    text, additions, deletions = apply_hunks("", files[0].hunks)

    assert text == "hello\nworld\n"
    assert (additions, deletions) == (2, 0)


def test_apply_deletes_a_file() -> None:
    patch = """\
--- a/old.txt
+++ /dev/null
@@ -1,2 +0,0 @@
-a
-b
"""
    files = parse_unified_diff(patch)

    assert files[0].new_path is None


def test_apply_allows_small_line_drift() -> None:
    patch = """\
--- a/f.txt
+++ b/f.txt
@@ -1,2 +1,2 @@
 a
-b
+B
"""
    text, _, _ = _apply(patch, "inserted\na\nb\n")

    assert text == "inserted\na\nB\n"


def test_apply_honours_no_newline_marker() -> None:
    patch = """\
--- a/f.txt
+++ b/f.txt
@@ -1 +1 @@
-old
+new
\\ No newline at end of file
"""
    text, _, _ = _apply(patch, "old\n")

    assert text == "new"


def test_patch_rejects_traversal_paths() -> None:
    patch = """\
--- a/../../etc/passwd
+++ b/../../etc/passwd
@@ -1 +1 @@
-a
+b
"""
    with pytest.raises(PatchError, match="'\\.\\.'"):
        parse_unified_diff(patch)


def test_patch_rejects_a_hunk_that_does_not_match() -> None:
    patch = """\
--- a/f.txt
+++ b/f.txt
@@ -1,2 +1,2 @@
 a
-b
+B
"""
    with pytest.raises(PatchError, match="does not match"):
        _apply(patch, "x\ny\nz\n")


def test_patch_rejects_inconsistent_hunk_counts() -> None:
    patch = """\
--- a/f.txt
+++ b/f.txt
@@ -1,5 +1,5 @@
 a
-b
+B
"""
    with pytest.raises(PatchError, match="old and"):
        parse_unified_diff(patch)


def test_patch_rejects_renames() -> None:
    patch = """\
diff --git a/old.txt b/new.txt
rename from old.txt
rename to new.txt
--- a/old.txt
+++ b/new.txt
@@ -1 +1 @@
-a
+b
"""
    with pytest.raises(PatchError, match="renames are not supported"):
        parse_unified_diff(patch)


def test_patch_rejects_binary_sections() -> None:
    patch = """\
Binary files a/image.png and b/image.png differ
"""
    with pytest.raises(PatchError, match="binary"):
        parse_unified_diff(patch)


async def test_tool_applies_add_modify_and_delete_in_one_patch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeBackend({"keep.txt": "old\n"})
    _wire_backend(monkeypatch, backend)
    patch = """\
--- a/keep.txt
+++ b/keep.txt
@@ -1 +1 @@
-old
+new
--- /dev/null
+++ b/added.txt
@@ -0,0 +1 @@
+hello
--- a/removed.txt
+++ /dev/null
@@ -1 +0,0 @@
-gone
"""

    result = await apply_patch_module.apply_patch(patch)

    assert result["success"] is True
    assert backend.files == {"keep.txt": "new\n", "added.txt": "hello\n"}
    assert backend.deletes == ["removed.txt"]


async def test_tool_writes_nothing_when_a_hunk_does_not_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeBackend({"f.txt": "actual\n"})
    _wire_backend(monkeypatch, backend)
    patch = """\
--- a/f.txt
+++ b/f.txt
@@ -1 +1 @@
-expected
+replacement
"""

    result = await apply_patch_module.apply_patch(patch)

    assert result["success"] is False
    assert backend.writes == []
    assert backend.files == {"f.txt": "actual\n"}
