Apply a unified diff to files in the sandbox.

Prefer this tool over rewriting a whole file: the patch is the reviewable unit,
it cannot silently drop the parts of a file you did not quote, and a failed
hunk leaves every file untouched.

**Required shape** — git-style unified diff, one section per file:

```text
--- a/path/to/file.py
+++ b/path/to/file.py
@@ -12,7 +12,8 @@
 existing context line
-removed line
+added line
 another context line
```

Rules:

- Paths are relative to the workspace. Use the same `a/` and `b/` prefixes as
  `git diff`; they are stripped. Never use `..` or `~`.
- Include at least three lines of context around each change. Hunk headers must
  match the patch body: `@@ -old_start,old_count +new_start,new_count @@`.
- One patch may update several files. To create a file, use `--- /dev/null` and
  `+++ b/path`; to delete one, use `+++ /dev/null`.
- Renames and binary patches are not supported. Do a delete plus an add instead.
- A hunk that does not match exactly is an error. Re-read the file and regenerate
  the patch rather than guessing; the tool reports the failing hunk.
