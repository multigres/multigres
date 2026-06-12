# pgregress patches (PostgreSQL 17)

This directory holds **per-test patches** that describe known-acceptable
differences between PostgreSQL 17's upstream expected output and multigres's
actual output. The regress runner applies the patch to the upstream
`expected/<name>.out` before comparing against the actual `results/<name>.out`.

## Layout

```text
pg17/
  README.md            # you are here
  patches/
    <testname>.patch   # unified diff, one per test that needs it
```

## What belongs here

A patch describes a **deviation that we accept** — typically because multigres
produces a differently-worded error message, or emits fewer error-context
lines than Postgres. Patches must not absorb real regressions (wrong result
rows, flipped success/error, changed column types). The reviewer's job is to
inspect each patch like any other diff.

## Workflow

**Verify mode (default, CI):**

```bash
make pgregress
```

Applies each patch to the upstream expected output, strict-diffs against
actual output, and fails if any diff remains. A test whose patch no longer
applies (upstream PG changed the file) also fails.

**Generate mode (developer):**

```bash
make pgregress-update-patches
```

For every test where the current patch doesn't produce a clean match, writes
a fresh `<testname>.patch` containing the actual delta. Review the resulting
diff in the PR before merging.

## Results format

Each test gets three columns in `results.json`:

| Field           | Meaning                                           |
| --------------- | ------------------------------------------------- |
| `status`        | `pass` or `fail`                                  |
| `patch_applied` | `true` if a patch was used to make expected match |
| `patch_path`    | relative path to the patch file (empty if none)   |

## When to delete a patch

Delete a `<testname>.patch` whenever the code change makes the test pass
without it — the next `make pgregress-update-patches` run will also remove
stale patches (it deletes `.patch` files whose corresponding test is now an
exact match).
