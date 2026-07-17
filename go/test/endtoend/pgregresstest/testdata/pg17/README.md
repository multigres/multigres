# pgregress patches (PostgreSQL 17)

This directory holds **per-test patches** that describe known-acceptable
differences between PostgreSQL 17's upstream expected output and multigres's
actual output. The regress and isolation runners apply these patches to the
upstream `expected/<name>.out` before comparing against the actual
`results/<name>.out`. PostgreSQL's stock numbered alternatives are checked
first, so a matching `_0.out` through `_9.out` is a compatible pass
and needs no patch.

## Layout

```text
pg17/
  README.md            # you are here
  patches/
    <testname>.patch             # regression-suite patch
    isolation/<testname>.patch   # isolation-suite patch
```

## What belongs here

A patch describes a **deviation that we accept** — typically because multigres
produces a differently-worded error message, or emits fewer error-context
lines than Postgres. Patches must not absorb real regressions (wrong result
rows, flipped success/error, changed column types). The reviewer's job is to
inspect each patch like any other diff.

Every intentional security/safety divergence must have a comment preamble that
names the blocked capability and explains any directly dependent output. When
a patch is based on a non-canonical upstream file, declare it explicitly:

```text
# pgregress-expected-file: xml_1.out
```

Verification fails if that file is missing or the directive is malformed. This
keeps patch generation deterministic instead of choosing whichever expected
file happens to produce the shortest diff. Core `xml` and `xmlmap` also have
explicit no-libxml preferences in the verifier so residual diffs use `_1.out`
even when no patch exists.

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

Together these fields classify every result:

- `pass` without a patch: compatible.
- `pass` with a patch: accepted Multigres divergence (also shown as passed).
- `fail`: genuine residual failure (possibly after an accepted narrow patch).

## When to delete a patch

Delete a `<testname>.patch` whenever the code change makes the test pass
without it — the next `make pgregress-update-patches` run will also remove
stale patches (it deletes `.patch` files whose corresponding test is now an
exact match).
