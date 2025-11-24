# Subprocess Coverage Collection Design

**Date:** 2025-11-24
**Status:** Proposed

## Overview

This design enables code coverage collection from subprocess binaries executed during integration tests. Currently, `go test -cover` only captures coverage from test code itself, missing coverage from binaries spawned via `exec.Command()`.

## Goals

1. Collect coverage data from subprocess executions (multipooler, multigateway, pgctld, etc.)
2. Maintain existing test coverage collection
3. Combine both coverage datasets into a unified report
4. Minimize CI runtime impact (parallel execution)
5. Enable local reproducibility

## Non-Goals (Phase 1)

- Unified coverage collection in a single `go test` run (Phase 2 future work)
- Automated make targets (manual control first, codify later if successful)

## Architecture

### Core Mechanism

Go's `-cover` flag instruments binaries to emit coverage data on exit. When `GOCOVERDIR` is set, instrumented binaries write coverage files to that directory.

**Key insight:** Environment variables (PATH, GOCOVERDIR) propagate through `exec.Command()`, enabling transparent coverage collection.

### Build Structure

**Coverage binaries location:** `bin/cov/<binary>` (no suffix)

Why separate directory without suffix:

- PATH lookup finds exact binary names
- Tests don't need modification (use bare names like "pgctld")
- Clean separation from production binaries

**Example structure:**

```
bin/
├── multigateway           # Production binary
├── multipooler            # Production binary
├── pgctld                 # Production binary
└── cov/
    ├── multigateway       # Coverage-instrumented binary
    ├── multipooler        # Coverage-instrumented binary
    └── pgctld             # Coverage-instrumented binary
```

### Coverage Collection Strategy

**Two parallel test runs:**

1. **Direct coverage:** `go test -coverprofile` captures test code coverage
2. **Subprocess coverage:** `GOCOVERDIR` + PATH manipulation captures subprocess coverage

**Why two runs:**

- `go test -cover` and `GOCOVERDIR` don't integrate cleanly
- Parallel execution prevents runtime penalty
- Each run focuses on one coverage dimension
- Merge step combines results

### Coverage Isolation

Each test run uses a unique subdirectory: `coverage/<run-id>/`

**Run ID sources:**

- Local: timestamp or random value
- CI: GitHub Actions run ID

**Benefits:**

- Prevents pollution between runs
- Enables debugging specific runs
- Allows comparison across runs
- Natural uniqueness in CI

## Implementation

### Makefile Changes

Update `build-coverage` to use separate directory:

```make
# Build Go binaries with coverage
build-coverage:
	mkdir -p bin/cov/
	cp external/pico/pico.* go/common/web/templates/css/
	for cmd in $(COMMANDS); do \
		go build -cover -covermode=atomic -coverpkg=./... -o bin/cov/$$cmd ./go/cmd/$$cmd; \
	done
```

**Changes from current:**

- Create `bin/cov/` instead of `bin/`
- Output to `bin/cov/$$cmd` instead of `bin/$$cmd.cov`
- No `.cov` suffix on binaries

### Code Changes: PrependBinToPath()

The existing `pathutil.PrependBinToPath()` function ([go/tools/pathutil/pathutil.go:88](go/tools/pathutil/pathutil.go:88)) currently prepends `bin/` to PATH. This needs to be enhanced to support coverage collection.

**Current implementation:**

```go
func PrependBinToPath() error {
	return prependModuleSubdirsToPath("bin", "go/test/endtoend")
}
```

**Required change:**
When coverage collection is enabled (detected by checking if `bin/cov/` exists or via environment variable), prepend `bin/cov` before `bin`:

```go
func PrependBinToPath() error {
	// Check if coverage binaries exist
	moduleRoot, err := findModuleRoot()
	if err != nil {
		return err
	}
	covDir := filepath.Join(moduleRoot, "bin/cov")

	// If coverage binaries exist, prepend bin/cov before bin
	if _, err := os.Stat(covDir); err == nil {
		return prependModuleSubdirsToPath("bin/cov", "bin", "go/test/endtoend")
	}

	// Otherwise, just prepend bin as before
	return prependModuleSubdirsToPath("bin", "go/test/endtoend")
}
```

**Alternative approach:** Use environment variable like `MULTIGRES_USE_COVERAGE_BINS=1` for explicit control instead of auto-detection.

**Impact:** All tests using `pathutil.PrependBinToPath()` will automatically use coverage binaries when available, making local testing easier.

### Local Workflow

```bash
# 1. Build coverage binaries
make build-coverage

# 2. Set up environment
COVERAGE_RUN_ID="$(date +%Y%m%d-%H%M%S)"
export GOCOVERDIR="$(pwd)/coverage/$COVERAGE_RUN_ID"
mkdir -p "$GOCOVERDIR"

# 3. Run tests (subprocess coverage only)
# PrependBinToPath() will automatically find bin/cov/ binaries
go test -v ./...

# 4. Validate coverage was collected
if [ -z "$(ls -A $GOCOVERDIR)" ]; then
    echo "ERROR: No coverage files generated. Check PATH setup."
    exit 1
fi

# 5. Convert coverage data
go tool covdata textfmt -i="$GOCOVERDIR" -o=coverage-subprocess.txt
```

For combined coverage, also run:

```bash
# Direct test coverage
go test -cover -covermode=atomic -coverprofile=coverage-tests.txt -coverpkg=./... ./...

# Merge both
./scripts/merge-coverage.sh coverage-tests.txt coverage-subprocess.txt coverage.txt
```

### CI Integration (GitHub Actions)

**Parallel job structure:**

```yaml
jobs:
  test-direct-coverage:
    name: Run tests with direct coverage
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@...
      - name: Set up Go
        uses: actions/setup-go@...
        with:
          go-version: "1.25.0"

      # PostgreSQL, etcd, build setup...

      - name: Build
        run: make build

      - name: Run tests with coverage
        run: |
          PATH="/usr/lib/postgresql/$(ls /usr/lib/postgresql/ | head -1)/bin:$PATH"
          export PATH
          export PGCONNECT_TIMEOUT=5
          go test -cover -covermode=atomic -coverprofile=coverage-tests.txt -coverpkg=./... -json -v ./... | tee test-results.jsonl | go tool tparse -follow

      - name: Upload direct coverage
        uses: actions/upload-artifact@...
        with:
          name: coverage-tests
          path: coverage-tests.txt

  test-subprocess-coverage:
    name: Run tests with subprocess coverage
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@...
      - name: Set up Go
        uses: actions/setup-go@...
        with:
          go-version: "1.25.0"

      # PostgreSQL, etcd setup...

      - name: Build coverage binaries
        run: make build-coverage

      - name: Run tests for subprocess coverage
        run: |
          PATH="/usr/lib/postgresql/$(ls /usr/lib/postgresql/ | head -1)/bin:$PATH"
          export PATH
          export GOCOVERDIR="${GITHUB_WORKSPACE}/coverage/${{ github.run_id }}"
          mkdir -p "$GOCOVERDIR"
          export PGCONNECT_TIMEOUT=5
          # PrependBinToPath() will automatically use bin/cov/ binaries
          go test -v ./...

          # Validate coverage was collected
          if [ -z "$(ls -A $GOCOVERDIR)" ]; then
            echo "ERROR: No coverage files generated. Check that:"
            echo "  1. bin/cov/ binaries were built correctly"
            echo "  2. PrependBinToPath() is using bin/cov/"
            echo "  3. Tests actually execute subprocess binaries"
            exit 1
          fi

          # Convert to text format
          go tool covdata textfmt -i="$GOCOVERDIR" -o=coverage-subprocess.txt

      - name: Upload subprocess coverage
        uses: actions/upload-artifact@...
        with:
          name: coverage-subprocess
          path: coverage-subprocess.txt

  merge-and-upload-coverage:
    name: Merge and upload coverage
    needs: [test-direct-coverage, test-subprocess-coverage]
    runs-on: ubuntu-24.04
    if: always()
    steps:
      - uses: actions/checkout@...

      - name: Download coverage artifacts
        uses: actions/download-artifact@...
        with:
          pattern: coverage-*
          merge-multiple: true

      - name: Merge coverage
        run: ./scripts/merge-coverage.sh coverage-tests.txt coverage-subprocess.txt coverage.txt

      - name: Upload to Coveralls
        uses: coverallsapp/github-action@...
        with:
          file: coverage.txt
          format: golang
```

**Key aspects:**

- Two test jobs run in parallel (no time penalty)
- Subprocess coverage job validates that coverage was actually collected
- Early failure if coverage directory is empty (indicates setup bug)
- Each job uploads its coverage artifact
- Merge job downloads both and combines them
- Single upload to Coveralls

### Helper Script: merge-coverage.sh

**Location:** `scripts/merge-coverage.sh`

**Responsibilities:**

1. Parse Go coverage format from both inputs
2. Merge line-level coverage data
3. Handle overlapping coverage (union)
4. Output combined coverage.txt

**Initial implementation strategy:**

- Use Go's coverage tools if possible
- May need custom parsing for merging
- Should be reusable locally and in CI
- Needs to handle edge cases (missing files, conflicts)

**To be determined:**

- Best approach for merging (Go tool vs custom script)
- How to handle coverage conflicts
- Performance for large coverage datasets

## Gotchas and Best Practices

### 1. Binary Path References

**Issue:** Tests using absolute paths won't benefit from PATH manipulation.

**Check:**

```bash
grep -r "bin/pgctld" go/test/
grep -r "bin/multipooler" go/test/
```

**Solution:** Use bare binary names in `exec.Command()` (already done in current tests).

### 2. GOCOVERDIR Must Exist

Coverage binaries silently fail if GOCOVERDIR doesn't exist. Always `mkdir -p` before running tests.

### 3. Validate Coverage Collection

**Issue:** Empty coverage directory indicates a setup problem (PATH not configured, binaries not instrumented, etc.).

**Solution:** After running subprocess coverage tests, check that coverage files were generated:

```bash
if [ -z "$(ls -A $GOCOVERDIR)" ]; then
    echo "ERROR: No coverage files generated"
    exit 1
fi
```

This catches setup bugs early rather than silently uploading empty coverage.

### 4. Coverage File Proliferation

Each subprocess execution creates a new file in GOCOVERDIR. Long integration tests may generate hundreds of files. This is normal - `go tool covdata` handles merging efficiently.

### 5. Clean Between Runs

Old coverage files can pollute results. In local workflows:

```bash
rm -rf coverage && mkdir -p coverage
```

In CI, fresh workspace prevents this.

### 6. PATH Order Matters

Coverage binaries must come FIRST in PATH:

```bash
# Correct
PATH="$(pwd)/bin/cov:$PATH"

# Wrong - won't find coverage binaries
PATH="$PATH:$(pwd)/bin/cov"
```

The `PrependBinToPath()` function handles this correctly by design.

## Phase 2 Considerations (Future Work)

**Unified coverage collection** - capturing both test and subprocess coverage in a single pass.

Potential approaches:

1. Precompile test binaries with `-c` flag
2. Use `-test.gocoverdir` if/when it becomes available
3. Custom test runner that coordinates both coverage types

**Challenge:** `go test -cover` wants to control coverage itself, conflicts with GOCOVERDIR.

**Decision:** Defer until Phase 1 proves viable. Two-pass approach may be sufficient.

## Success Metrics

1. Subprocess coverage is collected and non-zero
2. CI runtime doesn't increase (parallel execution works)
3. Coverage reports to Coveralls successfully
4. Local reproduction works reliably
5. Coverage percentage increases (proves subprocess coverage matters)

## Next Steps

1. **Update Makefile** - Implement `build-coverage` changes
2. **Update PrependBinToPath()** - Add support for bin/cov/ directory
3. **Create merge script** - Initial implementation (may iterate)
4. **Test locally** - Verify subprocess coverage collection works
5. **Update CI workflow** - Implement parallel job structure
6. **Validate in CI** - Ensure coverage reports correctly
7. **Iterate on merge** - Refine merge strategy based on results
8. **Document findings** - Note any issues or improvements needed

## Open Questions

- Best tool/approach for merge-coverage.sh implementation?
- What's the expected coverage increase from subprocess data?
- Any edge cases in test execution we should watch for?
- Performance impact of instrumented binaries?
- Should PrependBinToPath() use auto-detection or explicit env var?

## References

- Go coverage documentation: https://go.dev/testing/coverage/
- `go tool covdata` usage: https://pkg.go.dev/cmd/covdata
- Current CI workflow: `.github/workflows/test-integration.yml`
- PrependBinToPath implementation: `go/tools/pathutil/pathutil.go`
