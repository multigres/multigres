// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqllogictest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/tools/executil"
)

// runResult captures the outcome of running one .test/.slt file against one
// target. Divergences between two runResults are not fatal: the harness
// tracks and reports pass rates over time rather than failing hard.
type runResult struct {
	// File is the path to the .test file (relative to corpus root if possible).
	File string
	// Passed is true iff the runner exited 0.
	Passed bool
	// TimedOut is true if the per-file context deadline killed the runner.
	TimedOut bool
	// Duration is wall time spent running this file against this target.
	Duration time.Duration
	// Output is the captured stdout+stderr. Truncated before being placed
	// into the on-disk report so a single pathological file doesn't bloat
	// results.json.
	Output string
	// ExecErr is non-nil if sqllogictest failed to exec at all (binary
	// missing, etc). This is distinct from a non-zero exit, which just
	// means the test failed.
	ExecErr error
}

// runSqllogictest invokes the sqllogictest binary against one file on one
// target. The binary must be discoverable on PATH (it is installed by
// `make tools` into $repo/bin and shardsetup prepends that to PATH).
//
// Before each run the target's public schema is dropped and recreated so the
// corpus file sees an empty database (each .test file starts with its own
// CREATE TABLE statements and assumes no leftover state from the previous
// file or previous protocol).
//
// resetter performs the per-file `DROP SCHEMA public CASCADE; CREATE SCHEMA
// public; …` cycle. It is supplied by the caller so the same long-lived
// connection is reused across all files. Without that reuse, each fresh pgx
// connection-per-call generates a wave of pg_class / pg_namespace
// invalidation messages that pile up against pooled multipooler backends
// the simple-protocol path eventually lands on, producing a multi-second
// catalog-cache rebuild on first use of those connections (most visibly:
// `slt_good_125` simple hitting `statement_timeout` in CI).
//
// The runner always returns a *runResult; it never fails the test. Callers
// decide what to do with the outcome (typically: aggregate into the
// results.json report).
//
// engine selects the sqllogictest-rs executor mode: "postgres" for the simple
// wire protocol and "postgres-extended" for the extended one. An empty string
// defaults to "postgres".
func runSqllogictest(ctx context.Context, t suiteutil.Target, resetter *suiteutil.SchemaResetter, engine, file string) *runResult {
	bin, lookErr := exec.LookPath("sqllogictest")
	if lookErr != nil {
		return &runResult{
			File:    file,
			ExecErr: fmt.Errorf("sqllogictest not found on PATH (install via `make tools`): %w", lookErr),
		}
	}

	if err := resetter.ResetIfDirty(ctx); err != nil {
		return &runResult{
			File:    file,
			ExecErr: fmt.Errorf("reset target %s: %w", t.Name, err),
		}
	}

	if engine == "" {
		engine = "postgres"
	}

	args := []string{
		"-e", engine,
		"-h", t.Host,
		"-p", strconv.Itoa(t.Port),
		"-u", t.User,
		"-w", t.Pass,
		"-d", t.DB,
		"--color", "never",
		file,
	}

	cmd := executil.Command(ctx, bin, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	start := time.Now()
	err := cmd.Run()
	elapsed := time.Since(start)

	output := buf.String()

	// Mark the resetter dirty unless this run never reached the database.
	// sqllogictest exits with "failed to parse" before opening any
	// connection when a .test file uses syntax it doesn't recognise (e.g.
	// `onlyif mysql # …`); those files leave the schema untouched, so the
	// next file can reuse the prior reset. Skipping those resets keeps the
	// pg_namespace/pg_class invalidation queue from piling up against
	// pooled multipooler backends — see SchemaResetter for the failure
	// mode this prevents (slt_good_125's simple-protocol
	// statement_timeout in CI).
	if !strings.Contains(output, "failed to parse") {
		resetter.MarkDirty()
	}

	res := &runResult{
		File:     file,
		Duration: elapsed,
		Output:   truncateOutput(output, maxOutputBytes),
	}

	switch {
	case err == nil:
		res.Passed = true
	case ctx.Err() == context.DeadlineExceeded:
		res.TimedOut = true
	case isExitError(err):
		// Non-zero exit: test failure — the captured output names the
		// offending record. Treat as expected "not passed".
	default:
		res.ExecErr = err
	}

	return res
}

// maxOutputBytes bounds how much captured runner output we keep per file in
// the final report. Failing files often print enough context to diagnose;
// huge walls of diff text serve no one.
const maxOutputBytes = 4 * 1024

func truncateOutput(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "\n... (output truncated)"
}

func isExitError(err error) bool {
	var ee *exec.ExitError
	return errors.As(err, &ee)
}
