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
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/multigres/multigres/go/tools/executil"
)

// target describes one endpoint the sqllogictest runner should execute a test
// file against. Both the direct-PostgreSQL target and the Multigres
// multigateway target are expressed as one of these.
type target struct {
	// Name is a human-readable label used in logs, reports, and results
	// JSON ("postgres", "multigateway").
	Name string
	// Host / Port / User / Password / Database are forwarded to sqllogictest
	// via its -h / -p / -u / -w / -d flags.
	Host     string
	Port     int
	User     string
	Password string
	Database string
	// Engine selects the sqllogictest-rs executor mode ("postgres" = simple
	// protocol, "postgres-extended" = extended). Defaults to "postgres".
	Engine string
}

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
// The runner always returns a *runResult; it never fails the test. Callers
// decide what to do with the outcome (typically: aggregate into the
// results.json report).
func runSqllogictest(ctx context.Context, t target, file string) *runResult {
	bin, lookErr := exec.LookPath("sqllogictest")
	if lookErr != nil {
		return &runResult{
			File:    file,
			ExecErr: fmt.Errorf("sqllogictest not found on PATH (install via `make tools`): %w", lookErr),
		}
	}

	if err := resetTarget(ctx, t); err != nil {
		return &runResult{
			File:    file,
			ExecErr: fmt.Errorf("reset target %s: %w", t.Name, err),
		}
	}

	engine := t.Engine
	if engine == "" {
		engine = "postgres"
	}

	args := []string{
		"-e", engine,
		"-h", t.Host,
		"-p", strconv.Itoa(t.Port),
		"-u", t.User,
		"-w", t.Password,
		"-d", t.Database,
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

	res := &runResult{
		File:     file,
		Duration: elapsed,
		Output:   truncateOutput(buf.String(), maxOutputBytes),
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

// resetSQL returns the public schema to an empty state. sqllogictest corpus
// files start with their own CREATE TABLE statements, so anything the
// previous invocation created would otherwise trigger "already exists" errors
// on the next run — especially since every file is run four times (2 targets
// × 2 protocols) against the same shared database.
const resetSQL = `DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;`

// resetTarget connects to the target and clears the public schema. A fresh
// connection is used each time so session state (search_path, transaction
// state, prepared statements) can't leak in either direction.
func resetTarget(ctx context.Context, t target) error {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		t.Host, t.Port, t.User, t.Password, t.Database)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, resetSQL); err != nil {
		return fmt.Errorf("reset public schema: %w", err)
	}
	return nil
}
