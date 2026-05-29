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

package pgproto

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/tools/executil"
)

// runResult captures the outcome of running one data file against one target.
//
// pgproto has no embedded oracle: a data file describes messages to send, and
// pgproto prints the server's response trace to stderr. Correctness is decided
// differentially — PostgreSQL's trace is the baseline, and the multigateway is
// expected to reproduce it byte-for-byte (after normalization). So a runResult
// records the trace and whether pgproto executed cleanly; the pass/divergence
// verdict is computed later by comparing the two targets' traces.
type runResult struct {
	// File is the path to the data file (relative to corpus root if possible).
	File string
	// Ran is true iff pgproto connected and processed the data file to EOF
	// (exit 0). A server-side ErrorResponse does NOT clear this — pgproto
	// prints the error into the trace and keeps going, exiting 0. Only a
	// failure to connect, a malformed data file, or a socket error makes
	// pgproto exit non-zero, which leaves Ran false and populates ExecErr.
	Ran bool
	// TimedOut is true if the per-file context deadline killed pgproto.
	TimedOut bool
	// Duration is wall time spent running this file against this target.
	Duration time.Duration
	// Trace is the normalized FE/BE message trace (see normalizeTrace). This
	// is the value compared across targets.
	Trace string
	// RawTrace is the un-normalized stdout+stderr, kept for diagnostics on
	// failure. Truncated before being placed into the on-disk report.
	RawTrace string
	// ExecErr is non-nil when pgproto could not run or exited non-zero
	// (binary missing, connection refused/auth failure, malformed data file,
	// socket read error). This is a harness/corpus problem, distinct from a
	// protocol divergence.
	ExecErr error
}

// runPgproto invokes the pgproto binary against one file on one target. The
// binary must be discoverable on PATH (it is built by `make tools` into
// $repo/bin and shardsetup prepends that to PATH).
//
// Before each run the target's public schema is dropped and recreated so the
// data file sees an empty database (each .pgproto file issues its own CREATE
// statements and assumes no leftover state from the previous file). resetter is
// supplied by the caller so the same long-lived connection is reused across all
// files — see suiteutil.SchemaResetter for why connection reuse matters against
// pooled multipooler backends.
//
// pgproto takes the password via the PGPASSWORD environment variable: it builds
// its libpq conninfo from host/port/user/dbname only (no password field) and
// relies on libpq to read PGPASSWORD. We never pass the password on argv.
//
// The runner always returns a *runResult; it never fails the test.
func runPgproto(ctx context.Context, t suiteutil.Target, resetter *suiteutil.SchemaResetter, file string) *runResult {
	bin, lookErr := exec.LookPath("pgproto")
	if lookErr != nil {
		return &runResult{
			File:    file,
			ExecErr: fmt.Errorf("pgproto not found on PATH (build via `make tools`): %w", lookErr),
		}
	}

	if resetter != nil {
		if err := resetter.ResetIfDirty(ctx); err != nil {
			return &runResult{
				File:    file,
				ExecErr: fmt.Errorf("reset target %s: %w", t.Name, err),
			}
		}
	}

	cmd := executil.Command(ctx, bin,
		"-h", t.Host,
		"-p", strconv.Itoa(t.Port),
		"-u", t.User,
		"-d", t.DB,
		"-f", file,
	)
	// libpq reads the password from PGPASSWORD; pgproto has no password flag.
	cmd.AddEnv("PGPASSWORD=" + t.Pass)

	// pgproto writes the trace to stderr and only ever writes to stdout on a
	// usage error (which cannot happen with our fixed argv). Capture both so a
	// surprise on stdout is not lost, but the trace is what we compare.
	var stderrBuf, stdoutBuf bytes.Buffer
	cmd.Cmd.Stdout = &stdoutBuf
	cmd.Cmd.Stderr = &stderrBuf

	start := time.Now()
	err := cmd.Run()
	elapsed := time.Since(start)

	// Every data file issues SQL, so the schema may now be dirty.
	if resetter != nil {
		resetter.MarkDirty()
	}

	raw := stderrBuf.String()
	if stdoutBuf.Len() > 0 {
		raw = stdoutBuf.String() + raw
	}

	res := &runResult{
		File:     file,
		Duration: elapsed,
		Trace:    normalizeTrace(raw),
		RawTrace: suiteutil.TruncateOutput(raw, maxOutputBytes),
	}

	switch {
	case err == nil:
		res.Ran = true
	case ctx.Err() == context.DeadlineExceeded:
		res.TimedOut = true
		res.ExecErr = fmt.Errorf("pgproto timed out against %s", t.Name)
	case suiteutil.IsExitError(err):
		// Non-zero exit: pgproto could not complete (connect/auth failure,
		// malformed data file, or socket error). The captured trace usually
		// names the cause (e.g. "Failed to connect to ..."). Surface it as a
		// harness/corpus error, not a protocol divergence.
		res.ExecErr = fmt.Errorf("pgproto exited non-zero against %s: %w (trace: %s)",
			t.Name, err, suiteutil.FirstLine(raw))
	default:
		res.ExecErr = fmt.Errorf("pgproto exec error against %s: %w", t.Name, err)
	}

	return res
}

// normalizeTrace canonicalizes a pgproto trace for cross-target comparison.
//
// pgproto's trace is already remarkably stable: DataRow / RowDescription /
// ParameterStatus print only the message name (the payload is discarded), and
// BackendKeyData (PID + secret key) is consumed by libpq during startup before
// the trace begins, so it never appears. There is therefore no run-varying
// per-connection data (PID, secret, timestamp) to strip.
//
// What remains target-specific only on failure — the "Failed to connect to
// host=… port=…" line embeds the per-target port — never reaches comparison,
// because a failed connection leaves runResult.Ran false and is handled as an
// ExecErr instead of being diffed.
//
// Beyond trimming whitespace and blank lines, the one substantive rule is
// reduceErrorLine: Error/NoticeResponse lines are compared by SQLSTATE only.
// See that function for why the message text, position, and source-location
// fields are intentionally dropped.
func normalizeTrace(s string) string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimRight(line, " \t\r")
		if line == "" {
			continue
		}
		out = append(out, reduceErrorLine(line))
	}
	return strings.Join(out, "\n")
}

// errorLineRe matches a pgproto-rendered Error/NoticeResponse trace line and
// captures (1) the message kind prefix and (2) the SQLSTATE in the `C` field.
// pgproto prints diagnostic fields as space-separated "<letter> <value>" pairs;
// PostgreSQL always emits the code before the free-text message, so the first
// " C <5 chars>" is the SQLSTATE.
var errorLineRe = regexp.MustCompile(`^(<= BE (?:Error|Notice)Response)\(.*?\sC ([0-9A-Za-z]{5})\b.*\)$`)

// reduceErrorLine collapses an Error/NoticeResponse trace line to just its
// message kind and SQLSTATE code, dropping the human-readable message, the
// character position, and PostgreSQL's backend source-location fields
// (F=file, L=line, R=routine, W=where, etc.).
//
// The SQLSTATE is the only part of an error a proxy must preserve. The other
// fields are not reproducible by the multigateway: F/L/R are PostgreSQL's
// internal C-source location, which the proxy has no access to, and for errors
// the multigateway raises itself (e.g. parse failures from its own parser) the
// wording and character position legitimately differ from PostgreSQL's. Diffing
// those would only ever surface noise, never a real proxy bug — whereas a
// SQLSTATE mismatch (e.g. 57014 vs 08P01) is a genuine divergence and is kept.
//
// Lines that are not Error/NoticeResponse (or, defensively, an error line with
// no parseable C field) are returned unchanged.
func reduceErrorLine(line string) string {
	m := errorLineRe.FindStringSubmatch(line)
	if m == nil {
		return line
	}
	return fmt.Sprintf("%s(C %s)", m[1], m[2])
}

// maxOutputBytes bounds how much raw trace we keep per file in the final
// report. Diverging files print enough to diagnose; huge traces serve no one.
const maxOutputBytes = 8 * 1024
