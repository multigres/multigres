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

package pgregresstest

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// This file implements the transaction-wrapping transform behind
// ExternalExtension.WrapTransactions.
//
// Why wrapping works: multigateway uses transaction pooling, so consecutive
// autocommit statements from one client session land on different pooled
// backends (see scatterconn — only BEGIN, temp tables, WITH HOLD cursors, and
// session advisory locks reserve a dedicated backend). Suites that assert on
// BACKEND-LOCAL state across statements — hypopg's hypothetical indexes,
// pgTAP's session-temp plan tables, pgsql-http's curl option state — therefore
// can't run autocommit through the gateway. But inside an explicit transaction
// the gateway reserves ONE backend for its whole duration (ReasonTransaction),
// which is exactly how index_advisor's BEGIN/ROLLBACK-wrapped tests and
// pg_partman's transaction-wrapped pgTAP files already pass. The transform
// applies that proven shape to suites whose upstream files are autocommit:
// it materializes a copy of the suite with each test file wrapped in
// BEGIN … COMMIT, and the SAME insertions applied to the expected .out files
// (psql under pg_regress echoes input lines, so input and expected stay in
// lockstep).
//
// The wrap must break around three things, all detected line-wise:
//
//   - \connect: starts a new client session; the wrap closes before it and
//     reopens after.
//   - The file's OWN transaction blocks (BEGIN;/COMMIT;/ROLLBACK; at column
//     zero): passed through untouched — the wrap closes before their BEGIN and
//     reopens after their COMMIT/ROLLBACK, so nesting never occurs.
//   - Statements that cannot run inside a transaction block, currently VACUUM
//     (the only one the wrapped suites contain): the wrap commits, lets the
//     statement run autocommit, and reopens. Statements multigateway rejects
//     before they reach a backend (CREATE DATABASE, CREATE SERVER, …) do NOT
//     need a break — the backend transaction never sees them — and keeping
//     them inside avoids an unpinned gap.
//
// `\set ON_ERROR_ROLLBACK on` is emitted once at the top of each wrapped file:
// upstream's autocommit files contain statements that are EXPECTED to error
// (permission-denied probes, intentional timeouts), and in autocommit an error
// only fails that statement. Inside the wrap it would abort the whole
// transaction; ON_ERROR_ROLLBACK makes psql guard each statement with a
// savepoint so the error's output stays identical to autocommit and the
// transaction lives on.
//
// Limitations (documented, deliberate): boundary statements are matched on
// single lines at column zero outside dollar-quoted bodies. That holds for
// every wrapped suite (their VACUUM/BEGIN/COMMIT lines are single-line,
// top-level statements); a future suite that line-breaks a VACUUM would need
// the transform extended.

// WrapStatement is one statement the wrap transform injects right after each
// transaction REOPEN (after a VACUUM break or a \connect — not at the very top
// of the file, where the suite's extension may not even exist yet). SQL is the
// statement line for the materialized .sql file; Output is the exact psql
// result block (echo line excluded, trailing blank line included) spliced into
// the materialized expected files at the same point. hypopg injects
// `SELECT hypopg_reset();` so a backend acquired after a break starts with no
// leftover hypothetical indexes regardless of pool history.
type WrapStatement struct {
	SQL    string
	Output string
}

// TextRewrite is a literal substitution applied to both the .sql and expected
// .out copies before wrapping. Used to redirect a hard-coded live-internet
// endpoint at a harness-provisioned local server (pgsql-http's
// https://postgis.net TLS probes), keeping the suite hermetic while exercising
// the same client code path. The echoed statement text changes identically on
// both sides, so the substitution is diff-neutral.
type TextRewrite struct {
	Old string
	New string
}

var (
	wrapConnectRe = regexp.MustCompile(`^\\(connect|c)\b`)
	// The file's own top-level transaction control. Anchored to column zero and
	// matched outside dollar quotes only, so plpgsql bodies (BEGIN without a
	// semicolon) never match.
	wrapUserBeginRe = regexp.MustCompile(`(?i)^(BEGIN|START TRANSACTION)\s*;\s*$`)
	wrapUserEndRe   = regexp.MustCompile(`(?i)^(COMMIT|ROLLBACK|END)\s*;\s*$`)
	// Statements that must not run inside a transaction block AND actually
	// reach the backend through multigateway. Single-line form only (see file
	// header).
	wrapNonTxnRe = regexp.MustCompile(`(?i)^VACUUM\b.*;\s*$`)
)

// wrapLines applies the transaction-wrap state machine to one file's lines.
// The same function transforms the .sql input (isExpected=false) and each
// expected .out variant (isExpected=true): boundaries are detected by line
// content, which is identical in both because psql echoes input lines.
// stopPatterns, when any line matches, close the wrap permanently — the rest
// of the file runs autocommit (see ExternalExtension.WrapStopPatterns).
func wrapLines(lines []string, setup []WrapStatement, stopPatterns []*regexp.Regexp, isExpected bool) []string {
	out := make([]string, 0, len(lines)+8)

	emitSetup := func() {
		for _, s := range setup {
			out = append(out, s.SQL)
			if isExpected && s.Output != "" {
				out = append(out, strings.Split(strings.TrimSuffix(s.Output, "\n"), "\n")...)
			}
		}
	}

	out = append(out, `\set ON_ERROR_ROLLBACK on`, "BEGIN;")
	open := true
	userTxn := false
	inDollar := false
	stopped := false

	matchesStop := func(line string) bool {
		for _, re := range stopPatterns {
			if re.MatchString(line) {
				return true
			}
		}
		return false
	}

	for _, line := range lines {
		// Track dollar-quoting so statement keywords inside function/DO bodies
		// never register as boundaries. Toggling per $$ occurrence is enough
		// for the suites we wrap (no nested or tagged dollar quotes at column
		// zero around boundary keywords).
		toggles := strings.Count(line, "$$")

		boundary := !inDollar && !stopped
		switch {
		case boundary && matchesStop(line):
			if open {
				out = append(out, "COMMIT;")
			}
			out = append(out, line)
			open, stopped = false, true
		case boundary && wrapConnectRe.MatchString(line):
			if open {
				out = append(out, "COMMIT;")
			}
			out = append(out, line)
			out = append(out, "BEGIN;")
			emitSetup()
			open, userTxn = true, false
		case boundary && open && !userTxn && wrapUserBeginRe.MatchString(line):
			out = append(out, "COMMIT;", line)
			open, userTxn = false, true
		case boundary && userTxn && wrapUserEndRe.MatchString(line):
			out = append(out, line, "BEGIN;")
			emitSetup()
			open, userTxn = true, false
		case boundary && open && !userTxn && wrapNonTxnRe.MatchString(line):
			out = append(out, "COMMIT;", line, "BEGIN;")
			emitSetup()
		default:
			out = append(out, line)
		}

		if toggles%2 == 1 {
			inDollar = !inDollar
		}
	}
	if open {
		out = append(out, "COMMIT;")
	}
	return out
}

// transformSuiteFile reads one suite file, applies the extension's
// TextRewrites and (when enabled) the transaction wrap, and writes the result
// to dst.
func transformSuiteFile(ext ExternalExtension, src, dst string, isExpected bool) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	content := string(data)
	for _, rw := range ext.TextRewrites {
		content = strings.ReplaceAll(content, rw.Old, rw.New)
	}
	if ext.WrapTransactions {
		stopPatterns := make([]*regexp.Regexp, 0, len(ext.WrapStopPatterns))
		for _, p := range ext.WrapStopPatterns {
			re, err := regexp.Compile(p)
			if err != nil {
				return fmt.Errorf("invalid WrapStopPatterns entry %q: %w", p, err)
			}
			stopPatterns = append(stopPatterns, re)
		}
		trailingNL := strings.HasSuffix(content, "\n")
		lines := strings.Split(strings.TrimSuffix(content, "\n"), "\n")
		lines = wrapLines(lines, ext.WrapSetupSQL, stopPatterns, isExpected)
		content = strings.Join(lines, "\n")
		if trailingNL {
			content += "\n"
		}
	}
	return os.WriteFile(dst, []byte(content), 0o644)
}

// writePgPassFile writes a temporary .pgpass file (0600, as libpq requires)
// holding the admin password plus the extension's PgPassUsers entries, and
// returns its path. Entries match any port/database so they survive the
// per-run random gateway port.
func writePgPassFile(adminPassword string, users []PgPassUser) (string, error) {
	dir, err := os.MkdirTemp("", "pgregress-pgpass-")
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "localhost:*:*:postgres:%s\n", adminPassword)
	for _, u := range users {
		fmt.Fprintf(&sb, "localhost:*:*:%s:%s\n", u.Name, u.Password)
	}
	path := filepath.Join(dir, "pgpass")
	if err := os.WriteFile(path, []byte(sb.String()), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

// materializeTransformedSuite writes a transformed copy of an extension's
// pg_regress fixtures (sql/<test>.sql plus every expected/<test>*.out variant)
// under destDir and returns destDir, which then serves as both --inputdir and
// --expecteddir. Only the selected tests are copied. The patch pipeline diffs
// results against the transformed expected files, so patches stay scoped to
// genuine multigres divergences rather than the mechanical wrap lines.
func materializeTransformedSuite(ext ExternalExtension, testDir, expectedDir string, tests []string, destDir string) (string, error) {
	if err := os.MkdirAll(filepath.Join(destDir, "sql"), 0o755); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Join(destDir, "expected"), 0o755); err != nil {
		return "", err
	}
	for _, name := range tests {
		src := filepath.Join(testDir, "sql", name+".sql")
		if err := transformSuiteFile(ext, src, filepath.Join(destDir, "sql", name+".sql"), false); err != nil {
			return "", fmt.Errorf("transform %s: %w", src, err)
		}
		for _, variant := range expectedVariants(expectedDir, name) {
			dst := filepath.Join(destDir, "expected", filepath.Base(variant))
			if err := transformSuiteFile(ext, variant, dst, true); err != nil {
				return "", fmt.Errorf("transform %s: %w", variant, err)
			}
		}
	}
	return destDir, nil
}
