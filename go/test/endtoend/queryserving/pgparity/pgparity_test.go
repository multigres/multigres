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

package pgparity

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPgParity runs a curated corpus of query-parity files against direct
// PostgreSQL and the multigateway proxy. A record that passes on postgres but
// fails on multigateway fails the test — that's the parity contract.
//
// See the parser.go doc comment for the supported directives.
//
// Environment variables (optional):
//   - PGPARITY_CORPUS — directory containing .slt files (default: ./testdata)
//   - PGPARITY_FILES  — comma-separated filenames (without extension) under
//     the corpus to restrict the run to; useful for local iteration.
func TestPgParity(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping pgparity tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 30*time.Minute)

	primary := setup.GetPrimary(t)
	pgTarget := Target{
		Name: "postgres",
		Host: "localhost",
		Port: primary.Pgctld.PgPort,
		User: shardsetup.DefaultTestUser,
		Pass: shardsetup.TestPostgresPassword,
		DB:   "postgres",
	}
	mgwTarget := Target{
		Name: "multigateway",
		Host: "localhost",
		Port: setup.MultigatewayPgPort,
		User: shardsetup.DefaultTestUser,
		Pass: shardsetup.TestPostgresPassword,
		DB:   "postgres",
	}

	corpus := os.Getenv("PGPARITY_CORPUS")
	if corpus == "" {
		corpus = "testdata"
	}
	files, err := discoverTestFiles(corpus)
	if err != nil {
		t.Fatalf("discover test files in %s: %v", corpus, err)
	}
	if raw := os.Getenv("PGPARITY_FILES"); raw != "" {
		files = filterFiles(t, files, raw)
	}
	if len(files) == 0 {
		t.Fatalf("no pgparity test files found in %s (after filter)", corpus)
	}
	t.Logf("found %d pgparity test file(s) in %s", len(files), corpus)

	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			tf, err := ParseFile(file)
			if err != nil {
				t.Fatalf("parse %s: %v", file, err)
			}
			if len(tf.Records) == 0 {
				t.Skipf("no runnable records in %s", file)
			}

			// postgres runs first so its per-record pass/fail outcomes are
			// available when the multigateway run needs to decide whether a
			// failure is a divergence (pg pass + mgw fail) or a shared test
			// file issue (pg fail + mgw fail).
			pgPassed := runTarget(ctx, t, tf, pgTarget, pgTarget, nil)
			runTarget(ctx, t, tf, mgwTarget, pgTarget, pgPassed)
		})
	}
}

// runTarget executes a parsed test file against a single target inside a
// subtest and returns the set of record line numbers that passed.
//
// pgPassed == nil signals "this is the postgres run" — per-record failures
// are logged as likely corpus bugs but do not fail the test. When pgPassed is
// non-nil, this is the multigateway run and any failure of a record postgres
// passed is reported via t.Errorf as a real parity divergence.
//
// Schema resets always go through pgReset (direct postgres) so cleanup cannot
// be affected by a multigateway bug we're trying to detect.
func runTarget(ctx context.Context, parent *testing.T, tf *TestFile, target, pgReset Target, pgPassed map[int]bool) map[int]bool {
	parent.Helper()
	passed := make(map[int]bool)

	parent.Run(target.Name, func(t *testing.T) {
		if err := resetSchema(ctx, pgReset); err != nil {
			t.Fatalf("reset schema before %s: %v", target.Name, err)
		}
		result, err := RunFile(ctx, tf, target)
		if err != nil {
			t.Fatalf("run against %s: %v", target.Name, err)
		}
		t.Logf("%d/%d records passed", result.Passed, result.Total)

		for _, r := range result.Records {
			if r.Pass {
				passed[r.LineNo] = true
				continue
			}
			switch {
			case pgPassed == nil:
				t.Logf("postgres failed line %d (%s): %s (likely test file issue)",
					r.LineNo, r.Kind, r.Reason)
			case pgPassed[r.LineNo]:
				t.Errorf("divergence at line %d (%s): postgres passed but multigateway failed: %s",
					r.LineNo, r.Kind, r.Reason)
			default:
				t.Logf("multigateway failed line %d (%s): %s (postgres also failed)",
					r.LineNo, r.Kind, r.Reason)
			}
		}
	})
	return passed
}

// discoverTestFiles walks the corpus directory and returns all .slt files in
// stable (alphabetical) order so runs are reproducible. The `.test` extension
// is intentionally not supported — the repo `.gitignore` strips `*.test`
// because `go test -c` writes binaries with that suffix.
func discoverTestFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".slt" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// filterFiles restricts the discovered files to those whose basename (without
// extension) matches one of the comma-separated names in PGPARITY_FILES.
func filterFiles(t *testing.T, files []string, raw string) []string {
	t.Helper()
	wanted := make(map[string]bool)
	for name := range strings.SplitSeq(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		wanted[strings.TrimSuffix(name, ".slt")] = true
	}
	var out []string
	for _, f := range files {
		stem := strings.TrimSuffix(filepath.Base(f), ".slt")
		if wanted[stem] {
			out = append(out, f)
		}
	}
	return out
}
