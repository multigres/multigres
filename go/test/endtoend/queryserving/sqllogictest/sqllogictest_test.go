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
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

const (
	// standalonePassword is distinct from the shardsetup cluster password so
	// a mixup between targets is obvious.
	standalonePassword = "sqllogictest"

	// defaultPerFileTimeout bounds how long any single .test file can run
	// against one target on one protocol. A single slow file cannot starve
	// the whole suite.
	defaultPerFileTimeout = 5 * time.Minute

	// defaultBuildTimeout bounds the PostgreSQL source build.
	defaultBuildTimeout = 10 * time.Minute
)

// protocolConfig pairs a short human name (used in report keys) with the
// sqllogictest-rs engine string. Both configurations run against both
// targets, producing four invocations per file.
type protocolConfig struct {
	// Key labels the suite in results.json: "SQLLogicTest-" + Key.
	Key string
	// Engine is passed via -e to sqllogictest-rs.
	Engine string
}

var protocols = []protocolConfig{
	{Key: "simple", Engine: "postgres"},
	{Key: "extended", Engine: "postgres-extended"},
}

// TestPostgreSQLSqlLogicTest runs the pinned sqllogictest corpus against
// both a standalone PostgreSQL and the Multigres multigateway, under both
// the simple and extended PostgreSQL wire protocols. Results are written
// to results.json (one suite per protocol) plus a markdown summary.
//
// This test is deliberately tolerant of failures. It does not fail the Go
// test on proxy divergence or upstream corpus failures; it records per-
// file, per-protocol pass/fail counts over time (like pgregresstest). CI
// compares against a cached baseline; regressions surface there.
//
// Skipped by default. Set RUN_SQLLOGICTEST=1 to run.
//
// Environment variables:
//
//	RUN_SQLLOGICTEST=1        — enable the test (required)
//	SLT_CORPUS_DIR=<dir>      — use an external corpus instead of testdata/
//	SLT_CORPUS_GLOB=<glob>    — scope which corpus files run (default: **/*.slt)
//	SLT_PER_FILE_TIMEOUT=<d>  — Go duration, e.g. "90s" (default: 5m)
func TestPostgreSQLSqlLogicTest(t *testing.T) {
	if os.Getenv("RUN_SQLLOGICTEST") != "1" {
		t.Skip("skipping sqllogictest: set RUN_SQLLOGICTEST=1 to run")
	}

	if !utils.HasPostgreSQLBinaries() {
		t.Skip("skipping: PostgreSQL binaries not found on PATH")
	}

	if _, err := exec.LookPath("sqllogictest"); err != nil {
		t.Fatalf("sqllogictest binary not found on PATH; run `make tools` to install it: %v", err)
	}

	if err := pgbuilder.CheckBuildDependencies(t); err != nil {
		t.Skipf("Build dependencies not available: %v", err)
	}

	perFileTimeout := defaultPerFileTimeout
	if v := os.Getenv("SLT_PER_FILE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("SLT_PER_FILE_TIMEOUT=%q: %v", v, err)
		}
		perFileTimeout = d
	}

	// Phase 1: build PostgreSQL 17.6 from source (cached across runs).
	buildCtx := utils.WithTimeout(t, defaultBuildTimeout)
	builder := pgbuilder.New(t)
	t.Cleanup(builder.Cleanup)

	t.Logf("Phase 1: ensuring PostgreSQL source...")
	if err := builder.EnsureSource(t, buildCtx); err != nil {
		t.Fatalf("EnsureSource: %v", err)
	}
	t.Logf("Phase 1: building PostgreSQL...")
	if err := builder.Build(t, buildCtx); err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Phase 2: prepend the built bin directory to PATH so shardsetup uses
	// the same binaries as the standalone baseline.
	pgBinDir := builder.BinDir()
	origPath := os.Getenv("PATH")
	os.Setenv("PATH", pgBinDir+string(os.PathListSeparator)+origPath)
	t.Cleanup(func() { os.Setenv("PATH", origPath) })

	// Phase 3: resolve corpus. Default: shallow-clone the upstream
	// gregrahn/sqllogictest mirror pinned at CorpusCommit (upstream is
	// quad-licensed GPL / BSD / MIT / CC0 — we consume under MIT). Override
	// with SLT_CORPUS_DIR to point at a local directory.
	t.Logf("Phase 3: resolving sqllogictest corpus...")
	corpusRoot, err := resolveCorpusDir(t, buildCtx)
	if err != nil {
		t.Fatalf("resolveCorpusDir: %v", err)
	}
	files, err := listCorpusFiles(corpusRoot)
	if err != nil {
		t.Fatalf("listCorpusFiles: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no files matched SLT_CORPUS_GLOB in %s", corpusRoot)
	}
	t.Logf("Phase 3: corpus = %d files from %s (glob=%q, commit=%s)",
		len(files), corpusRoot, firstNonEmpty(os.Getenv("SLT_CORPUS_GLOB"), DefaultCorpusGlob), CorpusCommit)

	// Phase 4: start the baseline standalone postgres.
	t.Logf("Phase 4: starting standalone PostgreSQL...")
	standalone, err := pgbuilder.StartStandalone(t, buildCtx, builder, standalonePassword)
	if err != nil {
		t.Fatalf("StartStandalone: %v", err)
	}
	t.Cleanup(func() { _ = standalone.Stop() })

	// Phase 5: bring up the multigres cluster + multigateway.
	t.Logf("Phase 5: starting multigres cluster...")
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	pgTarget := target{
		Name:     "postgres",
		Host:     "127.0.0.1",
		Port:     standalone.Port,
		User:     standalone.User,
		Password: standalone.Password,
		Database: standalone.Database,
	}
	mgTarget := target{
		Name:     "multigateway",
		Host:     "127.0.0.1",
		Port:     setup.MultigatewayPgPort,
		User:     shardsetup.DefaultTestUser,
		Password: shardsetup.TestPostgresPassword,
		Database: "postgres",
	}

	// Phase 6: for each file, run both targets under both protocols. Four
	// invocations per file. Honors the overall test deadline so a timeout
	// still produces a coherent partial report.
	t.Logf("Phase 6: running %d files × %d protocols × 2 targets...", len(files), len(protocols))

	overall := t.Context()
	startedAt := time.Now()

	// One {pgResults, mgResults} slice per protocol, in the same order as
	// `protocols`. File order is also preserved within each slice.
	perProto := make(map[string]*protoResults, len(protocols))
	for _, p := range protocols {
		perProto[p.Key] = &protoResults{}
	}

	var (
		timedOut   bool
		ranAgainst int
	)
	for i, f := range files {
		select {
		case <-overall.Done():
			t.Logf("overall deadline reached after %d/%d files; stopping run loop", i, len(files))
			timedOut = true
		default:
		}
		if timedOut {
			break
		}

		ranAgainst++

		for _, p := range protocols {
			pgRes := runOne(overall, perFileTimeout, pgTarget, p.Engine, f)
			mgRes := runOne(overall, perFileTimeout, mgTarget, p.Engine, f)
			perProto[p.Key].pg = append(perProto[p.Key].pg, pgRes)
			perProto[p.Key].mg = append(perProto[p.Key].mg, mgRes)
		}

		if (i+1)%25 == 0 || (i+1) == len(files) {
			t.Logf("  progress: %d/%d files", i+1, len(files))
			for _, p := range protocols {
				pr := perProto[p.Key]
				t.Logf("    [%s] postgres pass=%d, multigateway pass=%d",
					p.Key, passedCount(pr.pg), passedCount(pr.mg))
			}
		}
	}

	t.Logf("Phase 6: ran %d of %d files (timed_out=%v)", ranAgainst, len(files), timedOut)

	// Phase 7: build one suiteReport per protocol and persist the merged
	// array. The array shape keeps detect-regressions.sh happy and lets
	// regression tracking be scoped per protocol.
	reports := make([]*suiteReport, 0, len(protocols))
	for _, p := range protocols {
		pr := perProto[p.Key]
		r := newSuiteReport("SQLLogicTest-"+p.Key, corpusRoot, pr.pg, pr.mg, startedAt, timedOut)
		r.logSummary(t)
		reports = append(reports, r)
	}

	if jsonPath, err := writeJSON(builder.OutputDir, reports); err != nil {
		t.Logf("warning: failed to write results.json: %v", err)
	} else {
		t.Logf("results.json written to: %s", jsonPath)
	}
	if _, err := writeMarkdownSummary(t, builder.OutputDir, reports); err != nil {
		t.Logf("warning: failed to write markdown summary: %v", err)
	}

	// No t.Fatal / t.Error based on pass rate. Tracking is the job here;
	// regression gating happens in CI against the cached baseline.
}

// protoResults groups per-protocol result slices so the outer loop can
// accumulate them in file order.
type protoResults struct {
	pg []*runResult
	mg []*runResult
}

// runOne invokes sqllogictest against a single target under a single
// protocol for one file, with its own bounded context.
func runOne(parent context.Context, perFileTimeout time.Duration, base target, engine, file string) *runResult {
	ctx, cancel := context.WithTimeout(parent, perFileTimeout)
	defer cancel()
	t := base
	t.Engine = engine
	return runSqllogictest(ctx, t, file)
}

func passedCount(results []*runResult) int {
	n := 0
	for _, r := range results {
		if r.Passed {
			n++
		}
	}
	return n
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
