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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pgconstants "github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/test/utils"
)

const (
	// standalonePassword is distinct from the shardsetup cluster password so a
	// mixup between targets is obvious.
	standalonePassword = "pgproto"

	// defaultPerFileTimeout bounds how long any single data file can run
	// against one target. A single pathological file cannot starve the suite.
	defaultPerFileTimeout = 2 * time.Minute

	// defaultBuildTimeout bounds the PostgreSQL source build.
	defaultBuildTimeout = 10 * time.Minute

	// defaultStandaloneStartTimeout bounds initdb + postgres launch +
	// pg_isready polling for the baseline standalone instance.
	defaultStandaloneStartTimeout = 2 * time.Minute
)

// TestPgProtoConformance runs the in-tree pgproto corpus against both a
// standalone PostgreSQL and the Multigres multigateway, and records where the
// multigateway's response trace diverges from PostgreSQL's. PostgreSQL is the
// oracle: a file "passes" on the multigateway when its normalized trace matches
// PostgreSQL's exactly. Results are written to results.json plus a markdown
// summary.
//
// This test is deliberately tolerant of failures. It does not fail the Go test
// on proxy divergence; it records per-file match/mismatch counts. CI inspects
// results.json and alerts when any file produces a postgres baseline but the
// multigateway trace differs.
//
// Skipped by default. Set RUN_EXTENDED_QUERY_SERVING_TESTS=1 to run (this is a
// correctness suite, gated by the same env var / PR label as sqllogictest and
// pgregresstest — not a benchmark).
//
// Environment variables:
//
//	RUN_EXTENDED_QUERY_SERVING_TESTS=1  — enable the test (required)
//	PGPROTO_CORPUS_DIR=<dir>            — use an external corpus instead of testdata/
//	PGPROTO_CORPUS_GLOB=<glob>          — scope which corpus files run (default: **/*.pgproto)
//	PGPROTO_PER_FILE_TIMEOUT=<d>        — Go duration, e.g. "30s" (default: 2m)
func TestPgProtoConformance(t *testing.T) {
	suiteutil.SkipUnlessEnabled(t, suiteutil.EnvRunExtendedQueryServingTests)

	if _, err := exec.LookPath("pgproto"); err != nil {
		t.Fatalf("pgproto binary not found on PATH; run `make tools` to build it: %v", err)
	}

	if err := pgbuilder.CheckBuildDependencies(t); err != nil {
		t.Skipf("Build dependencies not available: %v", err)
	}

	perFileTimeout := defaultPerFileTimeout
	if v := os.Getenv("PGPROTO_PER_FILE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("PGPROTO_PER_FILE_TIMEOUT=%q: %v", v, err)
		}
		perFileTimeout = d
	}

	// Phase 1: build PostgreSQL from source (cached across runs).
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

	// Phase 2: prepend the built bin directory to PATH so shardsetup uses the
	// same binaries as the standalone baseline.
	pgBinDir := builder.BinDir()
	origPath := os.Getenv("PATH")
	os.Setenv("PATH", pgBinDir+string(os.PathListSeparator)+origPath)
	t.Cleanup(func() { os.Setenv("PATH", origPath) })

	// Phase 3: resolve the in-tree corpus.
	t.Logf("Phase 3: resolving pgproto corpus...")
	corpusRoot, err := resolveCorpusDir()
	if err != nil {
		t.Fatalf("resolveCorpusDir: %v", err)
	}
	files, err := listCorpusFiles(corpusRoot)
	if err != nil {
		t.Fatalf("listCorpusFiles: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no files matched PGPROTO_CORPUS_GLOB in %s", corpusRoot)
	}
	t.Logf("Phase 3: corpus = %d files from %s", len(files), corpusRoot)

	// Phase 4: start the baseline standalone postgres.
	t.Logf("Phase 4: starting standalone PostgreSQL...")
	standaloneCtx := utils.WithTimeout(t, defaultStandaloneStartTimeout)
	standalone, err := pgbuilder.StartStandalone(t, standaloneCtx, builder, standalonePassword)
	if err != nil {
		t.Fatalf("StartStandalone: %v", err)
	}
	t.Cleanup(func() { _ = standalone.Stop() })

	// Phase 5: bring up the multigres cluster + multigateway.
	t.Logf("Phase 5: starting multigres cluster...")
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	pgTarget := suiteutil.Target{
		Name: "postgres",
		Host: "127.0.0.1",
		Port: standalone.Port,
		User: standalone.User,
		Pass: standalone.Password,
		DB:   standalone.Database,
	}
	mgTarget := suiteutil.Target{
		Name: "multigateway",
		Host: "127.0.0.1",
		Port: setup.MultigatewayPgPort,
		User: shardsetup.DefaultTestUser,
		Pass: shardsetup.TestPostgresPassword,
		DB:   "postgres",
	}

	// One long-lived schema-reset connection per target, reused across all
	// corpus files. The multigateway resetter connects directly to the
	// underlying primary PostgreSQL (not through the gateway) so every reset
	// pins to a single backend and the pg_namespace/pg_class invalidation
	// queue stays bounded — see suiteutil.SchemaResetter for the failure mode
	// this avoids.
	resetCtx, resetCancel := context.WithTimeout(t.Context(), 30*time.Second)
	pgResetter, err := suiteutil.NewSchemaResetter(resetCtx, pgTarget)
	resetCancel()
	if err != nil {
		t.Fatalf("open postgres schema-resetter: %v", err)
	}
	t.Cleanup(func() { pgResetter.Close(context.Background()) })

	primary := setup.GetPrimary(t)
	mgResetTarget := suiteutil.Target{
		Name: "multigateway-pg-direct",
		Host: "127.0.0.1",
		Port: primary.Pgctld.PgPort,
		User: pgconstants.DefaultPostgresUser,
		Pass: shardsetup.TestPostgresPassword,
		DB:   "postgres",
	}
	resetCtx, resetCancel = context.WithTimeout(t.Context(), 30*time.Second)
	mgResetter, err := suiteutil.NewSchemaResetter(resetCtx, mgResetTarget)
	resetCancel()
	if err != nil {
		t.Fatalf("open multigateway schema-resetter (direct to primary PG): %v", err)
	}
	t.Cleanup(func() { mgResetter.Close(context.Background()) })

	// Phase 6: for each file, run both targets. PG first (the baseline trace),
	// then the multigateway. Honors the overall test deadline so a timeout
	// still produces a coherent partial report.
	t.Logf("Phase 6: running %d files X 2 targets...", len(files))

	overall := t.Context()
	startedAt := time.Now()

	var (
		pgResults []*runResult
		mgResults []*runResult
		timedOut  bool
		ran       int
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

		ran++
		pgRes := runOne(overall, perFileTimeout, pgTarget, pgResetter, f)
		mgRes := runOne(overall, perFileTimeout, mgTarget, mgResetter, f)
		pgResults = append(pgResults, pgRes)
		mgResults = append(mgResults, mgRes)

		if (i+1)%25 == 0 || (i+1) == len(files) {
			t.Logf("  progress: %d/%d files (postgres baseline=%d, multigateway matched=%d)",
				i+1, len(files), ranCount(pgResults), matchedCount(pgResults, mgResults))
		}
	}

	t.Logf("Phase 6: ran %d of %d files (timed_out=%v)", ran, len(files), timedOut)

	// Phase 7: patch-aware comparison. A recorded known divergence lives in
	// testdata/patches/<name>.patch and is applied to PostgreSQL's trace to form
	// the expected multigateway trace; the file passes when the multigateway
	// matches that patched baseline. PGPROTO_PATCH_MODE=generate (re)writes the
	// patches from the current run. See patch.go.
	patchMode := getPatchMode()
	patchDir := filepath.Join(corpusRoot, "patches")
	t.Logf("Phase 7: patch verification (mode=%s, dir=%s)", patchMode, patchDir)
	outcomes := make([]patchOutcome, len(pgResults))
	for i := range pgResults {
		pg, mg := pgResults[i], mgResults[i]
		if !pg.Ran || !mg.Ran {
			continue // no baseline or no candidate trace — nothing to patch-compare
		}
		name := pg.File
		if rel, err := filepath.Rel(corpusRoot, pg.File); err == nil {
			name = rel
		}
		oc, err := verifyTracePatch(overall, pg.Trace, mg.Trace, patchDir, name, patchMode)
		if err != nil {
			t.Logf("  patch verify %s: %v", name, err)
			continue
		}
		outcomes[i] = oc
	}

	// Phase 8: build the suite report and persist it.
	report := newSuiteReport("PgProto", corpusRoot, pgResults, mgResults, outcomes, startedAt, timedOut)
	report.logSummary(t)

	if jsonPath, err := writeJSON(builder.OutputDir, report); err != nil {
		t.Logf("warning: failed to write results.json: %v", err)
	} else {
		t.Logf("results.json written to: %s", jsonPath)
	}
	if _, err := writeMarkdownSummary(t, builder.OutputDir, report); err != nil {
		t.Logf("warning: failed to write markdown summary: %v", err)
	}

	// No t.Fatal / t.Error based on divergence. Tracking is the job here;
	// regression gating happens in CI against the cached baseline.
}

// runOne invokes pgproto against a single target for one file, with its own
// bounded context.
func runOne(parent context.Context, perFileTimeout time.Duration, t suiteutil.Target, resetter *suiteutil.SchemaResetter, file string) *runResult {
	ctx, cancel := context.WithTimeout(parent, perFileTimeout)
	defer cancel()
	return runPgproto(ctx, t, resetter, file)
}

// ranCount counts how many PG runs produced a baseline trace.
func ranCount(pg []*runResult) int {
	n := 0
	for _, r := range pg {
		if r.Ran {
			n++
		}
	}
	return n
}

// matchedCount counts files where PG produced a baseline and the multigateway
// reproduced it exactly.
func matchedCount(pg, mg []*runResult) int {
	n := 0
	for i := range pg {
		if pg[i].Ran && mg[i].Ran && pg[i].Trace == mg[i].Trace {
			n++
		}
	}
	return n
}
