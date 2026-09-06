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

package postgresttests

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// TestPostgREST runs PostgREST's upstream spec suite with PostgREST pointed at the
// multigateway (client → multigateway → multipooler → postgres) — the behaviour
// we actually want to validate.
//
// The suite is upstream's own, so it passes against a correctly-set-up
// PostgreSQL; the interesting signal is which specs behave differently through
// the gateway. To keep that signal trustworthy without paying for a full second
// run every time, the direct-PostgreSQL "baseline" is used only as a CLASSIFIER:
// it runs solely when the gateway has failures (or when POSTGREST_FULL_BASELINE=1
// forces it), and only to decide, per failing spec, whether the failure is a
//   - gateway divergence: fails through the gateway, passes on direct PostgreSQL
//     (attributable to multigres), or
//   - environment failure: fails on both (our fixtures / PG version / config,
//     not the gateway).
//
// Gated off by default (builds/needs a PG + PostGIS and a Haskell test image).
func TestPostgREST(t *testing.T) {
	if !gateEnabled() {
		t.Skipf("skipping: set RUN_POSTGREST=1 (or %s=1) to run", suiteutil.EnvRunExtendedQueryServingTests)
	}
	requireDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()

	src, err := resolvePostgrestSource(t, ctx)
	if err != nil {
		t.Fatalf("resolve PostgREST source: %v", err)
	}
	if err := ensureSpecImage(t, ctx, src); err != nil {
		t.Fatalf("ensure spec image: %v", err)
	}

	// System PostgreSQL 17 (with PostGIS + fixture extensions) on PATH, before
	// cluster bootstrap so pgctld starts it.
	prefix := ensurePostgres(t, ctx)
	match := specMatch()

	// Write the run summary (badge + divergence count) to GITHUB_STEP_SUMMARY on
	// every exit path, so CI surfaces the count even when the test fails on an
	// open divergence. Populated as the arms run below.
	rep := &postgrestReport{}
	defer writeReport(t, rep)

	// Gateway run — the path we validate. The direct-PostgreSQL baseline is an
	// asserted invariant: every non-skipped spec passes on plain postgres, so any
	// gateway failure is by definition a divergence and the default run is
	// gateway-only — no second postgres run. The suite currently runs green; a new
	// failure here means a real behavioural gap on the proxied path.
	gw := runGateway(t, ctx, src, match)
	rep.Gateway = gw
	t.Logf("gateway: %d examples, %d passed, %d failed, %d pending",
		gw.Total, gw.Passed(), gw.Failures, gw.Pending)

	if os.Getenv("POSTGREST_FULL_BASELINE") != "1" {
		// Default: baseline assumed green, so every gateway failure is a divergence.
		rep.Divergences = gw.Failing
		for _, f := range gw.Failing {
			t.Logf("  DIVERGE: %s", f)
		}
		if len(gw.Failing) > 0 {
			t.Errorf("%d gateway divergence(s) — spec(s) fail through the gateway but pass on direct PostgreSQL", len(gw.Failing))
		}
		return
	}

	// POSTGREST_FULL_BASELINE=1 re-verifies the invariant: run the same match on
	// a throwaway direct PostgreSQL and classify. A gateway failure that also
	// fails on direct postgres is an environment failure — the invariant is
	// broken (fix the harness/fixtures), not a gateway bug. Use this after
	// bumping the PostgREST tag or editing fixtures.
	base := runDirectBaseline(t, ctx, src, prefix, match)
	rep.Baseline = base
	t.Logf("direct baseline: %d examples, %d passed, %d failed, %d pending",
		base.Total, base.Passed(), base.Failures, base.Pending)

	// The invariant says base.Failing should be empty; if it is not, surface each
	// one — those are the specs that broke the "postgres passes everything" rule.
	for _, f := range base.Failing {
		t.Logf("  BASELINE FAIL (invariant broken — harness/env, not gateway): %s", f)
	}

	baseFailed := toSet(base.Failing)
	var divergences, envFailures []string
	for _, f := range gw.Failing {
		if baseFailed[f] {
			envFailures = append(envFailures, f)
		} else {
			divergences = append(divergences, f)
		}
	}
	rep.Divergences = divergences
	rep.EnvFailures = envFailures

	if len(envFailures) > 0 {
		t.Logf("%d environment failure(s) (fail on direct PostgreSQL too — NOT gateway bugs):", len(envFailures))
		for _, f := range envFailures {
			t.Logf("  ENV:  %s", f)
		}
	}
	for _, f := range divergences {
		t.Logf("  DIVERGE: %s", f)
	}

	if len(divergences) > 0 {
		t.Errorf("%d gateway divergence(s) — spec(s) fail through the gateway but pass on direct PostgreSQL", len(divergences))
	}
}

// runGateway brings up a 2-pooler + multigateway cluster, loads PostgREST's
// fixtures on the primary (socket-only, so loaded directly, bypassing gateway
// DDL handling), and runs the spec suite against the gateway's TCP port.
func runGateway(t *testing.T, ctx context.Context, src, match string) *specResult {
	t.Helper()

	setup := shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby
		shardsetup.WithMultigateway(),
		// Keep pooled capacity under the generated max_connections=60 ceiling.
		shardsetup.WithMultipoolerExtraArgs("--connpool-global-capacity=50"),
	)
	setup.SetupTest(t)

	primary := setup.GetPrimary(t)
	conn := pgConn{
		Host:      filepath.Join(primary.Pgctld.PoolerDir, "pg_sockets"),
		Port:      primary.Pgctld.PgPort,
		SuperUser: shardsetup.DefaultTestUser,
		SuperPass: shardsetup.TestPostgresPassword,
		Database:  "postgres",
	}
	if err := loadFixtures(t, ctx, conn, src); err != nil {
		t.Fatalf("load fixtures on primary: %v", err)
	}

	res, err := runSpec(t, ctx, specTarget{Name: "gateway", Port: setup.MultigatewayPgPort}, match)
	if err != nil {
		t.Fatalf("run spec suite through gateway: %v", err)
	}
	return res
}

// runDirectBaseline starts a standalone PostgreSQL off the same system binaries,
// loads the fixtures, and runs the spec suite against it — the classifier for
// gateway failures. It is only called when the gateway has failures (or a full
// baseline is forced).
func runDirectBaseline(t *testing.T, ctx context.Context, src, prefix, match string) *specResult {
	t.Helper()

	b := &pgbuilder.Builder{InstallDir: prefix, OutputDir: t.TempDir()}
	sa, err := pgbuilder.StartStandalone(t, ctx, b, "test_password_123")
	if err != nil {
		t.Fatalf("start standalone postgres: %v", err)
	}
	defer func() { _ = sa.Stop() }()

	conn := pgConn{
		Host:      "127.0.0.1",
		Port:      sa.Port,
		SuperUser: sa.User,
		SuperPass: sa.Password,
		Database:  sa.Database,
		SSLMode:   "disable",
	}
	if err := loadFixtures(t, ctx, conn, src); err != nil {
		t.Fatalf("load fixtures on standalone: %v", err)
	}

	res, err := runSpec(t, ctx, specTarget{Name: "direct", Port: sa.Port}, match)
	if err != nil {
		t.Fatalf("run spec suite on direct baseline: %v", err)
	}
	return res
}

// gateEnabled reports whether the PostgREST suite should run. Gated off by
// default (heavy: needs a PostgreSQL + PostGIS and a Haskell test image).
func gateEnabled() bool {
	return os.Getenv("RUN_POSTGREST") == "1" ||
		os.Getenv(suiteutil.EnvRunExtendedQueryServingTests) == "1"
}

// requireDocker skips the test unless a working docker daemon is reachable.
func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found on PATH")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not reachable")
	}
}

// specMatch returns the hspec --match filter. Empty (the default) runs the whole
// PostgREST spec suite; set POSTGREST_MATCH to scope a run (e.g. to a single
// Feature.Query.QuerySpec while iterating).
func specMatch() string {
	return os.Getenv("POSTGREST_MATCH")
}

func toSet(items []string) map[string]bool {
	s := make(map[string]bool, len(items))
	for _, it := range items {
		s[it] = true
	}
	return s
}
