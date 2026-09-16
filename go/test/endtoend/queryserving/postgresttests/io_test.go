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
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// TestPostgRESTIO runs the proxy-relevant subset of PostgREST's upstream pytest
// `test/io` suite with PostgREST pointed at the multigateway
// (client → multigateway → multipooler → postgres).
//
// Unlike the hspec suite (TestPostgREST), the io suite spawns real postgrest
// binaries and drives them over HTTP, so it exercises paths the in-process
// hspec suite cannot — notably role-level settings applied per request via
// `set_config($1, $2, true)`, statement_timeout, hoisted transaction settings,
// and prepared statements. We run only the curated,
// non-timing subset (see io_selection.go / io_tests.md); the rest of test/io is
// PostgREST-internal (CLI, admin server, logging, config-reload machinery).
//
// Classification mirrors the hspec suite: the direct-PostgreSQL baseline is an
// asserted invariant (every selected test passes on plain postgres), so the
// default run is gateway-only and every gateway failure is a divergence.
// POSTGREST_FULL_BASELINE=1 re-verifies the invariant on a throwaway direct PG.
//
// Gated off by default (needs Docker + a PostgreSQL 17).
func TestPostgRESTIO(t *testing.T) {
	if !gateEnabled() {
		t.Skipf("skipping: set RUN_POSTGREST=1 (or %s=1) to run", suiteutil.EnvRunExtendedQueryServingTests)
	}
	requireDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	src, err := resolvePostgrestSource(t, ctx)
	if err != nil {
		t.Fatalf("resolve PostgREST source: %v", err)
	}
	if err := ensureIOImage(t, ctx, src); err != nil {
		t.Fatalf("ensure io image: %v", err)
	}

	// System PostgreSQL 17 on PATH (before cluster bootstrap so pgctld starts
	// it). The io fixtures need no contrib/PostGIS extensions, so unlike the
	// hspec arm this does not build PostGIS.
	prefix := ensurePostgresPlain(t)

	rep := &postgrestReport{}
	defer writeIOReport(t, rep)

	t.Logf("io suite: %d selected proxy-relevant tests, %d documented skip groups (see io_tests.md)",
		len(ioSelectedTests), len(ioSkipNotes))

	gw := runIOGateway(t, ctx, src)
	rep.Gateway = gw
	t.Logf("gateway: %d examples, %d passed, %d failed, %d pending",
		gw.Total, gw.Passed(), gw.Failures, gw.Pending)

	if os.Getenv("POSTGREST_FULL_BASELINE") != "1" {
		rep.Divergences = gw.Failing
		for _, f := range gw.Failing {
			t.Logf("  DIVERGE: %s", f)
		}
		if len(gw.Failing) > 0 {
			t.Errorf("%d gateway divergence(s) — io test(s) fail through the gateway but pass on direct PostgreSQL", len(gw.Failing))
		}
		return
	}

	// POSTGREST_FULL_BASELINE=1: re-verify the invariant on a throwaway direct
	// PostgreSQL and classify. Use after bumping the PostgREST tag or editing
	// the selection/fixtures.
	base := runIODirectBaseline(t, ctx, src, prefix)
	rep.Baseline = base
	t.Logf("direct baseline: %d examples, %d passed, %d failed, %d pending",
		base.Total, base.Passed(), base.Failures, base.Pending)

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

	for _, f := range envFailures {
		t.Logf("  ENV:  %s", f)
	}
	for _, f := range divergences {
		t.Logf("  DIVERGE: %s", f)
	}
	if len(divergences) > 0 {
		t.Errorf("%d gateway divergence(s) — io test(s) fail through the gateway but pass on direct PostgreSQL", len(divergences))
	}
}

// runIOGateway brings up a 2-pooler + multigateway cluster, loads PostgREST's
// io fixtures on the primary (socket-only, bypassing gateway DDL handling), and
// runs the selected io tests against the gateway's TCP port.
func runIOGateway(t *testing.T, ctx context.Context, src string) *specResult {
	t.Helper()

	setup := shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby
		shardsetup.WithMultigateway(),
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
	if err := loadIOFixtures(t, ctx, conn, src); err != nil {
		t.Fatalf("load io fixtures on primary: %v", err)
	}

	res, err := runIO(t, ctx, specTarget{Name: "gateway", Port: setup.MultigatewayPgPort})
	if err != nil {
		t.Fatalf("run io suite through gateway: %v", err)
	}
	return res
}

// runIODirectBaseline starts a standalone PostgreSQL off the same system
// binaries, loads the io fixtures, and runs the selected io tests against it —
// the classifier for gateway failures.
func runIODirectBaseline(t *testing.T, ctx context.Context, src, prefix string) *specResult {
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
	if err := loadIOFixtures(t, ctx, conn, src); err != nil {
		t.Fatalf("load io fixtures on standalone: %v", err)
	}

	res, err := runIO(t, ctx, specTarget{Name: "direct", Port: sa.Port})
	if err != nil {
		t.Fatalf("run io suite on direct baseline: %v", err)
	}
	return res
}

// ensurePostgresPlain locates a system PostgreSQL 17 and prepends its bin dir to
// PATH (so pgctld and host-side psql use it), returning the install prefix. It
// is the io-suite counterpart to ensurePostgres but skips the PostGIS/contrib
// provisioning, which the io fixtures do not need.
func ensurePostgresPlain(t *testing.T) string {
	t.Helper()

	binDir := resolvePgBinDir(t)
	prefix := filepath.Dir(binDir)

	if ver, err := runPgConfigVersion(binDir); err != nil {
		t.Skipf("pg_config not usable at %s: %v", binDir, err)
	} else if !strings.Contains(ver, "PostgreSQL 17") {
		t.Skipf("need PostgreSQL 17, found %q at %s", strings.TrimSpace(ver), binDir)
	}

	if err := os.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH")); err != nil {
		t.Fatalf("set PATH: %v", err)
	}
	t.Logf("Using system PostgreSQL 17 at %s", prefix)
	return prefix
}

// runPgConfigVersion returns `pg_config --version` output for the PG at binDir.
func runPgConfigVersion(binDir string) (string, error) {
	out, err := exec.Command(filepath.Join(binDir, "pg_config"), "--version").Output()
	return string(out), err
}
