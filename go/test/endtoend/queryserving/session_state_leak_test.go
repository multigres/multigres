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

package queryserving

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestHiddenFunctionStateDoesNotLeak is the acceptance test for the
// statement-rejection gate: a set_config hidden inside a SQL function body is
// invisible to the gateway's session-state tracking, so under the
// gateway-authoritative model the only sound handling is to reject creating
// such a function (creation-time body analysis). Until that gate lands, this
// reproduces a real cross-client session-state leak on both the regular and
// reserved pools and is skipped.
//
// Remove the skip when the creation-time rejection gate ships: the CREATE
// FUNCTION below must then fail (or, if certified body analysis lands
// instead, the leak assertions must hold).
func TestHiddenFunctionStateDoesNotLeak(t *testing.T) {
	t.Skip("known limitation: hidden set_config in routine bodies is untracked; " +
		"blocked on the creation-time statement-rejection gate")

	if testing.Short() {
		t.Skip("skipping session state leak test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 2*time.Minute)

	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	primaryDSN := shardsetup.GetTestUserDSN("localhost", setup.GetPrimary(t).Pgctld.PgPort, "sslmode=disable", "connect_timeout=5")
	primary, err := sql.Open("postgres", primaryDSN)
	require.NoError(t, err)
	defer primary.Close()

	// Install directly so the mutation is absent from the client's top-level SQL.
	_, err = primary.ExecContext(ctx, `CREATE OR REPLACE FUNCTION hidden_set()
		RETURNS text LANGUAGE sql AS $$SELECT set_config('work_mem', '123MB', false)$$`)
	require.NoError(t, err)
	defer primary.ExecContext(ctx, "DROP FUNCTION IF EXISTS hidden_set()") //nolint:errcheck

	connA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	connA.SetMaxIdleConns(0)
	_, err = connA.ExecContext(ctx, "SELECT hidden_set()")
	require.NoError(t, err)
	require.NoError(t, connA.Close())

	connB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer connB.Close()
	var workMem string
	require.NoError(t, connB.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem))
	require.NotEqual(t, "123MB", workMem, "hidden regular-pool state leaked across logical clients")

	// The same assertion for a reserved wrapper's underlying regular connection
	// after final reservation release.
	reservedA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	reservedA.SetMaxIdleConns(0)
	_, err = reservedA.ExecContext(ctx, "CREATE TEMP TABLE leak_pin(i integer)")
	require.NoError(t, err)
	_, err = reservedA.ExecContext(ctx, "SELECT hidden_set()")
	require.NoError(t, err)
	require.NoError(t, reservedA.Close())

	reservedB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer reservedB.Close()
	require.NoError(t, reservedB.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem))
	require.NotEqual(t, "123MB", workMem, "hidden reserved-pool state leaked across logical clients")
}

// TestSessionScrubberReplacesHiddenState verifies the pool's session-state
// scrubber: a set_config hidden inside a SQL function body escapes the
// gateway's session tracking and leaves real session GUC state on a pooled
// backend whose settings label doesn't know it. The scrubber (default 10s
// interval) probes idle connections, detects the divergence, and replaces the
// contaminated backend, so no later client can observe the leaked value.
//
// This is the detection net behind the creation-time rejection gates; see
// TestHiddenFunctionStateDoesNotLeak above for the gate itself.
func TestSessionScrubberReplacesHiddenState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping session scrubber test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 3*time.Minute)

	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	primaryDSN := shardsetup.GetTestUserDSN("localhost", setup.GetPrimary(t).Pgctld.PgPort, "sslmode=disable", "connect_timeout=5")
	primary, err := sql.Open("postgres", primaryDSN)
	require.NoError(t, err)
	defer primary.Close()

	// Canary: before contaminating anything, normal traffic must produce
	// ZERO divergence. The untracked rule assumes connection bootstrap
	// leaves no session-source GUC state outside the settings label (see
	// the invariant note in regular.Pool.Open); a future bootstrap-time SET
	// that bypasses the label would make the scrubber replace every backend
	// each sweep and trip this assertion. Warm the pool, wait for at least
	// one scrub sweep (default interval 10s), then assert a quiet log. In
	// full-suite runs this also covers all scrub sweeps during the earlier
	// tests' traffic.
	warmup, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	var one int
	require.NoError(t, warmup.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	require.NoError(t, warmup.Close())
	time.Sleep(15 * time.Second)
	quietLog, err := os.ReadFile(setup.PrimaryMultipooler(t).LogFile)
	require.NoError(t, err)
	require.NotContains(t, string(quietLog), "session-state divergence detected",
		"normal traffic must not diverge: connection bootstrap created session state outside the settings label")

	// Install directly so the mutation is absent from the client's top-level SQL.
	_, err = primary.ExecContext(ctx, `CREATE OR REPLACE FUNCTION hidden_scrub_set()
		RETURNS text LANGUAGE sql AS $$SELECT set_config('work_mem', '123MB', false)$$`)
	require.NoError(t, err)
	defer primary.ExecContext(ctx, "DROP FUNCTION IF EXISTS hidden_scrub_set()") //nolint:errcheck

	// Contaminate one pooled backend and capture its PID in the same
	// statement so both values come from the same physical connection.
	connA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	connA.SetMaxIdleConns(0)
	var ignored string
	var leakedPID int
	err = connA.QueryRowContext(ctx, "SELECT hidden_scrub_set(), pg_backend_pid()").Scan(&ignored, &leakedPID)
	require.NoError(t, err)
	require.NoError(t, connA.Close())

	// The scrubber must find the diverged backend while it sits idle in the
	// pool and terminate it.
	require.Eventually(t, func() bool {
		var alive int
		if err := primary.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", leakedPID).Scan(&alive); err != nil {
			return false
		}
		return alive == 0
	}, 90*time.Second, time.Second, "scrubber should have replaced the contaminated backend (pid %d)", leakedPID)

	// The replacement must have been the scrubber's doing, not routine pool
	// churn: the multipooler logged the divergence with the leaked GUC name.
	poolerLog, err := os.ReadFile(setup.PrimaryMultipooler(t).LogFile)
	require.NoError(t, err)
	require.Contains(t, string(poolerLog), "session-state divergence detected",
		"multipooler log should record the scrubber replacing the backend")
	require.Contains(t, string(poolerLog), "work_mem",
		"divergence log should name the leaked GUC")
	require.NotContains(t, strings.ToLower(string(poolerLog)), "123mb",
		"divergence log must never carry GUC values")

	// And no later client can observe the leaked value.
	connB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer connB.Close()
	var workMem string
	require.NoError(t, connB.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem))
	require.NotEqual(t, "123MB", workMem, "hidden session state leaked past the scrubber")
}
