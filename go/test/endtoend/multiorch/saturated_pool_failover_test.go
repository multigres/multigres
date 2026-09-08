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

package multiorch

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestFailoverWithSaturatedUserPools is a regression test for control-plane
// recovery probes sharing capacity with user traffic.
//
// The failover-critical probes (pg_is_in_recovery() and friends) used to run
// on the per-user regular pool. Under user-traffic saturation the probe's
// borrow queued behind user queries and timed out at its 500ms budget, the
// postgres monitor could not determine the node's role, and failover stalled
// exactly when it was most needed. The probes now run on the separate admin
// pool, which user traffic can never exhaust.
//
// The test saturates every pooler's regular pool with long-running user
// statements (the e2e test user is "postgres" — the same user the internal
// queries run as, so client sessions and internal borrows contend for the
// same per-user pool), verifies saturation server-side via pg_stat_activity
// on each node's postgres, then kills the primary and requires a new primary
// to be elected while the pools stay saturated.
func TestFailoverWithSaturatedUserPools(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestFailoverWithSaturatedUserPools in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	// Tiny global capacity so a handful of held sessions saturates the regular
	// pool. With the default reserved-ratio of 0.2, regular capacity is
	// int64(4 * 0.8) = 3 connections per pooler.
	const regularCapacity = 3

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(3),
		shardsetup.WithMultigateway(),
		shardsetup.WithMultigatewayReplicaPort(),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		// Short grace period: this test hard-kills the primary, so the grace
		// window only delays the failover the test is waiting for.
		shardsetup.WithLeaderFailoverGracePeriod("2s", "2s"),
		shardsetup.WithMultipoolerExtraArgs("--connpool-global-capacity=4"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	oldPrimaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", oldPrimaryName)

	// Wait for the replica-reads port to serve before saturating through it.
	roDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayReplicaPgPort, "sslmode=disable", "connect_timeout=5")
	roDB, err := sql.Open("postgres", roDSN)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var one int
		return roDB.QueryRow("SELECT 1").Scan(&one) == nil
	}, 60*time.Second, time.Second, "replica-reads port did not become ready")
	roDB.Close()

	// Saturate the regular pools: each session runs a long pg_sleep as an
	// autocommit statement, pinning one regular-pool connection for the test's
	// lifetime. (Transactions would pin *reserved*-pool connections instead —
	// the probes borrow from the regular pool, so plain statements it is.)
	// Sessions beyond pool capacity queue on the borrow or error; both are
	// fine, the first regularCapacity holders per pooler are what matter. The
	// replica port routes randomly across standbys, so open enough sessions
	// that every standby fills with overwhelming probability.
	holdCtx, cancelHolds := context.WithCancel(context.Background())
	defer cancelHolds()
	var holdWG sync.WaitGroup
	t.Cleanup(holdWG.Wait)
	t.Cleanup(cancelHolds)

	holdSessions := func(port, n int) {
		dsn := shardsetup.GetTestUserDSN("localhost", port, "sslmode=disable", "connect_timeout=5")
		for range n {
			db, err := sql.Open("postgres", dsn)
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			db.SetMaxOpenConns(1)
			holdWG.Go(func() {
				// Errors are expected: sessions beyond capacity may time out,
				// and primary-side holders die when the primary is killed.
				_, _ = db.ExecContext(holdCtx, "SELECT pg_sleep(600)")
			})
		}
	}
	holdSessions(setup.MultigatewayPgPort, 12)
	holdSessions(setup.MultigatewayReplicaPgPort, 40)

	// Server-side saturation check: count active pg_sleep backends directly on
	// each node's postgres (bypassing the pooler). Only when every node holds
	// at least regularCapacity of them is the regular pool provably full —
	// client-side blocking can't distinguish "all poolers full" from "I keep
	// getting routed to a full one".
	directDBs := make(map[string]*sql.DB, len(setup.Multipoolers))
	for name, inst := range setup.Multipoolers {
		db, err := sql.Open("postgres", shardsetup.GetPostgresDSN("localhost", inst.Pgctld.PgPort, "sslmode=disable", "connect_timeout=5"))
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		directDBs[name] = db
	}
	countHeld := func(db *sql.DB) (int, error) {
		var n int
		err := db.QueryRow(
			`SELECT count(*) FROM pg_stat_activity
			 WHERE state = 'active' AND query LIKE '%pg_sleep%' AND pid <> pg_backend_pid()`).Scan(&n)
		return n, err
	}
	require.Eventually(t, func() bool {
		for name, db := range directDBs {
			n, err := countHeld(db)
			if err != nil {
				t.Logf("saturation check on %s: %v", name, err)
				return false
			}
			if n < regularCapacity {
				t.Logf("saturation check on %s: %d/%d regular-pool connections held", name, n, regularCapacity)
				return false
			}
		}
		return true
	}, 90*time.Second, 2*time.Second, "regular pools did not saturate on every node")
	t.Log("All poolers' regular pools are saturated with held user statements")

	// Disable postgres auto-restart on the primary so the monitor does not
	// resurrect it mid-failover (same reasoning as TestDeadPrimaryRecovery).
	primaryClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = primaryClient.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	primaryClient.Close()

	t.Logf("Killing postgres on primary %s with pools saturated", oldPrimaryName)
	setup.KillPostgres(t, oldPrimaryName)

	// The regression assertion: a standby must still be promoted even though
	// every regular pool is saturated. Before the probes moved to the admin
	// pool, the standbys' monitors could not read pg_is_in_recovery() here and
	// no promotion happened.
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 60*time.Second)
	require.NotEmpty(t, newPrimaryName, "expected failover to complete despite saturated user pools")
	t.Logf("New primary elected under pool saturation: %s", newPrimaryName)

	// The regression signature: no control-plane query may ever starve on the
	// saturated regular pool. All manager, consensus, and heartbeat queries run
	// on the admin pool, so a "connection pool timed out" in a multipooler log
	// means some control-plane query regressed onto the regular pool (borrow
	// failures on the user query path surface to the client, not this log).
	// Before the move, this window produced ~70 such lines per run
	// (pg_is_in_recovery probe timeouts, replication-status reads,
	// consensus.recruit's primary_conninfo reset). Wall-clock failover time
	// cannot discriminate here — retry loops eventually limp through once held
	// sessions die with the postgres restarts around promotion — so the borrow
	// failures in the logs are the observable.
	for name, inst := range setup.Multipoolers {
		logBytes, err := os.ReadFile(inst.Multipooler.LogFile)
		require.NoError(t, err, "reading multipooler log for %s", name)
		for line := range strings.SplitSeq(string(logBytes), "\n") {
			require.NotContains(t, line, "connection pool timed out",
				"%s: a control-plane query starved on the saturated regular pool", name)
		}
	}
}
