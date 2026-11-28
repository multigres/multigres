// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multipooler

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// A probe role created after bootstrap gets read-only access to the heartbeat
// table the next time the primary pooler opens, without relying on PUBLIC.
//
// Uses an isolated shard because it restarts the primary pooler.
func TestProbeRole_HeartbeatGrantOnOpen(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()

	primary := setup.GetPrimary(t)
	standbys := setup.GetStandbys()
	require.Len(t, standbys, 1)
	standby := standbys[0]

	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer primaryDB.Close()
	standbyDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(standby.Pgctld.PoolerDir), standby.Pgctld.PgPort)
	defer standbyDB.Close()

	requireNoGrantWarning(t, primary, "bootstrap without the probe role should not fail the grant")

	// A shard bootstrapped before the schema granted USAGE to PUBLIC.
	_, err := primaryDB.Exec(`CREATE ROLE multigres_probe; REVOKE USAGE ON SCHEMA multigres FROM PUBLIC`)
	require.NoError(t, err)
	require.False(t, probeCanSelect(primaryDB), "a new role must not read the heartbeat table yet")

	restartMultipooler(t, primary)
	require.Eventually(t, func() bool { return probeCanSelect(primaryDB) },
		30*time.Second, 200*time.Millisecond, "open should grant the probe role SELECT on the heartbeat table")

	var canWrite bool
	require.NoError(t, primaryDB.QueryRow(
		`SELECT has_table_privilege('multigres_probe', 'multigres.heartbeat', 'INSERT, UPDATE, DELETE, TRUNCATE')`).Scan(&canWrite))
	require.False(t, canWrite, "the probe role must only read the heartbeat table")

	tx, err := primaryDB.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`SET LOCAL ROLE multigres_probe`)
	require.NoError(t, err)
	var ts int64
	err = tx.QueryRow(`SELECT ts FROM multigres.heartbeat LIMIT 1`).Scan(&ts)
	require.NoError(t, tx.Rollback())
	require.NoError(t, err, "the probe query must work as the probe role")

	require.Eventually(t, func() bool { return probeCanSelect(standbyDB) },
		30*time.Second, 200*time.Millisecond, "the grant should replicate to the standby")

	before := probeACLRowVersions(t, primaryDB)
	restartMultipooler(t, primary)
	require.Equal(t, before, probeACLRowVersions(t, primaryDB), "a repeat open should not rewrite the ACLs")
	requireNoGrantWarning(t, primary, "the grant should not fail once the role exists")
}

// probeACLRowVersions returns the xmin of the catalog rows that hold the
// heartbeat table's and the schema's ACLs. Any GRANT rewrites them.
func probeACLRowVersions(t *testing.T, db *sql.DB) string {
	t.Helper()
	var v string
	require.NoError(t, db.QueryRow(`SELECT
		(SELECT xmin::text FROM pg_class WHERE oid = 'multigres.heartbeat'::regclass) || '/' ||
		(SELECT xmin::text FROM pg_namespace WHERE nspname = 'multigres')`).Scan(&v))
	return v
}

// requireNoGrantWarning fails if the pooler logged a failed probe grant. A
// failure only logs, so the log is the one place a broken grant shows.
func requireNoGrantWarning(t *testing.T, inst *shardsetup.MultipoolerInstance, msg string) {
	t.Helper()
	data, err := os.ReadFile(inst.Multipooler.LogFile)
	require.NoError(t, err)
	require.NotContains(t, string(data), "failed to grant heartbeat read access", msg)
}

// probeCanSelect reports whether the probe role can read the heartbeat table.
func probeCanSelect(db *sql.DB) bool {
	var ok bool
	err := db.QueryRow(`SELECT has_table_privilege('multigres_probe', 'multigres.heartbeat', 'SELECT')`).Scan(&ok)
	return err == nil && ok
}

// probeHasDirectUsage reports whether the schema ACL names the probe role. The
// grant gives USAGE and SELECT in one transaction, so this also shows the
// table step has run.
func probeHasDirectUsage(db *sql.DB) bool {
	var ok bool
	err := db.QueryRow(`SELECT EXISTS (
		SELECT FROM pg_namespace n, aclexplode(n.nspacl) a
		WHERE n.nspname = 'multigres'
			AND a.grantee = 'multigres_probe'::regrole
			AND a.privilege_type = 'USAGE')`).Scan(&ok)
	return err == nil && ok
}

// SELECT granted to PUBLIC must not stop the probe role getting its own grant,
// or revoking the PUBLIC grant later would lock the probe out.
func TestProbeRole_HeartbeatGrantIgnoresPublicSelect(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()

	primary := setup.GetPrimary(t)
	db := connectToPostgresViaSocket(t,
		getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer db.Close()

	_, err := db.Exec(`CREATE ROLE multigres_probe;
		REVOKE USAGE ON SCHEMA multigres FROM PUBLIC;
		GRANT SELECT ON TABLE multigres.heartbeat TO PUBLIC`)
	require.NoError(t, err)

	restartMultipooler(t, primary)
	require.Eventually(t, func() bool { return probeHasDirectUsage(db) },
		30*time.Second, 200*time.Millisecond, "open should grant the probe role")

	_, err = db.Exec(`REVOKE SELECT ON TABLE multigres.heartbeat FROM PUBLIC`)
	require.NoError(t, err)
	require.True(t, probeCanSelect(db), "the probe role should keep SELECT after PUBLIC loses it")
}

// A standby promoted by a failover grants the probe role access without a
// restart. Its open ran while it was in recovery and skipped the grant.
func TestProbeRole_HeartbeatGrantOnPromotion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	// Three poolers so a quorum survives losing the primary.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()
	setup.StartMultiorchs(t.Context(), t)

	primary := setup.GetPrimary(t)
	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	_, err := primaryDB.Exec(`CREATE ROLE multigres_probe; REVOKE USAGE ON SCHEMA multigres FROM PUBLIC`)
	require.NoError(t, err)
	require.False(t, probeCanSelect(primaryDB), "a role created after open must not read the heartbeat table yet")
	primaryDB.Close()

	client, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()
	_, err = client.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	setup.KillPostgres(t, primary.Name)

	newPrimary := setup.GetMultipoolerInstance(shardsetup.WaitForNewPrimary(t, setup, primary.Name, 30*time.Second))
	newPrimaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(newPrimary.Pgctld.PoolerDir), newPrimary.Pgctld.PgPort)
	defer newPrimaryDB.Close()
	require.Eventually(t, func() bool { return probeCanSelect(newPrimaryDB) },
		30*time.Second, 200*time.Millisecond, "promotion should grant the probe role SELECT on the heartbeat table")
}
