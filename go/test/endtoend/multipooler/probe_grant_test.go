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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

	canSelect := func(db *sql.DB) bool {
		var ok bool
		err := db.QueryRow(`SELECT has_table_privilege('multigres_probe', 'multigres.heartbeat', 'SELECT')`).Scan(&ok)
		return err == nil && ok
	}

	// A shard bootstrapped before the schema granted USAGE to PUBLIC.
	_, err := primaryDB.Exec(`CREATE ROLE multigres_probe; REVOKE USAGE ON SCHEMA multigres FROM PUBLIC`)
	require.NoError(t, err)
	require.False(t, canSelect(primaryDB), "a new role must not read the heartbeat table yet")

	restartMultipooler(t, primary)
	require.Eventually(t, func() bool { return canSelect(primaryDB) },
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

	require.Eventually(t, func() bool { return canSelect(standbyDB) },
		30*time.Second, 200*time.Millisecond, "the grant should replicate to the standby")
}
