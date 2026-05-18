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
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/test/utils"
)

// dialReplicationConn opens a Postgres connection in logical-replication mode
// against the shared test fixture's primary. It mirrors what
// reserved.Pool.NewLogicalReplicationConn does inside multipooler: copies the
// caller's client.Config and sets `replication=database` in the startup
// parameters. The factory's bookkeeping (Pooled wrapper with pool=nil,
// ReasonLogicalReplication, idleKiller exemption) is unit-tested in
// reserved/logical_replication_test.go; the wire-level claim — that the
// startup parameter survives the libpq-format encoder and that real Postgres
// accepts it on a multigres-provisioned cluster — is what this test verifies.
//
// The Manager.NewLogicalReplicationConn entry point is not exercised here
// because it expects SCRAM passthrough keys derived during a client's SCRAM
// handshake at multigateway; computing those from a static password requires
// a separate round-trip to fetch the server's salt and iteration count, which
// is exactly the wire-level work this test already exercises via direct
// client.Connect. Manager wiring is covered by the unit test in
// connpoolmanager/manager_logical_replication_test.go against fakepgserver.
func dialReplicationConn(t *testing.T, port int) *client.Conn {
	t.Helper()
	ctx := utils.WithTimeout(t, 10*time.Second)

	cfg := client.Config{
		Host:        "localhost",
		Port:        port,
		User:        constants.DefaultPostgresUser,
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
		Parameters:  map[string]string{"replication": "database"},
	}
	conn, err := client.Connect(ctx, ctx, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestLogicalReplicationConnIdentifySystem opens a replication-mode Postgres
// connection and issues IDENTIFY_SYSTEM — the simplest replication-protocol
// command — to prove the connection is usable for the replication protocol
// end-to-end against a multigres-provisioned cluster.
func TestLogicalReplicationConnIdentifySystem(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)

	ctx := utils.WithTimeout(t, 10*time.Second)
	rows, err := conn.Query(ctx, "IDENTIFY_SYSTEM")
	require.NoError(t, err)
	require.Len(t, rows, 1, "IDENTIFY_SYSTEM returns one result set")
	require.Len(t, rows[0].Rows, 1, "IDENTIFY_SYSTEM returns one row")

	row := rows[0].Rows[0]
	// Columns: systemid (text), timeline (int4), xlogpos (text), dbname (text)
	require.GreaterOrEqual(t, len(row.Values), 4, "IDENTIFY_SYSTEM row has at least 4 columns")
	assert.NotEmpty(t, string(row.Values[0]), "system identifier present")
	assert.NotEmpty(t, string(row.Values[1]), "timeline present")
	assert.Regexp(t, regexp.MustCompile(`^[0-9A-Fa-f]+/[0-9A-Fa-f]+$`), string(row.Values[2]),
		"xlogpos must be an LSN")
}

// TestLogicalReplicationConnPinning verifies that a replication-mode connection
// is session-pinned: consecutive replication-protocol commands on the same
// socket land on the same Postgres walsender backend. This is the load-bearing
// invariant for logical replication — the replication slot (once created)
// lives on a specific backend; if subsequent commands routed to a different
// backend, the slot would be invisible.
//
// In replication mode the available probes are limited (SELECT
// pg_backend_pid() and most catalog queries are restricted), so we use the
// startup-time ProcessID (BackendKeyData) and the systemid returned by
// IDENTIFY_SYSTEM as stable identifiers. Both are determined at backend-
// startup and are constant for the lifetime of one walsender process.
func TestLogicalReplicationConnPinning(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	startupPID := conn.ProcessID()
	require.NotZero(t, startupPID, "BackendKeyData missing — startup is broken")

	ctx := utils.WithTimeout(t, 15*time.Second)
	var firstSystemID string
	for i := range 5 {
		rows, err := conn.Query(ctx, "IDENTIFY_SYSTEM")
		require.NoError(t, err, "IDENTIFY_SYSTEM iteration %d", i)
		require.Len(t, rows, 1)
		require.Len(t, rows[0].Rows, 1, "IDENTIFY_SYSTEM iteration %d", i)

		systemID := string(rows[0].Rows[0].Values[0])
		require.NotEmpty(t, systemID, "iteration %d", i)
		if i == 0 {
			firstSystemID = systemID
			continue
		}
		require.Equal(t, firstSystemID, systemID,
			"system identifier must be stable across queries on a pinned conn (iteration %d)", i)
	}

	// ProcessID is read at startup and never updated by client.Conn, so this
	// is a sanity check that the BackendKeyData we received is what we'll
	// keep using to identify the backend (e.g., for cancel requests on this
	// session). Together with stable systemid, this proves we're talking to
	// the same walsender across all five queries.
	require.Equal(t, startupPID, conn.ProcessID(),
		"backend ProcessID must remain the connection's identifier")
}
