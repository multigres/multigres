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

package queryserving

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_LogicalReplicationStream drives a logical replication
// client through the gateway on either topology: the gateway tunnels the
// replication=database connection to its pooler (StreamReplication). In
// Minigres that call still goes over the in-memory gRPC server while queries
// are direct calls, so this checks replication keeps working there. The
// Multigres-only suite in shardsetup covers the tunnel in depth.
func TestMultigateway_LogicalReplicationStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication stream test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping replication stream test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	db, err := sql.Open("postgres", shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	const table, pub, slot = "repl_stream_e2e", "repl_stream_e2e_pub", "repl_stream_e2e_slot"
	_, err = db.ExecContext(ctx, fmt.Sprintf("ALTER ROLE %s REPLICATION", shardsetup.DefaultTestUser))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP PUBLICATION IF EXISTS " + pub) })

	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.ClientPort(),
		User:        shardsetup.DefaultTestUser,
		Password:    shardsetup.TestPostgresPassword,
		Database:    "postgres",
		DialTimeout: 10 * time.Second,
		Parameters:  map[string]string{"replication": "database"},
	})
	require.NoError(t, err, "open replication connection through the gateway")
	t.Cleanup(func() { _ = conn.Close() })

	rows, err := conn.Query(ctx, "IDENTIFY_SYSTEM")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	_, err = conn.Query(ctx, fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	require.NoError(t, err)
	_, err = conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s')", slot, pub))
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, v) VALUES (1, 'hello')", table))
	require.NoError(t, err)

	// The insert arrives as an XLogData ('w') frame; keepalives ('k') and
	// notices may come first.
	deadline := time.Now().Add(30 * time.Second)
	for {
		require.True(t, time.Now().Before(deadline), "no XLogData frame through the gateway")
		rctx, cancel := context.WithDeadline(ctx, deadline)
		msg, err := conn.ReadReplicationMessage(rctx)
		cancel()
		require.NoError(t, err)
		if msg.Notice == nil && len(msg.Data) > 0 && msg.Data[0] == 'w' {
			return
		}
	}
}
