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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/utils"
)

// TestBackingConnectionsAreServedFromTheCatalog verifies, against a real
// catalog shard, that the default primary serves a backing connection from
// multigres.connections, the table the migrator shares: GetBackingConnection
// returns a connection, structured, whether it was written in SQL as an
// operator does or created through the migrator.
func TestBackingConnectionsAreServedFromTheCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(setup.PrimaryPgctld.PoolerDir), setup.PrimaryPgctld.PgPort)
	defer primaryDB.Close()
	var database string
	require.NoError(t, primaryDB.QueryRow(`SELECT current_database()`).Scan(&database))

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	pooler := multipoolerservicepb.NewMultipoolerServiceClient(conn)
	migrator := migratorpb.NewMigratorClient(conn)

	// The shared setup is used by other tests: remove what this one added. This
	// runs before primaryDB is closed (defers run last in, first out).
	defer func() {
		if _, err := primaryDB.Exec(`DELETE FROM multigres.connections WHERE name IN ('backing-src', 'migrator-src')`); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()

	// A connection for an unmanaged pooler, written in SQL as the operator does,
	// and one the migrator creates, in the keyword=value form it stores.
	_, err = primaryDB.Exec(`INSERT INTO multigres.connections (name, dsn)
		VALUES ('backing-src', 'postgres://alice:s3cret@db.example.com:6543/app?sslmode=verify-full&sslrootcert=/ca.pem')`)
	require.NoError(t, err)
	_, err = migrator.CreateConnection(utils.WithShortDeadline(t), &migratorpb.CreateConnectionRequest{
		Connection: &migratorpb.Connection{Name: "migrator-src", Dsn: "host=source.example.com port=5433 dbname=app user=repl password='p w'"},
	})
	require.NoError(t, err)

	t.Run("GetBackingConnection", func(t *testing.T) {
		resp, err := pooler.GetBackingConnection(utils.WithShortDeadline(t),
			&multipoolerservicepb.GetBackingConnectionRequest{Database: database, ConnectionName: "backing-src"})
		require.NoError(t, err)
		got := resp.GetConnection()
		assert.Equal(t, "backing-src", got.GetName())
		assert.Equal(t, "db.example.com", got.GetHost())
		assert.EqualValues(t, 6543, got.GetPort())
		assert.Equal(t, "app", got.GetDatabase())
		assert.Equal(t, "alice", got.GetUser())
		assert.Equal(t, "s3cret", got.GetPassword())
		assert.Equal(t, "verify-full", got.GetSslMode())
		assert.Equal(t, "/ca.pem", got.GetSslRootCert())

		_, err = pooler.GetBackingConnection(utils.WithShortDeadline(t),
			&multipoolerservicepb.GetBackingConnectionRequest{Database: database, ConnectionName: "no-such-connection"})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("the table is shared with the migrator", func(t *testing.T) {
		resp, err := migrator.ListConnections(utils.WithShortDeadline(t), &migratorpb.ListConnectionsRequest{})
		require.NoError(t, err)
		var names []string
		for _, c := range resp.GetConnections() {
			names = append(names, c.GetName())
		}
		assert.Contains(t, names, "migrator-src")
		assert.Contains(t, names, "backing-src")
	})

	t.Run("a connection the migrator created is servable as it is", func(t *testing.T) {
		// Its dsn is the keyword=value form the migrator stores; both forms are read alike.
		resp, err := pooler.GetBackingConnection(utils.WithShortDeadline(t),
			&multipoolerservicepb.GetBackingConnectionRequest{Database: database, ConnectionName: "migrator-src"})
		require.NoError(t, err)
		assert.Equal(t, "source.example.com", resp.GetConnection().GetHost())
		assert.EqualValues(t, 5433, resp.GetConnection().GetPort())
		assert.Equal(t, "app", resp.GetConnection().GetDatabase())
		assert.Equal(t, "repl", resp.GetConnection().GetUser())
		assert.Equal(t, "p w", resp.GetConnection().GetPassword())
	})

	t.Run("a dsn that is not usable is refused", func(t *testing.T) {
		_, err := primaryDB.Exec(`UPDATE multigres.connections SET dsn = 'postgres://alice@db.example.com:5432/app?channel_binding=require' WHERE name = 'backing-src'`)
		require.NoError(t, err)
		_, err = pooler.GetBackingConnection(utils.WithShortDeadline(t),
			&multipoolerservicepb.GetBackingConnectionRequest{Database: database, ConnectionName: "backing-src"})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "a parameter that is not carried is refused, not dropped: %v", err)
		_, err = primaryDB.Exec(`UPDATE multigres.connections SET dsn = 'postgres://alice@db.example.com:5432/app' WHERE name = 'backing-src'`)
		require.NoError(t, err)
	})
}
