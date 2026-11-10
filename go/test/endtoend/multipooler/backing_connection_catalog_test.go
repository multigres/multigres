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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/backingconn"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestBackingConnectionsAreServedFromTheCatalog verifies, against a real catalog shard, that the
// default primary says how a tablegroup is served from the catalog: GetBackingConnection returns
// the connection a tablegroup is bound to in multigres.connections, the table the migrator shares,
// structured, whether it was written in SQL as an operator does or created through the migrator,
// and says that a tablegroup with no binding is managed.
func TestBackingConnectionsAreServedFromTheCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	// The test creates tablegroups, which advance the catalog's versions, so it does not share
	// the catalog of the package's shared setup, whose versions other tests read as untouched.
	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()
	primary := setup.GetPrimary(t)

	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer primaryDB.Close()
	var database string
	require.NoError(t, primaryDB.QueryRow(`SELECT current_database()`).Scan(&database))

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", primary.Multipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	pooler := multipoolerservicepb.NewMultipoolerServiceClient(conn)
	migrator := migratorpb.NewMigratorClient(conn)
	manager := multipoolermanagerpb.NewMultipoolerManagerClient(conn)

	// A connection for an unmanaged pooler, written in SQL as the operator does, and one the
	// migrator creates. Both name the database the cluster serves, which a tablegroup bound to them
	// must.
	_, err = primaryDB.Exec(fmt.Sprintf(`INSERT INTO multigres.connections (name, dsn, conninfo)
		VALUES ('backing-src', 'not read', '{"host":["db.example.com"],"port":[6543],"dbname":"%s","user":"alice","password":"s3cret","sslmode":"verify-full","sslrootcert":"/ca.pem"}')`, database))
	require.NoError(t, err)
	_, err = migrator.CreateConnection(utils.WithShortDeadline(t), &migratorpb.CreateConnectionRequest{
		Connection: &migratorpb.Connection{Name: "migrator-src", Dsn: fmt.Sprintf("host=source.example.com port=5433 dbname=%s user=repl password='p w'", database)},
	})
	require.NoError(t, err)

	create := func(name, connection string) int64 {
		t.Helper()
		resp, err := manager.CreateTableGroup(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.CreateTableGroupRequest{Name: name, BackingConnection: connection})
		require.NoError(t, err)
		return resp.GetTablegroupId()
	}
	backingID := create("backingtg", "backing-src")
	migratorID := create("migratortg", "migrator-src")
	managedID := create("managedtg", "")

	get := func(id int64) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
		return pooler.GetBackingConnection(utils.WithShortDeadline(t),
			&multipoolerservicepb.GetBackingConnectionRequest{Database: database, TablegroupId: id})
	}

	t.Run("GetBackingConnection", func(t *testing.T) {
		resp, err := get(backingID)
		require.NoError(t, err)
		assert.False(t, resp.GetManaged())
		got := resp.GetConnection()
		assert.Equal(t, "backing-src", got.GetName())
		assert.Equal(t, []string{"db.example.com"}, got.GetHosts())
		assert.Equal(t, []int32{6543}, got.GetPorts())
		assert.Equal(t, database, got.GetDatabase())
		assert.Equal(t, "alice", got.GetUser())
		assert.Equal(t, "s3cret", got.GetPassword())
		assert.Equal(t, "verify-full", got.GetSslMode())
		assert.Equal(t, "/ca.pem", got.GetSslRootCert())
	})

	t.Run("a tablegroup with no binding is managed", func(t *testing.T) {
		resp, err := get(managedID)
		require.NoError(t, err)
		assert.True(t, resp.GetManaged())
		assert.Nil(t, resp.GetConnection())
	})

	t.Run("a tablegroup that does not exist is not found, and an id is required", func(t *testing.T) {
		_, err := get(migratorID + 1000)
		assert.Equal(t, codes.NotFound, status.Code(err))
		_, err = get(0)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
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
		resp, err := get(migratorID)
		require.NoError(t, err)
		assert.Equal(t, "migrator-src", resp.GetConnection().GetName())
		assert.Equal(t, []string{"source.example.com"}, resp.GetConnection().GetHosts())
		assert.Equal(t, []int32{5433}, resp.GetConnection().GetPorts())
		assert.Equal(t, database, resp.GetConnection().GetDatabase())
		assert.Equal(t, "repl", resp.GetConnection().GetUser())
		assert.Equal(t, "p w", resp.GetConnection().GetPassword())
	})

	t.Run("a connection created through the migrator is stored structured and served without parsing", func(t *testing.T) {
		var dsn, conninfo string
		require.NoError(t, primaryDB.QueryRow(
			`SELECT dsn, conninfo::text FROM multigres.connections WHERE name = 'migrator-src'`).Scan(&dsn, &conninfo))
		assert.Equal(t, fmt.Sprintf(`dbname='%s' host='source.example.com' password='p w' port='5433' sslmode='require' user='repl'`, database), dsn)
		assert.JSONEq(t, fmt.Sprintf(`{"host":["source.example.com"],"port":[5433],"dbname":"%s","user":"repl","password":"p w","sslmode":"require"}`, database), conninfo)

		// Only the conninfo is read: with the dsn made garbage the connection is served all the same.
		_, err := primaryDB.Exec(`UPDATE multigres.connections SET dsn = 'not a dsn' WHERE name = 'migrator-src'`)
		require.NoError(t, err)
		resp, err := get(migratorID)
		require.NoError(t, err)
		assert.Equal(t, []string{"source.example.com"}, resp.GetConnection().GetHosts())
		assert.Equal(t, "p w", resp.GetConnection().GetPassword())
	})

	t.Run("a connection libpq would read differently is refused when it is created", func(t *testing.T) {
		const dsn = "host=h port=5432 dbname=app user=alice password=s3cret morepw"
		_, err := migrator.CreateConnection(utils.WithShortDeadline(t), &migratorpb.CreateConnectionRequest{
			Connection: &migratorpb.Connection{Name: "invalid-src", Dsn: dsn},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
		assert.NotContains(t, fmt.Sprint(err), "s3cret")
		var n int
		require.NoError(t, primaryDB.QueryRow(`SELECT count(*) FROM multigres.connections WHERE name = 'invalid-src'`).Scan(&n))
		assert.Zero(t, n, "nothing was stored")
	})

	t.Run("a connection for the migrator that a pooler cannot front is created, and refused when a tablegroup is bound to it", func(t *testing.T) {
		_, err := migrator.CreateConnection(utils.WithShortDeadline(t), &migratorpb.CreateConnectionRequest{
			Connection: &migratorpb.Connection{Name: "migrator-cert", Dsn: fmt.Sprintf("host=source.example.com port=5433 dbname=%s user=repl password=pw sslmode=require connect_timeout=5", database)},
		})
		require.NoError(t, err)
		_, err = manager.CreateTableGroup(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.CreateTableGroupRequest{Name: "certtg", BackingConnection: "migrator-cert"})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		assert.Contains(t, fmt.Sprint(err), "connect_timeout")
	})

	t.Run("sslrootcert=system is created with verify-full and served as it is", func(t *testing.T) {
		_, err := migrator.CreateConnection(utils.WithShortDeadline(t), &migratorpb.CreateConnectionRequest{
			Connection: &migratorpb.Connection{Name: "migrator-system", Dsn: fmt.Sprintf("host=source.example.com port=5433 dbname=%s user=repl password=pw sslrootcert=system", database)},
		})
		require.NoError(t, err)
		resp, err := get(create("systemtg", "migrator-system"))
		require.NoError(t, err)
		assert.Equal(t, "system", resp.GetConnection().GetSslRootCert())
	})

	t.Run("the catalog refuses a conninfo that is structurally wrong, whoever writes it", func(t *testing.T) {
		insert := func(conninfo string) error {
			_, err := primaryDB.Exec(`INSERT INTO multigres.connections (name, dsn, conninfo) VALUES ('check-src', 'host=h', $1::jsonb)`, conninfo)
			if err == nil {
				_, delErr := primaryDB.Exec(`DELETE FROM multigres.connections WHERE name = 'check-src'`)
				require.NoError(t, delErr)
			}
			return err
		}
		for name, ok := range map[string]string{
			"one server":          `{"host":["h"],"port":[5432]}`,
			"two servers":         `{"host":["a","b"],"port":[1,2],"dbname":"d"}`,
			"two hosts, one port": `{"host":["a","b"],"port":[5432]}`,
		} {
			assert.NoError(t, insert(ok), name)
		}
		for name, bad := range map[string]string{
			"not an object":           `[]`,
			"no host or port":         `{}`,
			"a host that is a string": `{"host":"h","port":[5432]}`,
			"a port that is a number": `{"host":["h"],"port":5432}`,
			"no hosts":                `{"host":[],"port":[5432]}`,
			"no ports":                `{"host":["h"],"port":[]}`,
			"an empty host":           `{"host":[""],"port":[5432]}`,
			"a host that is a number": `{"host":[1],"port":[5432]}`,
			"a null host":             `{"host":[null],"port":[5432]}`,
			"a nested host":           `{"host":[["h"]],"port":[5432]}`,
			"port 0":                  `{"host":["h"],"port":[0]}`,
			"port 65536":              `{"host":["h"],"port":[65536]}`,
			"a negative port":         `{"host":["h"],"port":[-1]}`,
			"a fractional port":       `{"host":["h"],"port":[1.5]}`,
			"a port that is a string": `{"host":["h"],"port":["5432"]}`,
			"a port that is a bool":   `{"host":["h"],"port":[true]}`,
			"ports that do not fit":   `{"host":["a","b","c"],"port":[1,2]}`,
			"two ports for a host":    `{"host":["a"],"port":[1,2]}`,
		} {
			err := insert(bad)
			require.Error(t, err, name)
			assert.Contains(t, err.Error(), "connections_conninfo_servers", name)
		}
	})

	t.Run("a connection a tablegroup is bound to cannot be dropped through the migrator", func(t *testing.T) {
		_, err := migrator.DropConnection(utils.WithShortDeadline(t), &migratorpb.DropConnectionRequest{
			Ref: &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Name{Name: "migrator-src"}},
		})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		assert.Contains(t, err.Error(), "referenced by a migration or a tablegroup")
	})
}

// TestConnectionsStoredBeforeTheConnInfoColumnAreConvertedWhenTheCatalogIsSetUp verifies the upgrade
// of a catalog that has connections and no conninfo column, as main's multigres.connections has
// it: opening the catalog adds the column, converts the dsn of every connection that can be a
// connection into its conninfo, and leaves the others without one, which reads as a connection to
// drop and create again.
func TestConnectionsStoredBeforeTheConnInfoColumnAreConvertedWhenTheCatalogIsSetUp(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()
	primary := setup.GetPrimary(t)
	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer primaryDB.Close()
	var database string
	require.NoError(t, primaryDB.QueryRow(`SELECT current_database()`).Scan(&database))

	// The table as it was before the column: the column goes, and its constraint with it.
	_, err := primaryDB.Exec(`ALTER TABLE multigres.connections DROP COLUMN conninfo`)
	require.NoError(t, err)
	checkCount := func() (n int) {
		require.NoError(t, primaryDB.QueryRow(
			`SELECT count(*) FROM pg_constraint WHERE conname = 'connections_conninfo_servers'`).Scan(&n))
		return n
	}
	require.Zero(t, checkCount())

	validDSN := fmt.Sprintf("host=legacy.example.com port=6543 dbname=%s user=alice password='s3 cret' sslmode=verify-full sslrootcert=system", database)
	legacyDSNs := map[string]string{
		"legacy-ok":          validDSN,
		"legacy-no-password": fmt.Sprintf("host=legacy.example.com dbname=%s user=alice", database),
		"legacy-socket":      fmt.Sprintf("host=/var/run/postgresql dbname=%s user=alice password=pw", database),
	}
	for name, dsn := range legacyDSNs {
		_, err := primaryDB.Exec(`INSERT INTO multigres.connections (name, dsn, owner_role) VALUES ($1, $2, 'legacy_owner')`, name, dsn)
		require.NoError(t, err, name)
	}

	restartMultipooler(t, primary)

	conninfoOf := func(name string) (conninfo *string) {
		require.NoError(t, primaryDB.QueryRow(
			`SELECT conninfo::text FROM multigres.connections WHERE name = $1`, name).Scan(&conninfo))
		return conninfo
	}
	require.Eventually(t, func() bool {
		var filled bool
		err := primaryDB.QueryRow(`SELECT conninfo IS NOT NULL FROM multigres.connections WHERE name = 'legacy-ok'`).Scan(&filled)
		return err == nil && filled
	}, 30*time.Second, 200*time.Millisecond, "opening the catalog should convert the connection")

	dsnOf := func(name string) (dsn string) {
		require.NoError(t, primaryDB.QueryRow(`SELECT dsn FROM multigres.connections WHERE name = $1`, name).Scan(&dsn))
		return dsn
	}

	t.Run("a connection that can be converted is stored as CreateConnection stores it", func(t *testing.T) {
		want, err := backingconn.FromDSN(validDSN)
		require.NoError(t, err)
		wantJSON, err := json.Marshal(want)
		require.NoError(t, err)
		got := conninfoOf("legacy-ok")
		require.NotNil(t, got)
		assert.JSONEq(t, string(wantJSON), *got)
		assert.Equal(t, want.DSN(), dsnOf("legacy-ok"), "the dsn is the canonical rendering, not what was written")
		assert.NotEqual(t, validDSN, dsnOf("legacy-ok"))
	})

	t.Run("a connection that cannot be converted keeps no conninfo and its dsn", func(t *testing.T) {
		for _, name := range []string{"legacy-no-password", "legacy-socket"} {
			assert.Nil(t, conninfoOf(name), name)
			assert.Equal(t, legacyDSNs[name], dsnOf(name), name)
		}
	})

	t.Run("the constraint is added and still refuses a structure that is wrong", func(t *testing.T) {
		assert.Equal(t, 1, checkCount())
		_, err := primaryDB.Exec(`INSERT INTO multigres.connections (name, dsn, conninfo) VALUES ('check-src', 'host=h', '{"host":[],"port":[5432]}'::jsonb)`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connections_conninfo_servers")
		_, err = primaryDB.Exec(`INSERT INTO multigres.connections (name, dsn, conninfo) VALUES ('check-src', 'host=h', '{"host":["h"],"port":[5432]}'::jsonb)`)
		assert.NoError(t, err, "a structure that is right is accepted")
		_, err = primaryDB.Exec(`DELETE FROM multigres.connections WHERE name = 'check-src'`)
		require.NoError(t, err)
	})

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", primary.Multipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	migrator := migratorpb.NewMigratorClient(conn)
	manager := multipoolermanagerpb.NewMultipoolerManagerClient(conn)
	byName := func(name string) *migratorpb.ConnectionRef {
		return &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Name{Name: name}}
	}

	t.Run("a converted connection is served", func(t *testing.T) {
		// The restarted pooler is the catalog's primary again once it has taken its role back.
		var created *multipoolermanagerdatapb.CreateTableGroupResponse
		require.Eventually(t, func() bool {
			var err error
			created, err = manager.CreateTableGroup(utils.WithShortDeadline(t),
				&multipoolermanagerdatapb.CreateTableGroupRequest{Name: "legacytg", BackingConnection: "legacy-ok"})
			return err == nil
		}, 60*time.Second, 500*time.Millisecond, "a tablegroup can be bound to the converted connection")
		pooler := multipoolerservicepb.NewMultipoolerServiceClient(conn)
		resp, err := pooler.GetBackingConnection(utils.WithShortDeadline(t),
			&multipoolerservicepb.GetBackingConnectionRequest{Database: database, TablegroupId: created.GetTablegroupId()})
		require.NoError(t, err)
		assert.Equal(t, []string{"legacy.example.com"}, resp.GetConnection().GetHosts())
		assert.Equal(t, []int32{6543}, resp.GetConnection().GetPorts())
		assert.Equal(t, "s3 cret", resp.GetConnection().GetPassword())
	})

	t.Run("a connection with no conninfo is one that cannot be read", func(t *testing.T) {
		for _, name := range []string{"legacy-no-password", "legacy-socket"} {
			_, err := migrator.GetConnection(utils.WithShortDeadline(t), &migratorpb.GetConnectionRequest{Ref: byName(name)})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err), "%s: %v", name, err)
			assert.Contains(t, fmt.Sprint(err), "drop it and create it again", name)
			assert.NotContains(t, fmt.Sprint(err), "pw", name)
		}
		_, err := manager.CreateTableGroup(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.CreateTableGroupRequest{Name: "nulltg", BackingConnection: "legacy-no-password"})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		assert.Contains(t, fmt.Sprint(err), "drop it and create it again", "a tablegroup cannot be bound to it")
	})

	t.Run("a connection that cannot be read is listed with an empty dsn", func(t *testing.T) {
		resp, err := migrator.ListConnections(utils.WithShortDeadline(t), &migratorpb.ListConnectionsRequest{})
		require.NoError(t, err, "one connection that cannot be read does not fail the listing")
		listed := map[string]*migratorpb.Connection{}
		for _, c := range resp.GetConnections() {
			listed[c.GetName()] = c
		}
		require.Len(t, listed, 3)
		assert.Contains(t, listed["legacy-ok"].GetDsn(), "legacy.example.com")
		for _, name := range []string{"legacy-no-password", "legacy-socket"} {
			c := listed[name]
			assert.Empty(t, c.GetDsn(), name)
			assert.Equal(t, "legacy_owner", c.GetOwnerRole(), name)
			assert.NotZero(t, c.GetId(), name)
			assert.NotNil(t, c.GetCreatedAt(), name)
		}
	})

	t.Run("a connection with no conninfo can be dropped", func(t *testing.T) {
		for _, name := range []string{"legacy-no-password", "legacy-socket"} {
			_, err := migrator.DropConnection(utils.WithShortDeadline(t), &migratorpb.DropConnectionRequest{Ref: byName(name)})
			require.NoError(t, err, name)
		}
		resp, err := migrator.ListConnections(utils.WithShortDeadline(t), &migratorpb.ListConnectionsRequest{})
		require.NoError(t, err)
		require.Len(t, resp.GetConnections(), 1)
		assert.Equal(t, "legacy-ok", resp.GetConnections()[0].GetName())
	})
}
