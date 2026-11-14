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

package migration

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// A connection that is not valid is refused when it is created, naming the connection and no
// value of it, and nothing is stored. The rules are backingconn's; this is that the coordinator
// applies them, one case for each way a string is refused.
func TestCreateConnectionRefusesAnInvalidConnection(t *testing.T) {
	const secret = "s3cr3t"
	for name, dsn := range map[string]string{
		"a string that is read differently": "host=srv dbname=app user=bob password=" + secret + " morepw",
		"an invalid value":                  "host=h dbname=app user=alice password=" + secret + " sslmode=bogus",
		"a parameter that reads a file":     "postgres://u:" + secret + "@h:5432/app?passfile=/p",
		"no password":                       "host=h dbname=app user=alice",
	} {
		t.Run(name, func(t *testing.T) {
			tc := newTestCoord(t)
			conn := &Connection{Name: "bad", DSN: dsn}
			err := tc.c.CreateConnection(context.Background(), conn)
			require.ErrorIs(t, err, ErrInvalidConnection)
			require.ErrorContains(t, err, `connection "bad"`)
			require.NotContains(t, err.Error(), secret)
			require.Nil(t, conn.ConnInfo)
			_, getErr := tc.c.GetConnection(context.Background(), Ref{Name: "bad"}, "")
			require.ErrorIs(t, getErr, ErrConnectionNotFound, "nothing was stored")
		})
	}
}

// What is created is the structure that was checked, completed with what the connection left out
// (the port, the database, the sslmode), and its dsn is that structure's canonical rendering.
func TestCreateConnectionStoresTheCheckedStructure(t *testing.T) {
	tc := newTestCoord(t)
	conn := &Connection{Name: "src2", DSN: "postgres://alice:p%20w@db.example.com/app?sslrootcert=%2Fca.pem"}
	require.NoError(t, tc.c.CreateConnection(context.Background(), conn))

	want := &backingconn.Conn{
		Hosts: []string{"db.example.com"}, Ports: []int{5432}, Database: "app", User: "alice",
		Password: "p w", SSLMode: "require", SSLRootCert: "/ca.pem",
	}
	require.Equal(t, want, conn.ConnInfo)
	require.Equal(t, want.DSN(), conn.DSN)

	stored, err := tc.c.GetConnection(context.Background(), Ref{Name: "src2"}, "")
	require.NoError(t, err)
	require.Equal(t, conn.ConnInfo, stored.ConnInfo)
	require.Equal(t, conn.DSN, stored.DSN)

	// The dsn reads back as the same connection.
	back, err := backingconn.ParseDSN(stored.DSN)
	require.NoError(t, err)
	require.Equal(t, want, back)
}

// A connection for the migrator may carry what a pooler cannot: it is created, and the pooler's
// rules are applied where it is served.
func TestCreateConnectionDoesNotAskThatAPoolerCanFrontIt(t *testing.T) {
	for name, dsn := range map[string]string{
		"a connect timeout":   "host=h dbname=app user=u password=p connect_timeout=5",
		"an application name": "host=h port=5432 dbname=app user=u password=p application_name=mig",
		"a list of servers":   "host=a,b port=5432 dbname=app user=u password=p",
	} {
		t.Run(name, func(t *testing.T) {
			tc := newTestCoord(t)
			conn := &Connection{Name: "mig", DSN: dsn}
			require.NoError(t, tc.c.CreateConnection(context.Background(), conn))
			require.NotNil(t, conn.ConnInfo)
			require.Error(t, conn.ConnInfo.ForPooler(), "a pooler would refuse it, which is decided where it is served")
		})
	}
}

// sslrootcert=system, as in libpq 16: created with verify-full, and read by pgx, which the migrator
// dials the source with, as the system trust store.
func TestCreateConnectionAcceptsSystemRootCertificates(t *testing.T) {
	tc := newTestCoord(t)
	conn := &Connection{Name: "sys", DSN: "host=db.example.com dbname=app user=u password=p sslrootcert=system"}
	require.NoError(t, tc.c.CreateConnection(context.Background(), conn))
	require.Equal(t, "verify-full", conn.ConnInfo.SSLMode)

	cfg, err := pgx.ParseConfig(conn.DSN)
	require.NoError(t, err)
	require.NotNil(t, cfg.TLSConfig, "TLS is on")
	require.NotNil(t, cfg.TLSConfig.RootCAs, "the system CAs are the roots")
	require.Equal(t, "db.example.com", cfg.TLSConfig.ServerName, "and the host is verified")
}

func TestConnectionConn(t *testing.T) {
	conninfo := &backingconn.Conn{
		Hosts: []string{"from-conninfo"}, Ports: []int{5432}, Database: "app", User: "u", Password: "pw", SSLMode: "prefer",
	}
	c := &Connection{Name: "src", DSN: "host=not-read", ConnInfo: conninfo}
	got, err := c.Conn()
	require.NoError(t, err)
	require.Equal(t, "src", got.Name)
	require.Equal(t, []string{"from-conninfo"}, got.Hosts, "the dsn column is not read")

	// The result is the caller's own.
	got.Hosts[0] = "changed"
	got.Password = "changed"
	require.Equal(t, []string{"from-conninfo"}, conninfo.Hosts)
	require.Equal(t, "pw", conninfo.Password)
}

// A conninfo this version cannot read is refused, never read as less than it says.
func TestStoreMarksAConnectionWhoseConnInfoItCannotReadAsUnreadable(t *testing.T) {
	qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
		selectConnectionByNameSQL: {Rows: []*sqltypes.Row{connectionRowWithConnInfo(7, "src", "host=h", `{"host":["h"],"password":"s3cr3t","future":true}`)}},
	}}
	// The read itself succeeds, so that the caller's right to the connection is settled before
	// anyone is told what is wrong with it.
	conn, err := NewStore(qs).GetConnectionByRef(context.Background(), Ref{Name: "src"})
	require.NoError(t, err)
	require.ErrorIs(t, conn.Unreadable(), ErrConnectionUnreadable)
	require.NotContains(t, conn.Unreadable().Error(), "s3cr3t")
}

// TestAnUnreadableConnectionIsReportedOnlyToACallerWhoMayUseIt: a connection whose conninfo cannot be
// read is a fact about the row, reported as ErrConnectionUnreadable to a caller who may use the
// connection. To one who may not, it is exactly what a connection that does not exist is, so that
// the ownership check's masking holds for it too.
func TestAnUnreadableConnectionIsReportedOnlyToACallerWhoMayUseIt(t *testing.T) {
	// The connection is owned by alice. The fake target decides by the owner it is asked about,
	// so mayUse says whether the caller passes the ownership check.
	setup := func(t *testing.T, mayUse bool) *testCoord {
		tc := newTestCoord(t)
		tc.store.conns[defaultConnID].OwnerRole = "alice"
		tc.store.conns[defaultConnID].connInfoErr = fmt.Errorf("connection %q: %w", "src", ErrConnectionUnreadable)
		tc.tgt.checkConnectionOwnershipAllow = map[string]bool{"alice": mayUse}
		return tc
	}
	ctx := context.Background()

	t.Run("Get", func(t *testing.T) {
		_, err := setup(t, true).c.GetConnection(ctx, Ref{ID: defaultConnID}, "alice")
		require.ErrorIs(t, err, ErrConnectionUnreadable)

		_, err = setup(t, false).c.GetConnection(ctx, Ref{ID: defaultConnID}, "mallory")
		require.ErrorIs(t, err, ErrConnectionNotFound)
		require.NotErrorIs(t, err, ErrConnectionUnreadable)
	})

	t.Run("List omits what the caller may not use, whether or not it can be read", func(t *testing.T) {
		conns, err := setup(t, true).c.ListConnections(ctx, "alice")
		require.NoError(t, err, "a connection that cannot be read does not fail the listing")
		require.Len(t, conns, 1)
		require.ErrorIs(t, conns[0].Unreadable(), ErrConnectionUnreadable, "it is listed, and reports it")

		conns, err = setup(t, false).c.ListConnections(ctx, "mallory")
		require.NoError(t, err)
		require.Empty(t, conns)
	})

	t.Run("Drop removes it for a caller who may use it and masks it for anyone else", func(t *testing.T) {
		require.ErrorIs(t, setup(t, false).c.DropConnection(ctx, Ref{ID: defaultConnID}, "mallory", false), ErrConnectionNotFound)
		require.NoError(t, setup(t, true).c.DropConnection(ctx, Ref{ID: defaultConnID}, "alice", false),
			"an unreadable connection is the one that has to be dropped")
	})

	t.Run("a migration cannot be created from it, and a stranger is told it does not exist", func(t *testing.T) {
		p := CreateParams{ConnectionName: "src", TargetDatabase: "app", Tables: []string{"public.t"}}

		p.CallerRole = "mallory"
		_, err := setup(t, false).c.CreateMigration(ctx, p)
		require.ErrorContains(t, err, `connection "src" does not exist`)
		require.NotErrorIs(t, err, ErrConnectionUnreadable)

		p.CallerRole = "alice"
		_, err = setup(t, true).c.CreateMigration(ctx, p)
		require.ErrorIs(t, err, ErrConnectionUnreadable)
	})
}

// What a caller is shown is rendered from the conninfo, never the dsn column as it was stored:
// the column may say what the conninfo does not.
func TestConnectionRendering(t *testing.T) {
	conninfo := &backingconn.Conn{
		Hosts: []string{"from-conninfo"}, Ports: []int{5432}, Database: "app", User: "u", Password: "pw", SSLMode: "prefer",
	}
	c := &Connection{Name: "src", DSN: "host=from-dsn", ConnInfo: conninfo}
	require.Equal(t, conninfo.DSN(), c.Rendering())

	require.Empty(t, (&Connection{Name: "bad", ConnInfo: &backingconn.Conn{Hosts: []string{"h"}}}).Rendering(), "a connection that is not valid renders as nothing")
	require.Empty(t, (&Connection{Name: "bad", connInfoErr: ErrConnectionUnreadable}).Rendering())
}

// A stored conninfo that is valid JSON and a connection this version reads, but is not a valid
// connection (the CHECK constraint of the column cannot catch it all), is refused where it is
// used, naming the connection and no value of it.
func TestAStoredConnInfoThatIsNotValidIsRefusedWhereItIsUsed(t *testing.T) {
	for name, conninfo := range map[string]string{
		"no port":                       `{"host":["h"],"dbname":"d","user":"u","password":"s3cr3t","sslmode":"prefer"}`,
		"port 0":                        `{"host":["h"],"port":[0],"dbname":"d","user":"u","password":"s3cr3t","sslmode":"prefer"}`,
		"a bogus sslmode":               `{"host":["h"],"port":[5432],"dbname":"d","user":"u","password":"s3cr3t","sslmode":"bogus"}`,
		"a host with a space":           `{"host":["a b"],"port":[5432],"dbname":"d","user":"u","password":"s3cr3t","sslmode":"prefer"}`,
		"a bad integer parameter":       `{"host":["h"],"port":[5432],"dbname":"d","user":"u","password":"s3cr3t","sslmode":"prefer","params":{"connect_timeout":"abc"}}`,
		"a parameter that reads a file": `{"host":["h"],"port":[5432],"dbname":"d","user":"u","password":"s3cr3t","sslmode":"prefer","params":{"passfile":"/p"}}`,
		"no password":                   `{"host":["h"],"port":[5432],"dbname":"d","user":"u","sslmode":"prefer"}`,
	} {
		t.Run(name, func(t *testing.T) {
			tc := newTestCoord(t)
			stored := tc.store.conns[defaultConnID]
			decoded, err := DecodeConnInfo("src", conninfo)
			require.NoError(t, err)
			stored.ConnInfo = decoded

			_, err = stored.Conn()
			require.ErrorIs(t, err, ErrInvalidConnection)
			require.ErrorContains(t, err, `connection "src"`)
			require.NotContains(t, err.Error(), "s3cr3t")

			// Where the migrator uses it: to open a source, and to create a migration.
			_, err = tc.c.openSource(context.Background(), &Migration{ConnectionID: defaultConnID})
			require.ErrorIs(t, err, ErrInvalidConnection)
			_, err = tc.c.CreateMigration(context.Background(), CreateParams{
				ConnectionName: "src", TargetDatabase: "app", Tables: []string{"public.orders"},
			})
			require.ErrorIs(t, err, ErrInvalidConnection)
			require.Empty(t, tc.dialed)
		})
	}
}

// A conninfo this version cannot read is refused, naming the connection and no value of it.
func TestDecodeConnInfo(t *testing.T) {
	const conninfo = `{"host":["from-conninfo"],"port":[5432],"dbname":"app","user":"u","password":"pw","sslmode":"prefer"}`
	got, err := DecodeConnInfo("src", conninfo)
	require.NoError(t, err)
	require.Equal(t, &backingconn.Conn{
		Hosts: []string{"from-conninfo"}, Ports: []int{5432}, Database: "app", User: "u", Password: "pw", SSLMode: "prefer",
	}, got)

	_, err = DecodeConnInfo("future", `{"host":["h"],"password":"s3cr3t","future":true}`)
	require.ErrorIs(t, err, ErrConnectionUnreadable)
	require.NotContains(t, err.Error(), "s3cr3t")
}

// A connection with no conninfo (a NULL in the column, which a connection stored before the column
// existed keeps when setup could not convert its dsn) is a connection that cannot be read, which
// the caller who may use it is told to drop and create again.
func TestAConnectionWithNoConnInfoIsUnreadable(t *testing.T) {
	_, err := DecodeConnInfo("legacy", "")
	require.ErrorIs(t, err, ErrConnectionUnreadable)
	require.Contains(t, err.Error(), `connection "legacy"`)
	require.Contains(t, err.Error(), "drop it and create it again")

	qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
		selectConnectionByNameSQL: {Rows: []*sqltypes.Row{connectionRowWithConnInfo(7, "legacy", "host=h password=s3cr3t", "")}},
	}}
	conn, err := NewStore(qs).GetConnectionByRef(context.Background(), Ref{Name: "legacy"})
	require.NoError(t, err, "the read itself succeeds, as for any unreadable connection")
	require.ErrorIs(t, conn.Unreadable(), ErrConnectionUnreadable)
	require.NotContains(t, conn.Unreadable().Error(), "s3cr3t")
	_, err = conn.Conn()
	require.ErrorIs(t, err, ErrConnectionUnreadable)
	require.Empty(t, conn.Rendering())
}
