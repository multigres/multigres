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

package manager

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// connectionRow is the row of multigres.connections that the migrator's store reads, for the
// connection a libpq string names (completed, as CREATE CONNECTION stores it).
func connectionRow(name, dsn string) *sqltypes.Result {
	conn, err := backingconn.ParseDSN(dsn)
	if err != nil {
		panic(err)
	}
	conn.Complete()
	data, err := json.Marshal(conn)
	if err != nil {
		panic(err)
	}
	return connectionRowWithConnInfo(name, dsn, string(data))
}

// connectionRowWithConnInfo is a row whose conninfo column holds the given JSON.
func connectionRowWithConnInfo(name, dsn, conninfo string) *sqltypes.Result {
	return mock.MakeQueryResult(
		[]string{"connection_id", "name", "dsn", "owner_role", "created_at", "conninfo"},
		[][]any{{int64(1), name, dsn, "", "2026-01-01T00:00:00Z", conninfo}})
}

// TestBackingConnectionIsReadFromTheCatalog: the connection is a row of multigres.connections,
// read with one query, and its conninfo is the connection.
func TestBackingConnectionIsReadFromTheCatalog(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections WHERE name", connectionRow("src", "postgres://u:p@h:5432/db"))
		conn, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "src", conn.GetName())
		require.Equal(t, []string{"h"}, conn.GetHosts())
		require.Equal(t, []int32{5432}, conn.GetPorts())
		require.Equal(t, "u", conn.GetUser())
		require.EqualValues(t, 1, qs.AdminQueryCount(), "one query, nothing created")
	})
	t.Run("not found", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections", mock.MakeQueryResult(nil, nil))
		_, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.NoError(t, err)
		require.False(t, found)
	})
	t.Run("a catalog without the connections table is a precondition failure naming the catalog shard", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError("FROM multigres.connections",
			&mterrors.PgDiagnostic{Code: mterrors.PgSSUndefinedTable, Message: `relation "multigres.connections" does not exist`})
		_, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.False(t, found)
		require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
		require.ErrorContains(t, err, "connections live on the catalog shard")
	})
	t.Run("another failure of the read is passed on as it is", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError("FROM multigres.connections", errors.New("some other failure"))
		_, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.False(t, found)
		require.ErrorContains(t, err, "some other failure")
	})
	t.Run("a lost connection to the database is UNAVAILABLE, which the caller retries", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError("FROM multigres.connections",
			&mterrors.PgDiagnostic{Code: "08006", Message: "connection failure"})
		_, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.False(t, found)
		require.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "%v", err)
		require.ErrorContains(t, err, `read connection "src"`)
	})
	t.Run("no query service is UNAVAILABLE, not an unknown error", func(t *testing.T) {
		store := &connectionStore{queryService: func() executor.InternalQueryService { return nil }}
		_, found, err := store.BackingConnection(t.Context(), "src")
		require.False(t, found)
		require.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "%v", err)
	})
	t.Run("a connection that does not name one server is refused with its code, naming the connection", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections", connectionRow("src", "host=h1,h2 port=5432 dbname=app user=u password=p"))
		_, _, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
		require.ErrorContains(t, err, `connection "src"`)
	})
}

// A connection is served from its stored conninfo, and only the conninfo: the dsn column is not read.
func TestBackingConnectionIsServedFromTheStoredConnInfo(t *testing.T) {
	read := func(t *testing.T, row *sqltypes.Result) (*multipoolerservicepb.BackingConnection, error) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections", row)
		conn, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		if err == nil {
			require.True(t, found)
		}
		return conn, err
	}
	bound := func(dsn, conninfo string) *sqltypes.Result {
		return connectionRowWithConnInfo("src", dsn, conninfo)
	}

	t.Run("a conninfo", func(t *testing.T) {
		// The dsn column is not even a connection string: only the conninfo is read.
		conn, err := read(t, bound("not a dsn",
			`{"host":["db.example.com"],"port":[6543],"dbname":"app","user":"alice","password":"pw","sslmode":"verify-full","sslrootcert":"/ca.pem"}`))
		require.NoError(t, err)
		require.Equal(t, "src", conn.GetName())
		require.Equal(t, []string{"db.example.com"}, conn.GetHosts())
		require.Equal(t, []int32{6543}, conn.GetPorts())
		require.Equal(t, "alice", conn.GetUser())
		require.Equal(t, "pw", conn.GetPassword())
		require.Equal(t, "verify-full", conn.GetSslMode())
		require.Equal(t, "/ca.pem", conn.GetSslRootCert())
	})
	t.Run("a connection that is not valid is refused", func(t *testing.T) {
		_, err := read(t, bound("host=h", `{"host":["h"],"port":[5432],"dbname":"app","user":"u","sslmode":"prefer"}`))
		require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
	})
	t.Run("a connection a pooler cannot front is refused by the rules of the pooler", func(t *testing.T) {
		for name, conninfo := range map[string]string{
			"a parameter it does not carry": `{"host":["h"],"port":[5432],"dbname":"app","user":"u","password":"p","sslmode":"prefer","params":{"application_name":"x"}}`,
			"more than one server":          `{"host":["a","b"],"port":[5432],"dbname":"app","user":"u","password":"p","sslmode":"prefer"}`,
		} {
			t.Run(name, func(t *testing.T) {
				_, err := read(t, bound("host=h", conninfo))
				require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
				require.ErrorContains(t, err, `connection "src"`)
			})
		}
	})
	t.Run("a conninfo this version cannot read is refused, never read as less", func(t *testing.T) {
		_, err := read(t, bound("host=h port=5432 dbname=app user=u", `{"host":["h"],"password":"s3cr3t","future":true}`))
		require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
		require.NotContains(t, err.Error(), "s3cr3t")
	})
}

// TestTheCatalogIsTheDefaultDirectory: a manager with no directory of its own reads
// connections from the sharding catalog.
func TestTheCatalogIsTheDefaultDirectory(t *testing.T) {
	pm, _ := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
	require.IsType(t, &connectionStore{}, pm.connections())
}
