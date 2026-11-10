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

// bindingRow is the row the catalog gives for a tablegroup: its state and the name and conninfo of
// the connection it is bound to (empty for a managed tablegroup). The connection is the one a libpq string
// names, completed as CREATE CONNECTION stores it.
func bindingRow(state, name, dsn string) *sqltypes.Result {
	var conninfo string
	if dsn != "" {
		conn, err := backingconn.ParseDSN(dsn)
		if err != nil {
			panic(err)
		}
		conn.Complete()
		data, err := json.Marshal(conn)
		if err != nil {
			panic(err)
		}
		conninfo = string(data)
	}
	return bindingRowWithConnInfo(state, name, conninfo)
}

// bindingRowWithConnInfo is a row whose connection's conninfo column holds the given JSON.
func bindingRowWithConnInfo(state, name, conninfo string) *sqltypes.Result {
	return mock.MakeQueryResult(
		[]string{"state", "name", "conninfo"},
		[][]any{{state, name, conninfo}})
}

const bindingQuery = "FROM multigres.tablegroup t LEFT JOIN multigres.connections c"

// TestTableGroupBindingIsReadFromTheCatalog: how a tablegroup is served is its row and the
// connection the row names, read with one query, and its conninfo is the connection.
func TestTableGroupBindingIsReadFromTheCatalog(t *testing.T) {
	binding := func(t *testing.T, row *sqltypes.Result) (*mock.QueryService, *connectionStore) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern(bindingQuery, row)
		return qs, newConnectionStore(pm)
	}
	t.Run("a bound tablegroup", func(t *testing.T) {
		qs, store := binding(t, bindingRow(tableGroupStateActive, "src", "postgres://u:p@h:5432/db"))
		conn, found, err := store.TableGroupBinding(t.Context(), 7)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "src", conn.GetName())
		require.Equal(t, []string{"h"}, conn.GetHosts())
		require.Equal(t, []int32{5432}, conn.GetPorts())
		require.Equal(t, "u", conn.GetUser())
		require.EqualValues(t, 1, qs.AdminQueryCount(), "one query, nothing created")
	})
	t.Run("a managed tablegroup has no connection", func(t *testing.T) {
		_, store := binding(t, bindingRow(tableGroupStateCreating, "", ""))
		conn, found, err := store.TableGroupBinding(t.Context(), 7)
		require.NoError(t, err)
		require.True(t, found)
		require.Nil(t, conn)
	})
	t.Run("no such tablegroup", func(t *testing.T) {
		_, store := binding(t, mock.MakeQueryResult(nil, nil))
		conn, found, err := store.TableGroupBinding(t.Context(), 7)
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, conn)
	})
	t.Run("a tablegroup being dropped is served to no one", func(t *testing.T) {
		_, store := binding(t, bindingRow(tableGroupStateDropping, "src", "postgres://u:p@h:5432/db"))
		_, found, err := store.TableGroupBinding(t.Context(), 7)
		require.False(t, found)
		require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
	})
	t.Run("a failure of the read is passed on as it is", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError(bindingQuery, errors.New("some other failure"))
		_, found, err := newConnectionStore(pm).TableGroupBinding(t.Context(), 7)
		require.False(t, found)
		require.ErrorContains(t, err, "some other failure")
	})
	t.Run("a lost connection to the database is UNAVAILABLE, which the caller retries", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError(bindingQuery,
			&mterrors.PgDiagnostic{Code: "08006", Message: "connection failure"})
		_, found, err := newConnectionStore(pm).TableGroupBinding(t.Context(), 7)
		require.False(t, found)
		require.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "%v", err)
		require.ErrorContains(t, err, "read tablegroup 7")
	})
	t.Run("no query service is UNAVAILABLE, not an unknown error", func(t *testing.T) {
		store := &connectionStore{queryService: func() executor.InternalQueryService { return nil }}
		_, found, err := store.TableGroupBinding(t.Context(), 7)
		require.False(t, found)
		require.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "%v", err)
	})
}

// A connection is served from its stored conninfo, and only the conninfo.
func TestTableGroupBindingIsServedFromTheStoredConnInfo(t *testing.T) {
	read := func(t *testing.T, row *sqltypes.Result) (*multipoolerservicepb.BackingConnection, error) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern(bindingQuery, row)
		conn, found, err := newConnectionStore(pm).TableGroupBinding(t.Context(), 7)
		if err == nil {
			require.True(t, found)
		}
		return conn, err
	}
	bound := func(conninfo string) *sqltypes.Result {
		return bindingRowWithConnInfo(tableGroupStateActive, "src", conninfo)
	}

	t.Run("a conninfo", func(t *testing.T) {
		conn, err := read(t, bound(
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
		_, err := read(t, bound(`{"host":["h"],"port":[5432],"dbname":"app","user":"u","sslmode":"prefer"}`))
		require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
	})
	t.Run("a connection a pooler cannot front is refused by the rules of the pooler", func(t *testing.T) {
		_, err := read(t, bound(`{"host":["h"],"port":[5432],"dbname":"app","user":"u","password":"p","sslmode":"prefer","params":{"application_name":"x"}}`))
		require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
		require.ErrorContains(t, err, `connection "src"`)
	})
	t.Run("a conninfo this version cannot read is refused, never read as less", func(t *testing.T) {
		_, err := read(t, bound(`{"host":["h"],"password":"s3cr3t","future":true}`))
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
