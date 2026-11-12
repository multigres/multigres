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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// connectionRow is the row of multigres.connections that the migrator's store reads.
func connectionRow(name, dsn string) *sqltypes.Result {
	return mock.MakeQueryResult(
		[]string{"connection_id", "name", "dsn", "owner_role", "created_at"},
		[][]any{{int64(1), name, dsn, "", "2026-01-01T00:00:00Z"}})
}

// TestBackingConnectionIsReadFromTheCatalog: the connection is a row of multigres.connections,
// read with one query, and the dsn column decodes into the structured connection.
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
	t.Run("a keyword=value dsn the migrator wrote is servable as it is", func(t *testing.T) {
		const dsn = "host=db.example.com port=5432 dbname=app user=repl password='p w'"
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections", connectionRow("src", dsn))
		conn, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []string{"db.example.com"}, conn.GetHosts())
		require.Equal(t, "repl", conn.GetUser())
		require.Equal(t, "p w", conn.GetPassword())
	})
	t.Run("a dsn that does not name one server is refused with its code, naming the connection", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.connections", connectionRow("src", "host=h1,h2 port=5432 dbname=app user=u password=p"))
		_, _, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
		require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
		require.ErrorContains(t, err, `connection "src"`)
	})
}

// TestTheCatalogIsTheDefaultDirectory: a manager with no directory of its own reads
// connections from the sharding catalog.
func TestTheCatalogIsTheDefaultDirectory(t *testing.T) {
	pm, _ := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
	require.IsType(t, &connectionStore{}, pm.connections())
}
