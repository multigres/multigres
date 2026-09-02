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

package executor

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

// newInvalidationTestExecutor spins up an executor backed by a fake
// PostgreSQL server, with a single live backend connection for tests to call
// ensurePrepared against directly.
func newInvalidationTestExecutor(t *testing.T) (*Executor, *regular.Conn) {
	t.Helper()

	server := fakepgserver.New(t)
	t.Cleanup(server.Close)
	server.SetNeverFail(true)

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	t.Cleanup(pool.Close)

	rconn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	e := NewExecutor(slog.Default(), nil, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)
	return e, rconn.Conn()
}

// TestEnsurePrepared_ReusesUnaffectedStatement verifies the common case is
// unchanged: re-ensuring the same canonical statement with no intervening DDL
// keeps the same stamped generation (no re-Parse forced).
func TestEnsurePrepared_ReusesUnaffectedStatement(t *testing.T) {
	e, conn := newInvalidationTestExecutor(t)
	ctx := context.Background()
	stmt := &query.PreparedStatement{Query: "SELECT * FROM orders WHERE id = $1", UsedTables: []string{"orders"}}

	name1, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)
	_, gen1, ok := conn.State().GetPreparedStatement(name1)
	require.True(t, ok)

	name2, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)
	_, gen2, ok := conn.State().GetPreparedStatement(name2)
	require.True(t, ok)

	require.Equal(t, name1, name2)
	require.Equal(t, gen1, gen2, "no DDL happened; the cached entry must not have been re-Parsed")
}

// TestEnsurePrepared_ReParsesAfterDDLOnDependentRelation verifies that once a
// DDL statement invalidates a relation a cached statement depends on, the
// next ensurePrepared call re-Parses instead of trusting the stale entry —
// this is what makes Describe safe (see ensurePrepared's doc comment) since
// PostgreSQL itself doesn't revalidate on Describe.
func TestEnsurePrepared_ReParsesAfterDDLOnDependentRelation(t *testing.T) {
	e, conn := newInvalidationTestExecutor(t)
	ctx := context.Background()
	stmt := &query.PreparedStatement{Query: "SELECT * FROM orders WHERE id = $1", UsedTables: []string{"orders"}}

	name, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)
	_, genBefore, ok := conn.State().GetPreparedStatement(name)
	require.True(t, ok)

	// Simulate ALTER TABLE orders ... executing successfully.
	e.relationInvalidation.InvalidateRelations([]string{"orders"})

	name2, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)
	_, genAfter, ok := conn.State().GetPreparedStatement(name2)
	require.True(t, ok)

	require.Equal(t, name, name2, "canonical name is unaffected by DDL, only its freshness")
	require.Greater(t, genAfter, genBefore, "must have re-Parsed against the post-DDL schema")
}

// TestEnsurePrepared_UnaffectedByDDLOnUnrelatedRelation verifies invalidation
// is scoped to the relations a statement actually depends on: a DDL on some
// other table must not force a needless re-Parse.
func TestEnsurePrepared_UnaffectedByDDLOnUnrelatedRelation(t *testing.T) {
	e, conn := newInvalidationTestExecutor(t)
	ctx := context.Background()
	stmt := &query.PreparedStatement{Query: "SELECT * FROM orders WHERE id = $1", UsedTables: []string{"orders"}}

	name, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)
	_, genBefore, ok := conn.State().GetPreparedStatement(name)
	require.True(t, ok)

	// A DDL on a table this statement doesn't reference.
	e.relationInvalidation.InvalidateRelations([]string{"customers"})

	name2, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)
	_, genAfter, ok := conn.State().GetPreparedStatement(name2)
	require.True(t, ok)

	require.Equal(t, genBefore, genAfter, "DDL on an unrelated table must not invalidate this statement")
}
