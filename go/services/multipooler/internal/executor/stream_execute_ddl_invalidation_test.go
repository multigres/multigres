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
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
)

// newRegularPoolTestExecutor returns an executor whose regular-connection
// path is backed by a real (fake) PostgreSQL connection, for exercising
// StreamExecute's Case 3.
func newRegularPoolTestExecutor(t *testing.T, server *fakepgserver.Server) *Executor {
	t.Helper()

	clientConn, err := client.Connect(context.Background(), context.Background(), server.ClientConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.Close() })

	regularConn := regular.NewConn(clientConn, nil)
	pooled := &connpool.Pooled[*regular.Conn]{Conn: regularConn}

	return NewExecutor(slog.Default(), &stubPoolManager{regularConn: pooled},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)
}

// TestStreamExecute_DDLSuccessInvalidatesTargetRelations verifies that a
// successful StreamExecute bumps the invalidation generation for the
// relations named in ExecuteOptions.DdlTargetRelations, regardless of which
// internal case (reserved/regular) actually ran the statement.
func TestStreamExecute_DDLSuccessInvalidatesTargetRelations(t *testing.T) {
	server := fakepgserver.New(t)
	t.Cleanup(server.Close)
	server.SetNeverFail(true)

	e := newRegularPoolTestExecutor(t, server)

	_, err := e.StreamExecute(context.Background(), &query.Target{},
		"ALTER TABLE orders ALTER COLUMN amount TYPE numeric",
		&query.ExecuteOptions{DdlTargetRelations: []string{"orders"}}, nil, noopCallback)
	require.NoError(t, err)

	// A statement depending on "orders", registered after the DDL ran, must
	// observe a nonzero generation — the successful StreamExecute bumped it.
	e.relationInvalidation.RecordDependencies("ppstmt-test", []string{"orders"})
	require.NotZero(t, e.relationInvalidation.StatementGeneration("ppstmt-test"))
}

// TestStreamExecute_FailedDDLDoesNotInvalidate verifies a failed StreamExecute
// does not bump any relation's generation — the DDL never actually took
// effect on the schema, so there is nothing to invalidate against.
func TestStreamExecute_FailedDDLDoesNotInvalidate(t *testing.T) {
	server := fakepgserver.New(t)
	t.Cleanup(server.Close)
	server.AddRejectedQuery("ALTER TABLE orders ALTER COLUMN amount TYPE numeric", errors.New("boom"))

	e := newRegularPoolTestExecutor(t, server)

	_, err := e.StreamExecute(context.Background(), &query.Target{},
		"ALTER TABLE orders ALTER COLUMN amount TYPE numeric",
		&query.ExecuteOptions{DdlTargetRelations: []string{"orders"}}, nil, noopCallback)
	require.Error(t, err)

	e.relationInvalidation.RecordDependencies("ppstmt-test", []string{"orders"})
	require.Zero(t, e.relationInvalidation.StatementGeneration("ppstmt-test"))
}
