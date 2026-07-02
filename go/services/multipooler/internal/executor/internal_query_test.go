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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/fakepgserver"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// newInternalQueryTestExecutor wires a real connpoolmanager.Manager to the given
// fake server and returns an Executor backed by it. The manager is closed via
// t.Cleanup.
func newInternalQueryTestExecutor(t *testing.T, server *fakepgserver.Server) *Executor {
	t.Helper()

	reg := viperutil.NewRegistry()
	config := connpoolmanager.NewConfig(reg)
	// fakepgserver uses trust auth, so the password value is never consulted —
	// resolving just satisfies the "Resolve ran before Open" invariant.
	t.Setenv(constants.PgPasswordEnvVar, "test-password")
	require.NoError(t, config.ResolvePgPassword())

	manager := config.NewManager(slog.Default())
	manager.Open(context.Background(), &connpoolmanager.ConnectionConfig{
		SocketFile: server.ClientConfig().SocketFile,
		Host:       server.ClientConfig().Host,
		Port:       server.ClientConfig().Port,
		Database:   server.ClientConfig().Database,
	})
	// Close the manager before the server: the server's listener blocks on
	// Close until every client connection has gone away, so the manager's
	// pooled connections must be torn down first to avoid a deadlock.
	t.Cleanup(func() {
		manager.Close()
		server.Close()
	})

	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	return NewExecutor(slog.Default(), manager, poolerID, false)
}

func TestInternalTx_CommitFlow(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	tx, err := e.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Query(ctx, "INSERT INTO t VALUES (1)")
	require.NoError(t, err)

	require.NoError(t, tx.Commit(ctx))

	// BEGIN, the statement, and COMMIT all ran.
	assert.Equal(t, 1, server.GetQueryCalledNum("BEGIN"))
	assert.Equal(t, 1, server.GetQueryCalledNum("INSERT INTO t VALUES (1)"))
	assert.Equal(t, 1, server.GetQueryCalledNum("COMMIT"))
	assert.Equal(t, 0, server.GetQueryCalledNum("ROLLBACK"))
}

func TestInternalTx_RollbackFlow(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	tx, err := e.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Query(ctx, "INSERT INTO t VALUES (1)")
	require.NoError(t, err)

	require.NoError(t, tx.Rollback(ctx))

	assert.Equal(t, 1, server.GetQueryCalledNum("BEGIN"))
	assert.Equal(t, 1, server.GetQueryCalledNum("ROLLBACK"))
	assert.Equal(t, 0, server.GetQueryCalledNum("COMMIT"))
}

func TestInternalTx_DeferredRollbackAfterCommitIsNoop(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	tx, err := e.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Simulates the `defer tx.Rollback(ctx)` idiom firing after a successful
	// Commit: it must be a no-op and not issue a ROLLBACK.
	require.NoError(t, tx.Rollback(ctx))
	assert.Equal(t, 0, server.GetQueryCalledNum("ROLLBACK"))
}

func TestInternalTx_QueryAfterFinishedFails(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	tx, err := e.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	_, err = tx.Query(ctx, "SELECT 1")
	require.ErrorIs(t, err, errTxFinished)

	_, err = tx.QueryArgs(ctx, "SELECT $1", 1)
	require.ErrorIs(t, err, errTxFinished)

	require.ErrorIs(t, tx.Commit(ctx), errTxFinished)
}

func TestInternalTx_QueryArgsWithinTransaction(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	tx, err := e.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	// QueryArgs uses the extended query protocol; it must run on the same pinned
	// connection inside the transaction.
	res, err := tx.QueryArgs(ctx, "SELECT $1::int", 42)
	require.NoError(t, err)
	require.NotNil(t, res)

	require.NoError(t, tx.Commit(ctx))
}

func TestInternalTx_SequentialTransactionsReleaseReservedConn(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	// Two transactions in sequence: the second can only Begin if the first's
	// reserved connection was released back to the pool, so this guards against
	// leaking the reservation on Commit.
	for range 2 {
		tx, err := e.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
	}

	assert.Equal(t, 2, server.GetQueryCalledNum("BEGIN"))
	assert.Equal(t, 2, server.GetQueryCalledNum("COMMIT"))
}

func TestInternalTx_BeginErrorReleasesConnection(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)
	server.RejectQueryPattern("BEGIN", "cannot begin")

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	tx, err := e.Begin(ctx)
	require.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "failed to begin transaction")
}
