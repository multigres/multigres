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
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// collect gathers the single result a local answer produces.
func collect(results *[]*sqltypes.Result) func(context.Context, *sqltypes.Result) error {
	return func(_ context.Context, r *sqltypes.Result) error {
		*results = append(*results, r)
		return nil
	}
}

func TestGatewayEval_SimpleProtocolNeverReachesBackend(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	for range 2 {
		var results []*sqltypes.Result
		res, err := exec.StreamExecute(ctx, conn, nil, "SELECT 1 + 2 AS three", parseOne(t, "SELECT 1 + 2 AS three"), collect(&results))
		require.NoError(t, err)
		assert.Equal(t, engine.PlanTypeGatewayEval, res.PlanType)
		assert.False(t, res.CacheHit, "a constant SELECT is never cached")
		assert.Equal(t, "SELECT $1 + $2 AS three", res.NormalizedSQL, "the shape is still fingerprinted for logs and metrics")
		assert.NotEmpty(t, res.Fingerprint)
		require.Len(t, results, 1)
		require.Len(t, results[0].Rows, 1)
		assert.Equal(t, "3", string(results[0].Rows[0].Values[0]))
		assert.Equal(t, "three", results[0].Fields[0].Name)
		time.Sleep(20 * time.Millisecond) // theine processes writes asynchronously
	}
	assert.Zero(t, mock.streamExecuteCalls.Load(), "the backend must never see the query")
}

// A literal-dependent decline must not poison the normalized cache: the
// Route for `SELECT 1 + true` would otherwise be served to `SELECT 1 + 2`.
func TestGatewayEval_DeclinedSelectIsNotCached(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	res, err := exec.StreamExecute(ctx, conn, nil, "SELECT 1 + true", parseOne(t, "SELECT 1 + true"), noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeRoute, res.PlanType)
	assert.Equal(t, int32(1), mock.streamExecuteCalls.Load())
	assert.Equal(t, "SELECT 1 + true", mock.lastStreamExecuteSQL.Load(), "the literal SQL is sent, not a reconstruction")
	time.Sleep(20 * time.Millisecond)

	res, err = exec.StreamExecute(ctx, conn, nil, "SELECT 1 + 2", parseOne(t, "SELECT 1 + 2"), noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeGatewayEval, res.PlanType)
	assert.False(t, res.CacheHit)
	assert.Equal(t, int32(1), mock.streamExecuteCalls.Load())
}

// Inside a transaction block the planner always routes, so the statement
// stays on the cached path there.
func TestGatewayEval_InTransactionRoutesAndCaches(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	res, err := exec.StreamExecute(ctx, conn, nil, "SELECT 1", parseOne(t, "SELECT 1"), noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeRoute, res.PlanType)
	assert.False(t, res.CacheHit)
	time.Sleep(50 * time.Millisecond)

	res, err = exec.StreamExecute(ctx, conn, nil, "SELECT 2", parseOne(t, "SELECT 2"), noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeRoute, res.PlanType)
	assert.True(t, res.CacheHit)
	assert.Equal(t, int32(2), mock.streamExecuteCalls.Load())

	// Leaving the block, the same shape is answered locally despite the entry.
	conn.SetTxnStatus(protocol.TxnStatusIdle)
	res, err = exec.StreamExecute(ctx, conn, nil, "SELECT 3", parseOne(t, "SELECT 3"), noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeGatewayEval, res.PlanType)
	assert.Equal(t, int32(2), mock.streamExecuteCalls.Load())
}

func TestGatewayEval_EvaluationErrorIsReturned(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()

	_, err := exec.StreamExecute(context.Background(), testConn(), nil, "SELECT 1 / 0", parseOne(t, "SELECT 1 / 0"), noopCallback)
	require.Error(t, err)
	assert.Equal(t, mterrors.PgSSDivisionByZero, mterrors.ExtractSQLSTATE(err))
	assert.False(t, mterrors.IsGatewayRejection(err), "a PostgreSQL error, not a gateway policy rejection")
	assert.Zero(t, mock.streamExecuteCalls.Load())
}

func boundPortal(t *testing.T, sql string, paramTypes []uint32, params [][]byte, resultFormats []int16) *preparedstatement.PortalInfo {
	t.Helper()
	psi, err := preparedstatement.NewPreparedStatementInfo(protoutil.NewPreparedStatement("s", sql, paramTypes))
	require.NoError(t, err)
	return preparedstatement.NewPortalInfo(psi, protoutil.NewPortal("", "s", params, nil, resultFormats))
}

func TestGatewayEval_PortalNeverReachesBackend(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	pi := boundPortal(t, "SELECT $1 + $2 AS sum", []uint32{uint32(pgoid.INT4OID), 0}, [][]byte{[]byte("3"), []byte("4")}, nil)
	var results []*sqltypes.Result
	res, err := exec.PortalStreamExecute(ctx, conn, nil, pi, 0, false, collect(&results))
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeGatewayEval, res.PlanType)
	assert.False(t, res.CacheHit)
	assert.NotEmpty(t, res.Fingerprint)
	require.Len(t, results, 1)
	assert.Nil(t, results[0].Fields)
	assert.Equal(t, "7", string(results[0].Rows[0].Values[0]))
	assert.Zero(t, mock.portalStreamExecuteCalls.Load())
	assert.Zero(t, mock.streamExecuteCalls.Load())
}

func TestGatewayEval_Describe(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	t.Run("statement", func(t *testing.T) {
		psi, err := preparedstatement.NewPreparedStatementInfo(protoutil.NewPreparedStatement("s", "SELECT $1::int4 + $2", []uint32{0, 0}))
		require.NoError(t, err)
		desc, err := exec.Describe(ctx, conn, nil, nil, psi)
		require.NoError(t, err)
		require.NotNil(t, desc, "answered locally, not by the (nil-returning) mock")
		require.Len(t, desc.Parameters, 2)
		assert.Equal(t, uint32(pgoid.INT4OID), desc.Parameters[1].DataTypeOid, "deduced")
		require.Len(t, desc.Fields, 1)
		assert.Equal(t, "?column?", desc.Fields[0].Name)
		assert.Equal(t, uint32(pgoid.INT4OID), desc.Fields[0].DataTypeOid)
		assert.Equal(t, int32(0), desc.Fields[0].Format)
	})

	t.Run("portal reports its result format", func(t *testing.T) {
		pi := boundPortal(t, "SELECT $1::int4 + $2", []uint32{0, 0}, [][]byte{[]byte("3"), []byte("4")}, []int16{1})
		desc, err := exec.Describe(ctx, conn, nil, pi, nil)
		require.NoError(t, err)
		require.NotNil(t, desc)
		assert.Equal(t, int32(1), desc.Fields[0].Format)
	})

	t.Run("routed statement is forwarded", func(t *testing.T) {
		psi, err := preparedstatement.NewPreparedStatementInfo(protoutil.NewPreparedStatement("s", "SELECT now()", nil))
		require.NoError(t, err)
		desc, err := exec.Describe(ctx, conn, nil, nil, psi)
		require.NoError(t, err)
		assert.Nil(t, desc, "the mock backend answered")
	})

	t.Run("inside a transaction is forwarded", func(t *testing.T) {
		inTxn := testConn()
		inTxn.SetTxnStatus(protocol.TxnStatusInBlock)
		psi, err := preparedstatement.NewPreparedStatementInfo(protoutil.NewPreparedStatement("s", "SELECT 1", nil))
		require.NoError(t, err)
		desc, err := exec.Describe(ctx, inTxn, nil, nil, psi)
		require.NoError(t, err)
		assert.Nil(t, desc)
	})
}

// A session holding a reserved backend (advisory lock, temp table) routes
// even a constant SELECT, so the next statement after the backend is lost
// ends the session instead of succeeding locally; the plan is cached as any
// routed statement. Once the reservation is released the local path resumes.
func TestGatewayEval_ReservedBackendRoutes(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()
	state := handler.NewMultigatewayConnectionState()
	conn.SetConnectionState(state)
	state.SetReservedConnection(protoutil.NewTarget("", "default", constants.DefaultShard, query.Mode_MODE_UNSPECIFIED),
		&query.ReservedState{ReservedConnectionId: 7})

	res, err := exec.StreamExecute(ctx, conn, state, "SELECT 1", parseOne(t, "SELECT 1"), noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeRoute, res.PlanType)
	assert.Equal(t, int32(1), mock.streamExecuteCalls.Load())
	time.Sleep(50 * time.Millisecond)

	res, err = exec.StreamExecute(ctx, conn, state, "SELECT 2", parseOne(t, "SELECT 2"), noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeRoute, res.PlanType)
	assert.True(t, res.CacheHit)

	pi := boundPortal(t, "SELECT 1", nil, nil, nil)
	res, err = exec.PortalStreamExecute(ctx, conn, state, pi, 0, false, noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeRoute, res.PlanType)
	assert.Equal(t, int32(1), mock.portalStreamExecuteCalls.Load())

	desc, err := exec.Describe(ctx, conn, state, nil, pi.PreparedStatementInfo)
	require.NoError(t, err)
	assert.Nil(t, desc, "forwarded to the (nil-returning) mock backend")

	state.ClearAllReservedConnections()
	res, err = exec.StreamExecute(ctx, conn, state, "SELECT 3", parseOne(t, "SELECT 3"), noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeGatewayEval, res.PlanType)
	assert.Equal(t, int32(2), mock.streamExecuteCalls.Load())
}

// The portal's declared parameter types reach the planner on the
// non-cacheable path too (an unsafe connection), even though it routes.
func TestGatewayEval_UnsafeConnectionRoutes(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()

	pi := boundPortal(t, "SELECT 1", nil, nil, nil)
	conn := server.NewTestConn(&bytes.Buffer{}, server.WithTestUnsafeConnection()).Conn
	res, err := exec.PortalStreamExecute(context.Background(), conn, nil, pi, 0, false, noopCallback)
	require.NoError(t, err)
	assert.Equal(t, engine.PlanTypeRoute, res.PlanType)
	assert.Equal(t, int32(1), mock.portalStreamExecuteCalls.Load())
}
