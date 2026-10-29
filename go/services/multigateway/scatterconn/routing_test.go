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

package scatterconn

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// fakeRouter rewrites the default tablegroup of the databases in routed to
// the srctg tablegroup, as an import's rule does.
type fakeRouter struct {
	routed map[string]bool
	err    error
}

func (r *fakeRouter) Resolve(_ context.Context, t *querypb.Target) (*querypb.Target, error) {
	if t.GetControl() {
		return t, nil
	}
	if r.err != nil {
		return nil, r.err
	}
	ref := t.GetShardRef()
	if !r.routed[ref.GetDatabase()] || ref.GetTableGroup() != constants.DefaultTableGroup {
		return t, nil
	}
	return &querypb.Target{
		ShardRef: &clustermetadatapb.ShardRef{Database: ref.GetDatabase(), TableGroup: "srctg", Shard: ref.GetShard()},
		Mode:     t.GetMode(),
	}, nil
}

// targetRecordingGateway is mockGateway recording the target of every call,
// including those made through QueryServiceByID.
type targetRecordingGateway struct {
	*mockGateway
	targets []string
}

func (g *targetRecordingGateway) record(t *querypb.Target) {
	ref := t.GetShardRef()
	g.targets = append(g.targets, ref.GetDatabase()+"/"+ref.GetTableGroup()+"/"+ref.GetShard())
}

// last returns the target of the last call.
func (g *targetRecordingGateway) last() string {
	if len(g.targets) == 0 {
		return ""
	}
	return g.targets[len(g.targets)-1]
}

func (g *targetRecordingGateway) QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, t *querypb.Target) (queryservice.QueryService, error) {
	if _, err := g.mockGateway.QueryServiceByID(ctx, id, t); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *targetRecordingGateway) StreamExecute(ctx context.Context, t *querypb.Target, sql string, opts *querypb.ExecuteOptions, ro *querypb.ReservationOptions, cb func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.StreamExecute(ctx, t, sql, opts, ro, cb)
}

func (g *targetRecordingGateway) PortalStreamExecute(ctx context.Context, t *querypb.Target, ps *querypb.PreparedStatement, p *querypb.Portal, opts *querypb.ExecuteOptions, po *multipoolerpb.PortalExecuteOptions, ro *querypb.ReservationOptions, cb func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.PortalStreamExecute(ctx, t, ps, p, opts, po, ro, cb)
}

func (g *targetRecordingGateway) Describe(ctx context.Context, t *querypb.Target, ps *querypb.PreparedStatement, p *querypb.Portal, opts *querypb.ExecuteOptions) (*querypb.StatementDescription, error) {
	g.record(t)
	return g.mockGateway.Describe(ctx, t, ps, p, opts)
}

func (g *targetRecordingGateway) CopyReady(ctx context.Context, t *querypb.Target, q string, opts *querypb.ExecuteOptions, ro *querypb.ReservationOptions) (int16, []int16, *querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.CopyReady(ctx, t, q, opts, ro)
}

func (g *targetRecordingGateway) CopySendData(ctx context.Context, t *querypb.Target, data []byte, opts *querypb.ExecuteOptions) error {
	g.record(t)
	return g.mockGateway.CopySendData(ctx, t, data, opts)
}

func (g *targetRecordingGateway) CopyFinalize(ctx context.Context, t *querypb.Target, data []byte, opts *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.CopyFinalize(ctx, t, data, opts)
}

func (g *targetRecordingGateway) CopyAbort(ctx context.Context, t *querypb.Target, msg string, opts *querypb.ExecuteOptions) (*querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.CopyAbort(ctx, t, msg, opts)
}

func (g *targetRecordingGateway) CopyOutReady(ctx context.Context, t *querypb.Target, q string, opts *querypb.ExecuteOptions, ro *querypb.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.CopyOutReady(ctx, t, q, opts, ro)
}

func (g *targetRecordingGateway) CopyOutStream(ctx context.Context, t *querypb.Target, opts *querypb.ExecuteOptions, fn func(pgClient.CopyOutMessage) error) (*sqltypes.Result, *querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.CopyOutStream(ctx, t, opts, fn)
}

func (g *targetRecordingGateway) StreamReplication(ctx context.Context, init *multipoolerpb.StreamReplicationInit) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	g.record(init.GetTarget())
	return g.mockGateway.StreamReplication(ctx, init)
}

func (g *targetRecordingGateway) ConcludeTransaction(ctx context.Context, t *querypb.Target, opts *querypb.ExecuteOptions, c multipoolerpb.TransactionConclusion, names []string, all, chain bool, rollback map[string]string) (*sqltypes.Result, *querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.ConcludeTransaction(ctx, t, opts, c, names, all, chain, rollback)
}

func (g *targetRecordingGateway) DiscardTempTables(ctx context.Context, t *querypb.Target, opts *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.DiscardTempTables(ctx, t, opts)
}

func (g *targetRecordingGateway) ReleaseReservedConnection(ctx context.Context, t *querypb.Target, opts *querypb.ExecuteOptions, keepSticky bool) (*querypb.ReservedState, error) {
	g.record(t)
	return g.mockGateway.ReleaseReservedConnection(ctx, t, opts, keepSticky)
}

// reserved is a reserved state as a pooler returns it for a new reservation.
func reserved(id uint64, reasons uint32) *querypb.ReservedState {
	return &querypb.ReservedState{
		ReservedConnectionId: id,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "source-pooler"},
		ReservationReasons:   reasons,
	}
}

// newRoutedScatterConn returns a ScatterConn whose router sends db1's default
// tablegroup to srctg and leaves every other database alone.
func newRoutedScatterConn() (*ScatterConn, *targetRecordingGateway, *fakeRouter) {
	gw := &targetRecordingGateway{mockGateway: &mockGateway{
		callbackResult:          &sqltypes.Result{CommandTag: "SELECT 1"},
		copyReadyReturnState:    reserved(11, protoutil.ReasonCopy),
		copyOutReadyReturnState: reserved(12, protoutil.ReasonCopy),
		copyFinalizeResult:      &sqltypes.Result{CommandTag: "COPY 1"},
	}}
	router := &fakeRouter{routed: map[string]bool{"db1": true}}
	sc := NewScatterConn(gw, slog.Default())
	sc.SetRouter(router)
	return sc, gw, router
}

func connOf(database string) *server.Conn {
	return server.NewTestConn(&bytes.Buffer{}, server.WithTestDatabase(database)).Conn
}

const (
	defTG    = constants.DefaultTableGroup
	defShard = constants.DefaultShard
)

func noopResult(context.Context, *sqltypes.Result) error { return nil }

// Every ScatterConn path that sends application work to a pooler applies the
// routing rules: a routed database's statements land on srctg, and another
// database's on its own default tablegroup.
func TestRouting_EveryPathFollowsTheDatabasesRules(t *testing.T) {
	ctx := context.Background()
	paths := []struct {
		name string
		run  func(t *testing.T, sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState)
	}{
		{"StreamExecute", func(t *testing.T, sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) {
			require.NoError(t, sc.StreamExecute(ctx, conn, defTG, defShard, "SELECT 1", nil, state, engine.PlanExecInfo{}, false, noopResult))
		}},
		{"PortalStreamExecute", func(t *testing.T, sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) {
			require.NoError(t, sc.PortalStreamExecute(ctx, defTG, defShard, conn, state, testPortalInfo(), 0, false, engine.PlanExecInfo{}, false, noopResult))
		}},
		{"Describe", func(t *testing.T, sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) {
			_, err := sc.Describe(ctx, defTG, defShard, conn, state, testPortalInfo(), nil)
			require.NoError(t, err)
		}},
		{"StreamReplication", func(t *testing.T, sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) {
			_, err := sc.StreamReplication(ctx, conn, defTG, defShard, state, &multipoolerpb.StreamReplicationInit{})
			require.NoError(t, err)
		}},
		{"CopyInitiate, CopySendData, CopyFinalize", func(t *testing.T, sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) {
			_, _, err := sc.CopyInitiate(ctx, conn, defTG, defShard, "COPY t FROM STDIN", state, noopResult)
			require.NoError(t, err)
			require.NoError(t, sc.CopySendData(ctx, conn, defTG, defShard, state, []byte("1\n")))
			require.NoError(t, sc.CopyFinalize(ctx, conn, defTG, defShard, state, nil, noopResult))
		}},
		{"CopyInitiate, CopyAbort", func(t *testing.T, sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) {
			_, _, err := sc.CopyInitiate(ctx, conn, defTG, defShard, "COPY t FROM STDIN", state, noopResult)
			require.NoError(t, err)
			require.NoError(t, sc.CopyAbort(ctx, conn, defTG, defShard, state))
		}},
		{"CopyOutInitiate, CopyOutStream", func(t *testing.T, sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) {
			_, _, _, err := sc.CopyOutInitiate(ctx, conn, defTG, defShard, "COPY t TO STDOUT", state)
			require.NoError(t, err)
			_, err = sc.CopyOutStream(ctx, conn, defTG, defShard, state, func(pgClient.CopyOutMessage) error { return nil })
			require.NoError(t, err)
		}},
	}
	for _, p := range paths {
		t.Run(p.name, func(t *testing.T) {
			for _, tc := range []struct{ database, want string }{
				{"db1", "db1/srctg/0-inf"},
				{"db2", "db2/default/0-inf"},
			} {
				sc, gw, _ := newRoutedScatterConn()
				p.run(t, sc, connOf(tc.database), handler.NewMultigatewayConnectionState())
				require.NotEmpty(t, gw.targets, "the path must reach the gateway")
				for _, got := range gw.targets {
					assert.Equal(t, tc.want, got, "every call of %s for %s", p.name, tc.database)
				}
			}
		})
	}
}

// While a database's routing is unknown its statements fail with the
// router's retryable error and never reach a pooler.
func TestRouting_UnknownRoutingFailsClosed(t *testing.T) {
	sc, gw, router := newRoutedScatterConn()
	router.err = mterrors.WithCode(mterrors.NewPgError("ERROR", mterrors.PgSSCannotConnectNow, "the database's routing is not available yet; please retry", ""), mtrpcpb.Code_UNAVAILABLE)

	err := sc.StreamExecute(context.Background(), connOf("db1"), defTG, defShard, "SELECT 1", nil,
		handler.NewMultigatewayConnectionState(), engine.PlanExecInfo{}, false, noopResult)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSCannotConnectNow), "error: %v", err)
	assert.Empty(t, gw.targets)
}

// A reservation records both where it lives and the target its statements
// were planned for, and the planner's pinning question is answered for the
// planned target.
func TestRouting_ReservationRemembersItsPlannedTarget(t *testing.T) {
	sc, _, _ := newRoutedScatterConn()
	state := handler.NewMultigatewayConnectionState()
	_, _, err := sc.CopyInitiate(context.Background(), connOf("db1"), defTG, defShard, "COPY t FROM STDIN", state, noopResult)
	require.NoError(t, err)

	require.Len(t, state.ShardStates, 1)
	ss := state.ShardStates[0]
	assert.Equal(t, "srctg", ss.Target.GetShardRef().GetTableGroup())
	assert.Equal(t, defTG, ss.Planned.GetShardRef().GetTableGroup())
	assert.True(t, state.HasReservedConnectionFor(defTG, defShard), "the session is pinned for the tablegroup the planner routes to")
	assert.False(t, state.HasReservedConnectionFor("srctg", defShard))
}

// A session holding a reservation keeps using it after the rule that placed
// it is deleted, until it is released; statements after that follow the
// rules again. (Until fences exist, rules change only while the application
// is quiesced, so this does not happen in practice.)
func TestRouting_ReservationOutlivesARuleChange(t *testing.T) {
	sc, gw, router := newRoutedScatterConn()
	ctx := context.Background()
	conn := connOf("db1")
	state := handler.NewMultigatewayConnectionState()

	// A temp table reserves a backend on srctg.
	gw.streamExecuteReturnState = reserved(21, protoutil.ReasonTempTable)
	require.NoError(t, sc.StreamExecute(ctx, conn, defTG, defShard, "CREATE TEMP TABLE t (a int)", nil, state,
		engine.PlanExecInfo{TempTable: true}, false, noopResult))
	require.Equal(t, "db1/srctg/0-inf", gw.last())

	// The rule is deleted. The reserved session stays on srctg.
	router.routed = nil
	gw.queryServiceByIDCalled = false
	require.NoError(t, sc.StreamExecute(ctx, conn, defTG, defShard, "SELECT * FROM t", nil, state, engine.PlanExecInfo{}, false, noopResult))
	assert.Equal(t, "db1/srctg/0-inf", gw.last())
	assert.True(t, gw.queryServiceByIDCalled, "the statement runs on the reserved backend")

	// Once the reservation is released, the session follows the rules.
	state.ClearReservedConnection(state.ShardStates[0].Target)
	gw.streamExecuteReturnState = nil
	require.NoError(t, sc.StreamExecute(ctx, conn, defTG, defShard, "SELECT 1", nil, state, engine.PlanExecInfo{}, false, noopResult))
	assert.Equal(t, "db1/default/0-inf", gw.last())

	// A session that held no reservation follows the rules at once.
	router.routed = map[string]bool{"db1": true}
	require.NoError(t, sc.StreamExecute(ctx, conn, defTG, defShard, "SELECT 1", nil, handler.NewMultigatewayConnectionState(), engine.PlanExecInfo{}, false, noopResult))
	assert.Equal(t, "db1/srctg/0-inf", gw.last())
}

// The paths that act on the session's reservations rather than on a planned
// target (COMMIT/ROLLBACK, DISCARD TEMP, releasing reservations) reach the
// backend the reservation lives on.
func TestRouting_ReservationPathsUseTheReservedTarget(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		run  func(sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) error
	}{
		{"ConcludeTransaction", func(sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) error {
			return sc.ConcludeTransaction(ctx, conn, state, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, nil, false, false, noopResult)
		}},
		{"DiscardTempTables", func(sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) error {
			return sc.DiscardTempTables(ctx, conn, state, noopResult)
		}},
		{"ReleaseAllReservedConnections", func(sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) error {
			return sc.ReleaseAllReservedConnections(ctx, conn, state, false)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, gw, _ := newRoutedScatterConn()
			conn := connOf("db1")
			state := handler.NewMultigatewayConnectionState()
			gw.streamExecuteReturnState = reserved(31, protoutil.ReasonTempTable|protoutil.ReasonTransaction)
			require.NoError(t, sc.StreamExecute(ctx, conn, defTG, defShard, "CREATE TEMP TABLE t (a int)", nil, state,
				engine.PlanExecInfo{TempTable: true}, false, noopResult))
			gw.targets = nil

			require.NoError(t, tc.run(sc, conn, state))
			require.NotEmpty(t, gw.targets)
			for _, got := range gw.targets {
				assert.Equal(t, "db1/srctg/0-inf", got)
			}
		})
	}
}
