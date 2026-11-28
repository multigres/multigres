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
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// Catalog ids of the fence-test snapshot: default (1) with 0-inf (1), and
// commerce (2) with 0-80 (2) and 80-inf (3).
const (
	fenceTestCommerceID = int64(2)
	fenceTestShard0080  = int64(2)
	fenceTestShard80Inf = int64(3)
)

// fenceTestSnapshot is a valid snapshot at version with the given fences.
func fenceTestSnapshot(version int64, fences ...*shardingcatalogpb.Fence) *multipoolermanagerdatapb.GetRoutingSnapshotResponse {
	active := shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE
	return &multipoolermanagerdatapb.GetRoutingSnapshotResponse{
		RoutingVersion: version,
		Snapshot: &shardingcatalogpb.Snapshot{
			RoutingVersion: version,
			Tablegroups: []*shardingcatalogpb.TableGroup{
				{TablegroupId: 1, Name: constants.DefaultTableGroup, State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE},
				{TablegroupId: fenceTestCommerceID, Name: "commerce", State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE},
			},
			Shards: []*shardingcatalogpb.Shard{
				{ShardId: 1, TablegroupId: 1, Name: constants.DefaultShard, KeyRange: &clustermetadatapb.KeyRange{}, State: active},
				{ShardId: fenceTestShard0080, TablegroupId: fenceTestCommerceID, Name: "0-80", KeyRange: &clustermetadatapb.KeyRange{End: []byte{0x80}}, State: active},
				{ShardId: fenceTestShard80Inf, TablegroupId: fenceTestCommerceID, Name: "80-inf", KeyRange: &clustermetadatapb.KeyRange{Start: []byte{0x80}}, State: active},
			},
			Fences: fences,
		},
	}
}

// servedUnder sets the consensus rule under which the catalog primary serves
// resp.
func servedUnder(term int64, resp *multipoolermanagerdatapb.GetRoutingSnapshotResponse) *multipoolermanagerdatapb.GetRoutingSnapshotResponse {
	resp.ConsensusRule = &clustermetadatapb.RuleNumber{CoordinatorTerm: term}
	return resp
}

// admit ends an admission StartRequest granted at once, and returns its error.
func admit(a poolerserver.Admission, err error) error {
	if err == nil {
		a.Done()
	}
	return err
}

func testFence(id, tablegroup int64, shard *int64, state shardingcatalogpb.FenceState) *shardingcatalogpb.Fence {
	return &shardingcatalogpb.Fence{FenceId: id, OperationId: importOp, TablegroupId: tablegroup, ShardId: shard, State: state, Version: 1}
}

// fenceTestEnv is a commerce/0-80 primary with an armed fence gate, a
// catalog primary in the topology and a fake client to read it with.
type fenceTestEnv struct {
	*tableGroupTestEnv
	qsc     *poolerserver.QueryPoolerServer
	client  *rpcclient.FakeClient
	catalog topoclient.ComponentID
}

func newFenceTestEnv(t *testing.T, tableGroupID int64) *fenceTestEnv {
	t.Helper()
	env := &fenceTestEnv{tableGroupTestEnv: newTableGroupTestEnv(t, "commerce", "0-80"), client: rpcclient.NewFakeClient()}
	env.pm.record = newRecordFromProto(&clustermetadatapb.Multipooler{
		ShardRef:     &clustermetadatapb.ShardRef{Database: testTableGroupDB, TableGroup: "commerce", Shard: "0-80"},
		TablegroupId: tableGroupID,
	})
	env.qsc = poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), nil, testPoolerID, "commerce", "0-80", nil, time.Second, false, true)
	env.qsc.EnableFenceGate()
	env.setRole(t, servingstate.RoutingRolePrimary)
	env.pm.qsc = env.qsc
	env.pm.fenceGateEnabled = true
	env.pm.config.ManagerClient = env.client
	env.addServingPrimary(t, "cell2", constants.DefaultTableGroup, constants.DefaultShard, 0)
	env.catalog = topoclient.ComponentIDString(&clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell2", Name: constants.DefaultTableGroup + "-" + constants.DefaultShard,
	})
	return env
}

func (env *fenceTestEnv) setRole(t *testing.T, role servingstate.RoutingRole) {
	t.Helper()
	require.NoError(t, env.qsc.OnStateChange(t.Context(), servingstate.State{
		Routing: servingstate.RoutingState{Role: role}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}))
}

// serve makes the catalog primary answer with resp.
func (env *fenceTestEnv) serve(resp *multipoolermanagerdatapb.GetRoutingSnapshotResponse) {
	env.client.SetGetRoutingSnapshotResponse(env.catalog, resp)
}

func (env *fenceTestEnv) refresh(t *testing.T, minVersion int64) (*multipoolermanagerdatapb.RefreshFencesResponse, error) {
	t.Helper()
	return env.pm.RefreshFences(t.Context(), &multipoolermanagerdatapb.RefreshFencesRequest{
		MinRoutingVersion: minVersion, DrainTimeout: durationpb.New(time.Second),
	})
}

func (env *fenceTestEnv) requireAdmits(t *testing.T) {
	t.Helper()
	require.NoError(t, admit(env.qsc.StartRequest(nil, poolerserver.RequestSingleQuery)))
	require.NoError(t, admit(env.qsc.StartRequest(nil, poolerserver.RequestNewReservation)))
}

func (env *fenceTestEnv) requireFenced(t *testing.T) {
	t.Helper()
	for _, kind := range []poolerserver.RequestKind{poolerserver.RequestSingleQuery, poolerserver.RequestNewReservation} {
		err := admit(env.qsc.StartRequest(nil, kind))
		require.Error(t, err)
		require.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "want MTF02, got %v", err)
	}
	require.NoError(t, admit(env.qsc.StartRequest(nil, poolerserver.RequestExistingReserved)))
}

func TestRefreshFences_AppliesAndLifts(t *testing.T) {
	for _, tc := range []struct {
		name         string
		tableGroupID int64 // 0 matches by name
	}{{"by catalog id", fenceTestCommerceID}, {"by name", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			env := newFenceTestEnv(t, tc.tableGroupID)
			env.requireFenced(t) // unread

			env.serve(fenceTestSnapshot(3))
			resp, err := env.refresh(t, 3)
			require.NoError(t, err)
			assert.Equal(t, int64(3), resp.GetRoutingVersion())
			assert.False(t, resp.GetFenced())
			env.requireAdmits(t)

			env.serve(fenceTestSnapshot(4, testFence(9, fenceTestCommerceID, nil, shardingcatalogpb.FenceState_FENCE_STATE_FENCED)))
			resp, err = env.refresh(t, 4)
			require.NoError(t, err)
			assert.True(t, resp.GetFenced())
			assert.Equal(t, int64(4), resp.GetRoutingVersion())
			env.requireFenced(t)

			// Refreshing again with nothing changed is a no-op.
			resp, err = env.refresh(t, 4)
			require.NoError(t, err)
			assert.True(t, resp.GetFenced())
			env.requireFenced(t)

			// A snapshot without a fence on the scope reopens the gate.
			env.serve(fenceTestSnapshot(5))
			resp, err = env.refresh(t, 5)
			require.NoError(t, err)
			assert.False(t, resp.GetFenced())
			env.requireAdmits(t)
		})
	}
}

func TestRefreshFences_Scopes(t *testing.T) {
	for name, tc := range map[string]struct {
		fence  *shardingcatalogpb.Fence
		fenced bool
	}{
		"the whole tablegroup":    {testFence(9, fenceTestCommerceID, nil, shardingcatalogpb.FenceState_FENCE_STATE_FENCED), true},
		"the pooler's shard":      {testFence(9, fenceTestCommerceID, new(fenceTestShard0080), shardingcatalogpb.FenceState_FENCE_STATE_FENCED), true},
		"FENCING gates as FENCED": {testFence(9, fenceTestCommerceID, new(fenceTestShard0080), shardingcatalogpb.FenceState_FENCE_STATE_FENCING), true},
		"another shard":           {testFence(9, fenceTestCommerceID, new(fenceTestShard80Inf), shardingcatalogpb.FenceState_FENCE_STATE_FENCED), false},
		"another tablegroup":      {testFence(9, 1, nil, shardingcatalogpb.FenceState_FENCE_STATE_FENCED), false},
	} {
		t.Run(name, func(t *testing.T) {
			env := newFenceTestEnv(t, fenceTestCommerceID)
			env.serve(fenceTestSnapshot(4, tc.fence))
			resp, err := env.refresh(t, 0)
			require.NoError(t, err)
			assert.Equal(t, tc.fenced, resp.GetFenced())
			if tc.fenced {
				env.requireFenced(t)
			} else {
				env.requireAdmits(t)
			}
		})
	}

	t.Run("an id of another incarnation of the name matches nothing", func(t *testing.T) {
		env := newFenceTestEnv(t, 42)
		env.serve(fenceTestSnapshot(4, testFence(9, fenceTestCommerceID, nil, shardingcatalogpb.FenceState_FENCE_STATE_FENCED)))
		resp, err := env.refresh(t, 0)
		require.NoError(t, err)
		assert.False(t, resp.GetFenced())
	})
}

func TestRefreshFences_RefusesAnOlderSnapshot(t *testing.T) {
	env := newFenceTestEnv(t, fenceTestCommerceID)
	env.serve(fenceTestSnapshot(3))
	_, err := env.refresh(t, 4)
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	env.requireFenced(t) // still unread: nothing was applied
}

// A catalog primary that a failover deposed may still answer, with an older
// snapshot. Once the pooler has applied its successor's snapshot, the deposed
// primary's answer is refused whatever its routing version, also after the
// pooler is promoted again, so it never reopens a gate a newer snapshot
// closed.
func TestRefreshFences_RefusesASnapshotOlderThanOneApplied(t *testing.T) {
	env := newFenceTestEnv(t, fenceTestCommerceID)
	fenced := testFence(9, fenceTestCommerceID, nil, shardingcatalogpb.FenceState_FENCE_STATE_FENCED)
	env.serve(servedUnder(2, fenceTestSnapshot(4, fenced)))
	resp, err := env.refresh(t, 4)
	require.NoError(t, err)
	require.True(t, resp.GetFenced())

	for _, stale := range []*multipoolermanagerdatapb.GetRoutingSnapshotResponse{
		servedUnder(1, fenceTestSnapshot(9)), // the deposed primary, at a higher version
		servedUnder(2, fenceTestSnapshot(3)), // the same primary, at a lower version
	} {
		env.serve(stale)
		_, err = env.refresh(t, 0)
		assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "%v", err)
		env.requireFenced(t)
	}

	env.setRole(t, servingstate.RoutingRoleReplica)
	env.setRole(t, servingstate.RoutingRolePrimary)
	env.serve(servedUnder(1, fenceTestSnapshot(9)))
	_, err = env.refresh(t, 0)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "the floor survives a promotion: %v", err)
	env.requireFenced(t) // unread

	// The successor's own unfence is applied.
	env.serve(servedUnder(2, fenceTestSnapshot(5)))
	resp, err = env.refresh(t, 5)
	require.NoError(t, err)
	assert.False(t, resp.GetFenced())
	env.requireAdmits(t)
}

func TestRefreshFences_FailsWhenTheCatalogPrimaryIsUnreachable(t *testing.T) {
	env := newFenceTestEnv(t, fenceTestCommerceID)
	env.serve(fenceTestSnapshot(3))
	applied, err := env.refresh(t, 0)
	require.NoError(t, err)
	require.False(t, applied.GetFenced())

	env.client.SetError(env.catalog, status.Error(codes.Unavailable, "connection refused"))
	_, err = env.refresh(t, 0)
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
	env.requireAdmits(t) // a failed read changes nothing

	t.Run("no catalog primary registered", func(t *testing.T) {
		env := newFenceTestEnv(t, fenceTestCommerceID)
		require.NoError(t, env.ts.UnregisterMultipooler(t.Context(), &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell2", Name: constants.DefaultTableGroup + "-" + constants.DefaultShard,
		}))
		_, err := env.refresh(t, 0)
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
		env.requireFenced(t)
	})
}

func TestRefreshFences_NoCatalogIsNoFences(t *testing.T) {
	env := newFenceTestEnv(t, fenceTestCommerceID)
	env.client.SetError(env.catalog, status.Error(codes.NotFound, "the sharding catalog was never created on this primary"))
	resp, err := env.refresh(t, 0)
	require.NoError(t, err)
	assert.False(t, resp.GetFenced())
	assert.Zero(t, resp.GetRoutingVersion())
	env.requireAdmits(t)
}

func TestRefreshFences_StaticLeaderHasNoFences(t *testing.T) {
	env := newFenceTestEnv(t, fenceTestCommerceID)
	env.pm.fenceGateEnabled = false
	_, err := env.refresh(t, 0)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
}

func TestRefreshFences_RejectsADrainTimeoutOutOfRange(t *testing.T) {
	env := newFenceTestEnv(t, fenceTestCommerceID)
	for _, d := range []time.Duration{-time.Second, poolerserver.MaxFenceDrainTimeout + time.Second} {
		_, err := env.pm.RefreshFences(t.Context(), &multipoolermanagerdatapb.RefreshFencesRequest{DrainTimeout: durationpb.New(d)})
		assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err), "drain_timeout %v", d)
	}
}

// catalogGateController is a real query server, for its fence gate, whose
// internal queries go to a mock.
type catalogGateController struct {
	*poolerserver.QueryPoolerServer
	qs executor.InternalQueryService
}

func (c catalogGateController) InternalQueryService() executor.InternalQueryService { return c.qs }

// The catalog primary reads its own catalog in process. A catalog primary
// on which the catalog was never created has no fences.
func TestRefreshFences_TheCatalogPrimaryReadsItsOwnCatalog(t *testing.T) {
	newCatalogFenceEnv := func(t *testing.T) (*tableGroupTestEnv, *poolerserver.QueryPoolerServer) {
		env := newCatalogTestEnv(t)
		qsc := poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), nil, testPoolerID, constants.DefaultTableGroup, constants.DefaultShard, nil, time.Second, false, true)
		qsc.EnableFenceGate()
		require.NoError(t, qsc.OnStateChange(t.Context(), servingstate.State{
			Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		}))
		env.pm.qsc = catalogGateController{QueryPoolerServer: qsc, qs: env.qs}
		env.pm.fenceGateEnabled = true
		return env, qsc
	}

	t.Run("fenced", func(t *testing.T) {
		env, qsc := newCatalogFenceEnv(t)
		env.expectPrimary()
		env.expectSnapshotRead(9, 2)
		env.expectCatalogRows() // fences the whole default tablegroup: fence 50
		// Drained, it acknowledges the fence for its shard (0-inf, id 1) in
		// its own catalog.
		env.expect(lockAckedFencePattern, []string{"in_scope"}, [][]any{{true}})
		env.expect(upsertFenceAckPattern, nil, nil)
		env.expectCommit()
		resp, err := env.pm.RefreshFences(t.Context(), &multipoolermanagerdatapb.RefreshFencesRequest{MinRoutingVersion: 9})
		require.NoError(t, err)
		assert.True(t, resp.GetFenced())
		assert.Equal(t, int64(9), resp.GetRoutingVersion())
		err = admit(qsc.StartRequest(nil, poolerserver.RequestSingleQuery))
		assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
		assert.Len(t, env.queriesMatching("INSERT INTO multigres.admission_fence_ack"), 1)
		assert.Len(t, env.queriesMatching("COMMIT"), 1)
	})
	t.Run("no catalog on this primary", func(t *testing.T) {
		env, qsc := newCatalogFenceEnv(t)
		env.expectPrimary()
		env.expect("^SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY$", nil, nil)
		env.qs.AddQueryPatternWithError("^SELECT routing_version, plan_epoch FROM multigres.catalog_state$",
			mterrors.NewPgError("ERROR", mterrors.PgSSUndefinedTable, `relation "multigres.catalog_state" does not exist`, ""))
		resp, err := env.pm.RefreshFences(t.Context(), &multipoolermanagerdatapb.RefreshFencesRequest{})
		require.NoError(t, err)
		assert.False(t, resp.GetFenced())
		require.NoError(t, admit(qsc.StartRequest(nil, poolerserver.RequestNewReservation)))
	})
}

// snapshotReads counts the GetRoutingSnapshot calls the pooler made.
func (env *fenceTestEnv) snapshotReads() int {
	n := 0
	for _, c := range env.client.GetCallLog() {
		if strings.HasPrefix(c, "GetRoutingSnapshot(") {
			n++
		}
	}
	return n
}

// waitAdmits waits for the gate to admit new work.
func (env *fenceTestEnv) waitAdmits(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		return admit(env.qsc.StartRequest(nil, poolerserver.RequestNewReservation)) == nil
	}, 5*time.Second, 10*time.Millisecond, "the gate never opened")
}

// startReads runs the background fence reader, as registration does.
func (env *fenceTestEnv) startReads(t *testing.T) {
	t.Helper()
	env.pm.record.registeredOnce.Do(func() { close(env.pm.record.registered) })
	go env.pm.runFenceReads(t.Context())
}

// A primary rejects work until its fences are read; the reader keeps
// retrying an unreachable catalog primary and opens the gate once it reads.
func TestFenceReads_FailClosedOnStart(t *testing.T) {
	saved := fenceReadRetryMax
	fenceReadRetryMax = 20 * time.Millisecond
	t.Cleanup(func() { fenceReadRetryMax = saved })

	env := newFenceTestEnv(t, fenceTestCommerceID)
	env.client.SetError(env.catalog, errors.New("connection refused"))
	env.serve(fenceTestSnapshot(3))

	// Nothing is read before the pooler is registered.
	go env.pm.runFenceReads(t.Context())
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, env.snapshotReads(), "no read before registration")

	env.startReads(t)
	require.Eventually(t, func() bool { return env.snapshotReads() >= 2 }, 5*time.Second, 10*time.Millisecond, "the reader retries")
	env.requireFenced(t)

	env.client.SetError(env.catalog, nil)
	env.waitAdmits(t)
	calls := env.snapshotReads()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, calls, env.snapshotReads(), "an unfenced pooler does not poll")
}

// Becoming primary closes the gate until the fences are read again, and a
// fenced primary re-reads them, so a missed unfence is lifted.
func TestFenceReads_PromotionAndMissedUnfence(t *testing.T) {
	saved := fenceRecheckInterval
	fenceRecheckInterval = 50 * time.Millisecond
	t.Cleanup(func() { fenceRecheckInterval = saved })

	env := newFenceTestEnv(t, fenceTestCommerceID)
	env.setRole(t, servingstate.RoutingRoleReplica)
	env.serve(fenceTestSnapshot(3))
	env.startReads(t)
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, env.snapshotReads(), "a replica does not read")
	env.requireAdmits(t)

	// The fence is raised while the pooler is a replica; it is read on
	// promotion, before any work is admitted.
	env.serve(fenceTestSnapshot(4, testFence(9, fenceTestCommerceID, nil, shardingcatalogpb.FenceState_FENCE_STATE_FENCED)))
	env.setRole(t, servingstate.RoutingRolePrimary)
	env.requireFenced(t)
	require.Eventually(t, func() bool { return env.snapshotReads() >= 1 }, 5*time.Second, 10*time.Millisecond)
	env.requireFenced(t)

	// The fence is lifted with no refresh: the periodic re-read lifts it.
	env.serve(fenceTestSnapshot(5))
	env.waitAdmits(t)
}

// fencedBy is a FENCED testFence.
func fencedBy(id, tablegroup int64, shard *int64) *shardingcatalogpb.Fence {
	return testFence(id, tablegroup, shard, shardingcatalogpb.FenceState_FENCE_STATE_FENCED)
}

// acks returns the acks of every AckFences call the pooler made, one slice
// per call.
func (env *fenceTestEnv) acks() [][]*multipoolermanagerdatapb.FenceAck {
	var out [][]*multipoolermanagerdatapb.FenceAck
	for _, req := range env.client.GetAckFencesRequests() {
		out = append(out, req.GetAcks())
	}
	return out
}

// ackOf is the ack the test pooler, of commerce/0-80, sends for a fence.
func (env *fenceTestEnv) ackOf(fenceID int64) *multipoolermanagerdatapb.FenceAck {
	return &multipoolermanagerdatapb.FenceAck{
		FenceId: fenceID, ShardId: fenceTestShard0080,
		PoolerId: string(topoclient.ComponentIDString(testPoolerID)), ConsensusRule: "1.0",
	}
}

func requireAcks(t *testing.T, want, got [][]*multipoolermanagerdatapb.FenceAck) {
	t.Helper()
	text := func(calls [][]*multipoolermanagerdatapb.FenceAck) [][]string {
		out := make([][]string, 0, len(calls))
		for _, call := range calls {
			acks := make([]string, 0, len(call))
			for _, a := range call {
				acks = append(acks, prototext.Format(a))
			}
			out = append(out, acks)
		}
		return out
	}
	require.Equal(t, text(want), text(got), "AckFences calls")
}

// A drained primary acknowledges every fence covering its shard, the whole
// tablegroup's and its shard's, once per fence in a gate epoch, and again
// after a promotion.
func TestRefreshFences_AcksEveryCoveringFence(t *testing.T) {
	env := newFenceTestEnv(t, fenceTestCommerceID)
	whole := fencedBy(9, fenceTestCommerceID, nil)
	own := fencedBy(10, fenceTestCommerceID, new(fenceTestShard0080))
	other := fencedBy(11, fenceTestCommerceID, new(fenceTestShard80Inf))
	env.serve(fenceTestSnapshot(6, whole, own, other))
	_, err := env.refresh(t, 6)
	require.NoError(t, err)
	want := [][]*multipoolermanagerdatapb.FenceAck{{env.ackOf(9), env.ackOf(10)}}
	requireAcks(t, want, env.acks())

	// Already acknowledged in this epoch: nothing is sent again.
	_, err = env.refresh(t, 6)
	require.NoError(t, err)
	requireAcks(t, want, env.acks())

	// A fence rewritten in place (a newer version, the same id) is the same
	// fence: nothing is sent. A new fence is acknowledged on its own.
	rewritten := fencedBy(10, fenceTestCommerceID, new(fenceTestShard0080))
	rewritten.Version = 8
	env.serve(fenceTestSnapshot(8, whole, rewritten))
	_, err = env.refresh(t, 8)
	require.NoError(t, err)
	requireAcks(t, want, env.acks())
	env.serve(fenceTestSnapshot(9, whole, fencedBy(12, fenceTestCommerceID, new(fenceTestShard0080))))
	_, err = env.refresh(t, 9)
	require.NoError(t, err)
	want = append(want, []*multipoolermanagerdatapb.FenceAck{env.ackOf(12)})
	requireAcks(t, want, env.acks())

	// A promotion starts a new epoch: everything is acknowledged again.
	env.setRole(t, servingstate.RoutingRoleReplica)
	env.setRole(t, servingstate.RoutingRolePrimary)
	_, err = env.refresh(t, 9)
	require.NoError(t, err)
	want = append(want, []*multipoolermanagerdatapb.FenceAck{env.ackOf(9), env.ackOf(12)})
	requireAcks(t, want, env.acks())
}

// Only a drained primary acknowledges: not an unfenced pooler, not a replica,
// and not a pooler whose shard the snapshot does not hold.
func TestRefreshFences_AcksOnlyWhenDrainedAsPrimary(t *testing.T) {
	fenced := fenceTestSnapshot(4, fencedBy(9, fenceTestCommerceID, nil))

	t.Run("unfenced", func(t *testing.T) {
		env := newFenceTestEnv(t, fenceTestCommerceID)
		env.serve(fenceTestSnapshot(4, fencedBy(9, fenceTestCommerceID, new(fenceTestShard80Inf))))
		resp, err := env.refresh(t, 0)
		require.NoError(t, err)
		require.False(t, resp.GetFenced())
		assert.Empty(t, env.acks())
	})
	t.Run("a replica", func(t *testing.T) {
		env := newFenceTestEnv(t, fenceTestCommerceID)
		env.setRole(t, servingstate.RoutingRoleReplica)
		env.serve(fenced)
		resp, err := env.refresh(t, 0)
		require.NoError(t, err)
		require.True(t, resp.GetFenced())
		assert.Empty(t, env.acks())
	})
	t.Run("a shard the snapshot does not hold", func(t *testing.T) {
		env := newFenceTestEnv(t, fenceTestCommerceID)
		env.pm.record = newRecordFromProto(&clustermetadatapb.Multipooler{
			ShardRef:     &clustermetadatapb.ShardRef{Database: testTableGroupDB, TableGroup: "commerce", Shard: "0-40"},
			TablegroupId: fenceTestCommerceID,
		})
		env.serve(fenced)
		resp, err := env.refresh(t, 0)
		require.NoError(t, err)
		require.True(t, resp.GetFenced(), "a fence on the whole tablegroup fences it")
		env.requireFenced(t)
		assert.Empty(t, env.acks(), "it has no shard id to acknowledge for")
	})
	t.Run("a pooler with no consensus rule (unmanaged) sends none", func(t *testing.T) {
		env := newFenceTestEnv(t, fenceTestCommerceID)
		env.pm.stateManager = nil
		env.serve(fenced)
		_, err := env.refresh(t, 0)
		require.NoError(t, err)
		ack := env.ackOf(9)
		ack.ConsensusRule = ""
		requireAcks(t, [][]*multipoolermanagerdatapb.FenceAck{{ack}}, env.acks())
	})
}

// hookedGate is the fence-test query server with ApplyFence wrapped.
type hookedGate struct {
	*poolerserver.QueryPoolerServer
	apply func(ctx context.Context, read poolerserver.FenceRead, drainTimeout time.Duration) error
}

func (g hookedGate) ApplyFence(ctx context.Context, read poolerserver.FenceRead, drainTimeout time.Duration) error {
	return g.apply(ctx, read, drainTimeout)
}

// A drain that fails, and a read whose epoch ended under it, acknowledge
// nothing.
func TestRefreshFences_NoAckUnlessClosedInTheReadsEpoch(t *testing.T) {
	t.Run("a failed drain", func(t *testing.T) {
		env := newFenceTestEnv(t, fenceTestCommerceID)
		env.pm.qsc = hookedGate{env.qsc, func(context.Context, poolerserver.FenceRead, time.Duration) error {
			return mterrors.WithCode(poolerserver.ErrDrainIncomplete, mtrpcpb.Code_UNAVAILABLE)
		}}
		env.serve(fenceTestSnapshot(4, fencedBy(9, fenceTestCommerceID, nil)))
		_, err := env.refresh(t, 0)
		assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "%v", err)
		assert.Empty(t, env.acks())
	})
	t.Run("a promotion after the drain", func(t *testing.T) {
		env := newFenceTestEnv(t, fenceTestCommerceID)
		env.pm.qsc = hookedGate{env.qsc, func(ctx context.Context, read poolerserver.FenceRead, d time.Duration) error {
			if err := env.qsc.ApplyFence(ctx, read, d); err != nil {
				return err
			}
			// The pooler loses and regains the primary role before the
			// read returns: its gate is unread, in a new epoch.
			env.setRole(t, servingstate.RoutingRoleReplica)
			env.setRole(t, servingstate.RoutingRolePrimary)
			return nil
		}}
		env.serve(fenceTestSnapshot(4, fencedBy(9, fenceTestCommerceID, nil)))
		_, err := env.refresh(t, 0)
		require.NoError(t, err)
		assert.Empty(t, env.acks())
		env.requireFenced(t) // unread
	})
}

// RefreshFences succeeds only once its acks are recorded: a failed ack fails
// it, with the gate left closed.
func TestRefreshFences_AFailedAckFailsTheRefresh(t *testing.T) {
	env := newFenceTestEnv(t, fenceTestCommerceID)
	env.serve(fenceTestSnapshot(4, fencedBy(9, fenceTestCommerceID, nil)))
	env.client.SetAckFencesError(status.Error(codes.Unavailable, "connection refused"))
	_, err := env.refresh(t, 4)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "%v", err)
	env.requireFenced(t)

	env.client.SetAckFencesError(nil)
	resp, err := env.refresh(t, 4)
	require.NoError(t, err)
	assert.True(t, resp.GetFenced())
	assert.Len(t, env.acks(), 2, "the failed ack is sent again")
}

// The background reader retries a failed ack, and once it is recorded does
// not send it again on every recheck.
func TestFenceReads_RetryAFailedAck(t *testing.T) {
	savedRecheck, savedRetry := fenceRecheckInterval, fenceReadRetryMax
	fenceRecheckInterval, fenceReadRetryMax = 20*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { fenceRecheckInterval, fenceReadRetryMax = savedRecheck, savedRetry })

	env := newFenceTestEnv(t, fenceTestCommerceID)
	env.serve(fenceTestSnapshot(4, fencedBy(9, fenceTestCommerceID, nil)))
	env.client.SetAckFencesError(errors.New("connection refused"))
	// The reader must stop before the intervals are restored.
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	t.Cleanup(func() { cancel(); <-stopped })
	env.pm.record.registeredOnce.Do(func() { close(env.pm.record.registered) })
	go func() { defer close(stopped); env.pm.runFenceReads(ctx) }()
	require.Eventually(t, func() bool { return len(env.acks()) >= 2 }, 5*time.Second, 10*time.Millisecond, "the reader retries the ack")
	env.requireFenced(t)

	env.client.SetAckFencesError(nil)
	require.Eventually(t, func() bool {
		return env.snapshotReads() > 0 && !strings.HasPrefix(env.client.GetCallLog()[len(env.client.GetCallLog())-1], "AckFences(")
	}, 5*time.Second, 10*time.Millisecond, "the reader goes on rechecking once the ack is recorded")
	sent := len(env.acks())
	reads := env.snapshotReads()
	require.Eventually(t, func() bool { return env.snapshotReads() >= reads+3 }, 5*time.Second, 10*time.Millisecond, "the reader rechecks")
	assert.Equal(t, sent, len(env.acks()), "a recorded ack is not sent again in the same epoch")
}
