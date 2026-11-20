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
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	env.qsc = poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), nil, testPoolerID, "commerce", "0-80", nil, time.Second, false)
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

func TestRefreshFences_RejectsANegativeDrainTimeout(t *testing.T) {
	env := newFenceTestEnv(t, fenceTestCommerceID)
	_, err := env.pm.RefreshFences(t.Context(), &multipoolermanagerdatapb.RefreshFencesRequest{DrainTimeout: durationpb.New(-time.Second)})
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
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
		qsc := poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), nil, testPoolerID, constants.DefaultTableGroup, constants.DefaultShard, nil, time.Second, false)
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
		env.expectCatalogRows() // fences the whole default tablegroup
		resp, err := env.pm.RefreshFences(t.Context(), &multipoolermanagerdatapb.RefreshFencesRequest{MinRoutingVersion: 9})
		require.NoError(t, err)
		assert.True(t, resp.GetFenced())
		assert.Equal(t, int64(9), resp.GetRoutingVersion())
		err = admit(qsc.StartRequest(nil, poolerserver.RequestSingleQuery))
		assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
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
