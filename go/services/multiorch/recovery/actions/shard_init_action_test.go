// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actions

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

var testCoordinatorID = &clustermetadatapb.ID{
	Component: clustermetadatapb.ID_MULTIORCH,
	Cell:      "cell1",
	Name:      "test-multiorch",
}

// mockCoordinator implements shardInitCoordinator for tests.
type mockCoordinator struct {
	bootstrapPolicy    *clustermetadatapb.DurabilityPolicy
	bootstrapPolicyErr error

	appointInitialLeaderErr error
	appointedCohort         []*multiorchdatapb.PoolerHealthState
	appointedShardID        string
	appointedDatabase       string

	coordinatorID *clustermetadatapb.ID
}

func (m *mockCoordinator) GetBootstrapPolicy(_ context.Context, _ string) (*clustermetadatapb.DurabilityPolicy, error) {
	return m.bootstrapPolicy, m.bootstrapPolicyErr
}

func (m *mockCoordinator) AppointInitialLeader(_ context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, database string) error {
	m.appointedShardID = shardID
	m.appointedCohort = cohort
	m.appointedDatabase = database
	return m.appointInitialLeaderErr
}

func (m *mockCoordinator) GetCoordinatorID() *clustermetadatapb.ID {
	if m.coordinatorID != nil {
		return m.coordinatorID
	}
	return testCoordinatorID
}

var testShardInitShardKey = commontypes.ShardKey{
	Database:   "testdb",
	TableGroup: "default",
	Shard:      "0",
}

func makePoolerState(cell, name, db, tableGroup, shard string, initialized bool, cohortMembers []*clustermetadatapb.ID) *multiorchdatapb.PoolerHealthState {
	return &multiorchdatapb.PoolerHealthState{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: initialized,
			CohortMembers: cohortMembers,
		},
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      cell,
				Name:      name,
			},
			Database:   db,
			TableGroup: tableGroup,
			Shard:      shard,
		},
	}
}

func newTestAction(t *testing.T, coord shardInitCoordinator, poolerStore *store.PoolerStore, ts topoclient.Store) *ShardInitAction {
	t.Helper()
	if ts == nil {
		ts = memorytopo.NewServer(t.Context(), "cell1")
	}
	return NewShardInitAction(nil, coord, poolerStore, ts, slog.Default())
}

func newPoolerStore(t *testing.T) *store.PoolerStore {
	t.Helper()
	return store.NewPoolerStore(rpcclient.NewFakeClient(), slog.Default())
}

// --- Interface / metadata ---

func TestShardInitAction_Metadata(t *testing.T) {
	action := NewShardInitAction(nil, nil, nil, nil, slog.Default())
	m := action.Metadata()
	assert.Equal(t, "ShardInit", m.Name)
	assert.True(t, m.Retryable)
	assert.Equal(t, 60*time.Second, m.Timeout)
}

func TestShardInitAction_RequiresHealthyPrimary(t *testing.T) {
	assert.False(t, NewShardInitAction(nil, nil, nil, nil, slog.Default()).RequiresHealthyPrimary())
}

func TestShardInitAction_Priority(t *testing.T) {
	assert.Equal(t, types.PriorityShardBootstrap, NewShardInitAction(nil, nil, nil, nil, slog.Default()).Priority())
}

func TestShardInitAction_GracePeriod(t *testing.T) {
	assert.Nil(t, NewShardInitAction(nil, nil, nil, nil, slog.Default()).GracePeriod())
}

// --- getInitializedPoolers ---

func TestShardInitAction_GetInitializedPoolers_FiltersByShard(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-other", makePoolerState("cell1", "other", "otherdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-shard1", makePoolerState("cell1", "shard1", "testdb", "default", "1", true, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testShardInitShardKey)

	assert.False(t, cohortEstablished)
	require.Len(t, initialized, 2)
	names := []string{initialized[0].MultiPooler.Id.Name, initialized[1].MultiPooler.Id.Name}
	assert.ElementsMatch(t, []string{"p1", "p2"}, names)
}

func TestShardInitAction_GetInitializedPoolers_CohortAlreadyEstablished(t *testing.T) {
	ps := newPoolerStore(t)
	existingCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
	}
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, existingCohort))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testShardInitShardKey)

	assert.True(t, cohortEstablished)
	assert.Nil(t, initialized)
}

func TestShardInitAction_GetInitializedPoolers_NotYetInitialized(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", false, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testShardInitShardKey)

	assert.False(t, cohortEstablished)
	assert.Empty(t, initialized)
}

// --- Execute ---

func TestShardInitAction_Execute_NoInitializedPoolers(t *testing.T) {
	ps := newPoolerStore(t)
	// Pooler exists but is not initialized
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", false, nil))

	action := newTestAction(t, nil, ps, nil)
	err := action.Execute(t.Context(), types.Problem{ShardKey: testShardInitShardKey})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no initialized poolers found for shard")
}

func TestShardInitAction_Execute_CohortAlreadyEstablished(t *testing.T) {
	ps := newPoolerStore(t)
	existingCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
	}
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, existingCohort))

	coord := &mockCoordinator{}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.Problem{ShardKey: testShardInitShardKey})

	require.NoError(t, err)
	assert.Empty(t, coord.appointedCohort, "AppointInitialLeader must not be called when cohort is already established")
}

func TestShardInitAction_Execute_GetBootstrapPolicyError(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicyErr: errors.New("etcd unreachable")}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.Problem{ShardKey: testShardInitShardKey})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load durability policy")
	assert.Contains(t, err.Error(), "etcd unreachable")
}

func TestShardInitAction_Execute_InsufficientInitializedPoolers(t *testing.T) {
	ps := newPoolerStore(t)
	// Only 1 initialized pooler but policy requires 2
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.Problem{ShardKey: testShardInitShardKey})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient initialized poolers")
	assert.Empty(t, coord.appointedCohort)
}

func TestShardInitAction_Execute_Success(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")
	action := newTestAction(t, coord, ps, ts)

	err := action.Execute(t.Context(), types.Problem{ShardKey: testShardInitShardKey})
	require.NoError(t, err)

	require.Len(t, coord.appointedCohort, 2)
	names := []string{coord.appointedCohort[0].MultiPooler.Id.Name, coord.appointedCohort[1].MultiPooler.Id.Name}
	assert.ElementsMatch(t, []string{"p1", "p2"}, names)
	assert.Equal(t, "0", coord.appointedShardID)
	assert.Equal(t, "testdb", coord.appointedDatabase)
}

func TestShardInitAction_Execute_ClaimAfterCrash(t *testing.T) {
	// Same coordinator already claimed but crashed before appointing.
	// On retry it should win again and proceed with the committed cohort
	// from etcd, NOT the current pooler store contents.
	ps := newPoolerStore(t)
	// Pooler store has all four poolers, but the committed cohort only has prior-p1/prior-p2.
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-prior-p1", makePoolerState("cell1", "prior-p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-prior-p2", makePoolerState("cell1", "prior-p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")

	// Pre-write the claim with the same coordinator ID but different pooler names
	// than p1/p2. The committed cohort from etcd should take priority over what
	// the current pooler store would freshly select.
	priorCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "prior-p1"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "prior-p2"},
	}
	won, _, err := ts.ClaimShardInitialization(t.Context(), testShardInitShardKey, testCoordinatorID, priorCohort)
	require.NoError(t, err)
	require.True(t, won)

	action := newTestAction(t, coord, ps, ts)
	err = action.Execute(t.Context(), types.Problem{ShardKey: testShardInitShardKey})
	require.NoError(t, err)

	// The appointed cohort should use the etcd-committed names, not the pooler store names.
	require.Len(t, coord.appointedCohort, 2)
	names := []string{coord.appointedCohort[0].MultiPooler.Id.Name, coord.appointedCohort[1].MultiPooler.Id.Name}
	assert.ElementsMatch(t, []string{"prior-p1", "prior-p2"}, names)
}

func TestShardInitAction_Execute_ClaimLostToDifferentCoordinator(t *testing.T) {
	// A different coordinator already claimed this shard. We should back off
	// without calling AppointInitialLeader.
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")

	// Pre-write the claim with a different coordinator ID.
	otherCoord := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell2", Name: "other-multiorch"}
	otherCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell2", Name: "p3"},
	}
	won, _, err := ts.ClaimShardInitialization(t.Context(), testShardInitShardKey, otherCoord, otherCohort)
	require.NoError(t, err)
	require.True(t, won)

	action := newTestAction(t, coord, ps, ts)
	err = action.Execute(t.Context(), types.Problem{ShardKey: testShardInitShardKey})

	require.NoError(t, err)
	assert.Empty(t, coord.appointedCohort, "AppointInitialLeader must not be called when another coordinator owns the claim")
}

func TestShardInitAction_Execute_AppointInitialLeaderError(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{
		bootstrapPolicy:         topoclient.AtLeastN(2),
		appointInitialLeaderErr: errors.New("consensus failed"),
	}
	action := newTestAction(t, coord, ps, memorytopo.NewServer(t.Context(), "cell1"))

	err := action.Execute(t.Context(), types.Problem{ShardKey: testShardInitShardKey})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to appoint initial leader")
	assert.Contains(t, err.Error(), "consensus failed")
}
