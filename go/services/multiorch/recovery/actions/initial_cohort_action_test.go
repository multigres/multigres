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
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// mockCoordinator implements initialCohortCoordinator for tests.
type mockCoordinator struct {
	bootstrapPolicy    *clustermetadatapb.DurabilityPolicy
	bootstrapPolicyErr error

	appointInitialLeaderErr error
	appointedCohort         []*multiorchdatapb.PoolerHealthState
	appointedShardID        string
	appointedDatabase       string
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

var testInitialCohortShardKey = commontypes.ShardKey{
	Database:   "testdb",
	TableGroup: "default",
	Shard:      "0",
}

// atLeastNPolicy returns a DurabilityPolicy requiring n nodes.
func atLeastNPolicy(n int32) *clustermetadatapb.DurabilityPolicy {
	return &clustermetadatapb.DurabilityPolicy{RequiredCount: n}
}

func makePoolerState(cell, name, db, tableGroup, shard string, initialized bool, cohortMembers []*clustermetadatapb.ID) *multiorchdatapb.PoolerHealthState {
	return &multiorchdatapb.PoolerHealthState{
		IsInitialized: initialized,
		CohortMembers: cohortMembers,
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

func newTestAction(t *testing.T, coord initialCohortCoordinator, poolerStore *store.PoolerStore, ts topoclient.Store) *InitialCohortAction {
	t.Helper()
	if ts == nil {
		ts = memorytopo.NewServer(t.Context(), "cell1")
	}
	return NewInitialCohortAction(nil, coord, poolerStore, ts, slog.Default())
}

func newPoolerStore(t *testing.T) *store.PoolerStore {
	t.Helper()
	return store.NewPoolerStore(rpcclient.NewFakeClient(), slog.Default())
}

// --- Interface / metadata ---

func TestInitialCohortAction_Metadata(t *testing.T) {
	action := NewInitialCohortAction(nil, nil, nil, nil, slog.Default())
	m := action.Metadata()
	assert.Equal(t, "InitialCohort", m.Name)
	assert.True(t, m.Retryable)
	assert.Equal(t, 60*time.Second, m.Timeout)
}

func TestInitialCohortAction_RequiresHealthyPrimary(t *testing.T) {
	assert.False(t, NewInitialCohortAction(nil, nil, nil, nil, slog.Default()).RequiresHealthyPrimary())
}

func TestInitialCohortAction_Priority(t *testing.T) {
	assert.Equal(t, types.PriorityShardBootstrap, NewInitialCohortAction(nil, nil, nil, nil, slog.Default()).Priority())
}

func TestInitialCohortAction_GracePeriod(t *testing.T) {
	assert.Nil(t, NewInitialCohortAction(nil, nil, nil, nil, slog.Default()).GracePeriod())
}

// --- getInitializedPoolers ---

func TestInitialCohortAction_GetInitializedPoolers_FiltersByShard(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-other", makePoolerState("cell1", "other", "otherdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-shard1", makePoolerState("cell1", "shard1", "testdb", "default", "1", true, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testInitialCohortShardKey)

	assert.False(t, cohortEstablished)
	require.Len(t, initialized, 2)
	names := []string{initialized[0].MultiPooler.Id.Name, initialized[1].MultiPooler.Id.Name}
	assert.ElementsMatch(t, []string{"p1", "p2"}, names)
}

func TestInitialCohortAction_GetInitializedPoolers_CohortAlreadyEstablished(t *testing.T) {
	ps := newPoolerStore(t)
	existingCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
	}
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, existingCohort))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testInitialCohortShardKey)

	assert.True(t, cohortEstablished)
	assert.Nil(t, initialized)
}

func TestInitialCohortAction_GetInitializedPoolers_NotYetInitialized(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", false, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testInitialCohortShardKey)

	assert.False(t, cohortEstablished)
	assert.Empty(t, initialized)
}

// --- buildCohortFromIDs ---

func TestInitialCohortAction_BuildCohortFromIDs(t *testing.T) {
	makeState := func(name string) *multiorchdatapb.PoolerHealthState {
		return &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{Id: &clustermetadatapb.ID{Name: name}},
		}
	}
	cohort := []*multiorchdatapb.PoolerHealthState{makeState("p1"), makeState("p2"), makeState("p3")}
	action := NewInitialCohortAction(nil, nil, nil, nil, slog.Default())

	t.Run("returns matching subset", func(t *testing.T) {
		result := action.buildCohortFromIDs(cohort, []string{"p1", "p3"})
		require.Len(t, result, 2)
		assert.ElementsMatch(t,
			[]string{"p1", "p3"},
			[]string{result[0].MultiPooler.Id.Name, result[1].MultiPooler.Id.Name})
	})

	t.Run("returns empty when no match", func(t *testing.T) {
		assert.Empty(t, action.buildCohortFromIDs(cohort, []string{"p4", "p5"}))
	})

	t.Run("returns all when all match", func(t *testing.T) {
		assert.Len(t, action.buildCohortFromIDs(cohort, []string{"p1", "p2", "p3"}), 3)
	})
}

// --- Execute ---

func TestInitialCohortAction_Execute_NoInitializedPoolers(t *testing.T) {
	ps := newPoolerStore(t)
	// Pooler exists but is not initialized
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", false, nil))

	action := newTestAction(t, nil, ps, nil)
	err := action.Execute(t.Context(), types.Problem{ShardKey: testInitialCohortShardKey})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no initialized poolers found for shard")
}

func TestInitialCohortAction_Execute_CohortAlreadyEstablished(t *testing.T) {
	ps := newPoolerStore(t)
	existingCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
	}
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, existingCohort))

	coord := &mockCoordinator{}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.Problem{ShardKey: testInitialCohortShardKey})

	require.NoError(t, err)
	assert.Empty(t, coord.appointedCohort, "AppointInitialLeader must not be called when cohort is already established")
}

func TestInitialCohortAction_Execute_GetBootstrapPolicyError(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicyErr: errors.New("etcd unreachable")}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.Problem{ShardKey: testInitialCohortShardKey})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load durability policy")
	assert.Contains(t, err.Error(), "etcd unreachable")
}

func TestInitialCohortAction_Execute_InsufficientInitializedPoolers(t *testing.T) {
	ps := newPoolerStore(t)
	// Only 1 initialized pooler but policy requires 2
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: atLeastNPolicy(2)}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.Problem{ShardKey: testInitialCohortShardKey})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient initialized poolers")
	assert.Empty(t, coord.appointedCohort)
}

func TestInitialCohortAction_Execute_Success(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: atLeastNPolicy(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")
	action := newTestAction(t, coord, ps, ts)

	err := action.Execute(t.Context(), types.Problem{ShardKey: testInitialCohortShardKey})
	require.NoError(t, err)

	require.Len(t, coord.appointedCohort, 2)
	names := []string{coord.appointedCohort[0].MultiPooler.Id.Name, coord.appointedCohort[1].MultiPooler.Id.Name}
	assert.ElementsMatch(t, []string{"p1", "p2"}, names)
	assert.Equal(t, "0", coord.appointedShardID)
	assert.Equal(t, "testdb", coord.appointedDatabase)
}

func TestInitialCohortAction_Execute_CASIdempotent(t *testing.T) {
	// Another multiorch instance already claimed the cohort with [p1, p2].
	// Our store also knows p1 and p2, so we get the same committed IDs and proceed normally.
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: atLeastNPolicy(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")

	// Pre-write the cohort claim to simulate a prior claimant.
	committed, err := ts.ClaimInitialCohort(t.Context(), testInitialCohortShardKey, []string{"p1", "p2"})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"p1", "p2"}, committed)

	action := newTestAction(t, coord, ps, ts)
	err = action.Execute(t.Context(), types.Problem{ShardKey: testInitialCohortShardKey})
	require.NoError(t, err)

	require.Len(t, coord.appointedCohort, 2)
	names := []string{coord.appointedCohort[0].MultiPooler.Id.Name, coord.appointedCohort[1].MultiPooler.Id.Name}
	assert.ElementsMatch(t, []string{"p1", "p2"}, names)
}

func TestInitialCohortAction_Execute_CommittedCohortPartiallyUnknown(t *testing.T) {
	// Another multiorch already committed [p2, p3, p4]. Our store knows p1, p2, p3
	// (enough to pass the first quorum check of 3), but not p4. After ClaimInitialCohort
	// returns the prior [p2, p3, p4], buildCohortFromIDs finds only [p2, p3] — below
	// the required quorum of 3 — so we return UNAVAILABLE to retry next cycle.
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p3", makePoolerState("cell1", "p3", "testdb", "default", "0", true, nil))
	// p4 is not in our store

	coord := &mockCoordinator{bootstrapPolicy: atLeastNPolicy(3)}
	ts := memorytopo.NewServer(t.Context(), "cell1")

	// Pre-write a claim that includes p4 (unknown to us).
	_, err := ts.ClaimInitialCohort(t.Context(), testInitialCohortShardKey, []string{"p2", "p3", "p4"})
	require.NoError(t, err)

	action := newTestAction(t, coord, ps, ts)
	err = action.Execute(t.Context(), types.Problem{ShardKey: testInitialCohortShardKey})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient initial cohort poolers reachable")
	assert.Empty(t, coord.appointedCohort)
}

func TestInitialCohortAction_Execute_CommittedCohortCompletelyUnknown(t *testing.T) {
	// Another multiorch committed [p3, p4], which are completely unknown to our store.
	// Our store knows [p1, p2] but ClaimInitialCohort returns the prior [p3, p4].
	// buildCohortFromIDs finds no matches, so AppointInitialLeader must not be called.
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: atLeastNPolicy(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")

	// Pre-write a claim with entirely different pooler IDs.
	_, err := ts.ClaimInitialCohort(t.Context(), testInitialCohortShardKey, []string{"p3", "p4"})
	require.NoError(t, err)

	action := newTestAction(t, coord, ps, ts)
	err = action.Execute(t.Context(), types.Problem{ShardKey: testInitialCohortShardKey})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient initial cohort poolers reachable")
	assert.Empty(t, coord.appointedCohort, "AppointInitialLeader must not be called when committed cohort is unknown")
}

func TestInitialCohortAction_Execute_AppointInitialLeaderError(t *testing.T) {
	ps := newPoolerStore(t)
	ps.Set("multipooler-cell1-p1", makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	ps.Set("multipooler-cell1-p2", makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{
		bootstrapPolicy:         atLeastNPolicy(2),
		appointInitialLeaderErr: errors.New("consensus failed"),
	}
	action := newTestAction(t, coord, ps, memorytopo.NewServer(t.Context(), "cell1"))

	err := action.Execute(t.Context(), types.Problem{ShardKey: testInitialCohortShardKey})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to appoint initial leader")
	assert.Contains(t, err.Error(), "consensus failed")
}
