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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
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
	appointedShardKey       *clustermetadatapb.ShardKey

	coordinatorID *clustermetadatapb.ID
}

func (m *mockCoordinator) GetBootstrapPolicy(_ context.Context, _ string) (*clustermetadatapb.DurabilityPolicy, error) {
	return m.bootstrapPolicy, m.bootstrapPolicyErr
}

func (m *mockCoordinator) AppointInitialLeader(_ context.Context, shardKey *clustermetadatapb.ShardKey, cohort []*multiorchdatapb.PoolerHealthState) error {
	m.appointedShardKey = shardKey
	m.appointedCohort = cohort
	return m.appointInitialLeaderErr
}

func (m *mockCoordinator) GetCoordinatorID() *clustermetadatapb.ID {
	if m.coordinatorID != nil {
		return m.coordinatorID
	}
	return testCoordinatorID
}

var testShardInitShardKey = &clustermetadatapb.ShardKey{
	Database:   "testdb",
	TableGroup: "default",
	Shard:      "0",
}

func makePoolerState(cell, name, db, tableGroup, shard string, initialized bool, cohortMembers []*clustermetadatapb.ID) *store.Pooler {
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}
	return store.NewPooler(&multiorchdatapb.PoolerHealthState{
		LastSeen: timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: initialized,
		},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{
						CohortMembers: cohortMembers,
					},
				},
			},
		},
		Multipooler: &clustermetadatapb.Multipooler{
			Id: id,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   db,
				TableGroup: tableGroup,
				Shard:      shard,
			},
		},
	}, nil)
}

func newTestAction(t *testing.T, coord shardInitCoordinator, poolerStore *store.PoolerCache, ts topoclient.Store) *ShardInitAction {
	t.Helper()
	if ts == nil {
		ts = memorytopo.NewServer(t.Context(), "cell1")
	}
	return NewShardInitAction(config.NewTestConfig(), coord, poolerStore, ts, slog.Default())
}

func newPoolerStore(t *testing.T) *store.PoolerCache {
	t.Helper()
	return store.NewTestCache(t)
}

// --- Interface / metadata ---

func TestShardInitAction_Metadata(t *testing.T) {
	action := NewShardInitAction(config.NewTestConfig(), nil, nil, nil, slog.Default())
	m := action.Metadata()
	assert.Equal(t, "ShardInit", m.Name)
	assert.True(t, m.Retryable)
	assert.Equal(t, 2*timeouts.RuleWriteTimeout+5*time.Second, m.Timeout)
}

func TestShardInitAction_RequiresHealthyLeader(t *testing.T) {
	assert.False(t, NewShardInitAction(config.NewTestConfig(), nil, nil, nil, slog.Default()).RequiresHealthyLeader())
}

func TestShardInitAction_GracePeriod(t *testing.T) {
	action := NewShardInitAction(config.NewTestConfig(), nil, nil, nil, slog.Default())
	assert.Nil(t, action.GracePeriod(), "bootstrapping an uninitialized shard has nothing to defer for")
}

// --- getInitializedPoolers ---

func TestShardInitAction_GetInitializedPoolers_FiltersByShard(t *testing.T) {
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "other", "otherdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "shard1", "testdb", "default", "1", true, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testShardInitShardKey)

	assert.False(t, cohortEstablished)
	require.Len(t, initialized, 2)
	names := []string{initialized[0].Health().Multipooler.Id.Name, initialized[1].Health().Multipooler.Id.Name}
	assert.ElementsMatch(t, []string{"p1", "p2"}, names)
}

func TestShardInitAction_GetInitializedPoolers_ExcludesStaleObservation(t *testing.T) {
	// p1 looked initialized once but its observation is now well past
	// store.DefaultObservationFreshness — it must not be trusted for the
	// bootstrap cohort just because the durable IsInitialized flag is still true.
	ps := newPoolerStore(t)
	stale := makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil)
	stale.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
		h.LastSeen = timestamppb.New(time.Now().Add(-time.Hour))
	})
	store.SeedCache(t, ps, stale)
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testShardInitShardKey)

	assert.False(t, cohortEstablished)
	require.Len(t, initialized, 1)
	assert.Equal(t, "p2", initialized[0].Health().Multipooler.Id.Name)
}

func TestShardInitAction_GetInitializedPoolers_CohortAlreadyEstablished(t *testing.T) {
	ps := newPoolerStore(t)
	existingCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
	}
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, existingCohort))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testShardInitShardKey)

	assert.True(t, cohortEstablished)
	assert.Nil(t, initialized)
}

func TestShardInitAction_GetInitializedPoolers_CohortEstablishedViaUndecidedProposal(t *testing.T) {
	// The cohort is only reflected on p1's outstanding proposal (e.g. a
	// self-promotion that reached WAL but wasn't marked decided) — not its
	// decision. getInitializedPoolers must still recognize the cohort as
	// established via PossiblyUndecidedRule, not just the decision.
	ps := newPoolerStore(t)
	existingCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
	}
	p1 := store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Status: &multipoolermanagerdatapb.Status{IsInitialized: true},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}},
					Proposal: &clustermetadatapb.ShardRule{
						RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
						CohortMembers: existingCohort,
					},
				},
			},
		},
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
			ShardKey: testShardInitShardKey,
		},
	}, nil)
	store.SeedCache(t, ps, p1)
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testShardInitShardKey)

	assert.True(t, cohortEstablished)
	assert.Nil(t, initialized)
}

func TestShardInitAction_GetInitializedPoolers_NotYetInitialized(t *testing.T) {
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", false, nil))

	action := newTestAction(t, nil, ps, nil)
	initialized, cohortEstablished := action.getInitializedPoolers(testShardInitShardKey)

	assert.False(t, cohortEstablished)
	assert.Empty(t, initialized)
}

// --- Execute ---

func TestShardInitAction_Execute_NoInitializedPoolers(t *testing.T) {
	ps := newPoolerStore(t)
	// Pooler exists but is not initialized
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", false, nil))

	action := newTestAction(t, nil, ps, nil)
	err := action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no initialized poolers found for shard")
}

func TestShardInitAction_Execute_CohortAlreadyEstablished(t *testing.T) {
	ps := newPoolerStore(t)
	existingCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
	}
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, existingCohort))

	coord := &mockCoordinator{}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})

	require.NoError(t, err)
	assert.Empty(t, coord.appointedCohort, "AppointInitialLeader must not be called when cohort is already established")
}

func TestShardInitAction_Execute_GetBootstrapPolicyError(t *testing.T) {
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicyErr: errors.New("etcd unreachable")}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load durability policy")
	assert.Contains(t, err.Error(), "etcd unreachable")
}

func TestShardInitAction_Execute_InsufficientInitializedPoolers(t *testing.T) {
	ps := newPoolerStore(t)
	// Only 1 initialized pooler but policy requires 2
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient initialized poolers")
	assert.Empty(t, coord.appointedCohort)
}

func TestShardInitAction_Execute_NotFailureSafe(t *testing.T) {
	// 2 poolers satisfy AT_LEAST_N(2) but can never survive losing either one
	// of them — must be rejected rather than bootstrapped into a cohort with
	// no redundancy margin. Resolved only by starting a third pooler.
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	action := newTestAction(t, coord, ps, nil)
	err := action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "aren't failure-safe")
	assert.Empty(t, coord.appointedCohort)
}

func TestShardInitAction_Execute_NotFailureSafe_AllowedByConfig(t *testing.T) {
	// Same 2-pooler, non-failure-safe setup as TestShardInitAction_Execute_NotFailureSafe,
	// but with the override enabled — e.g. a test deliberately exercising a
	// minimum-size cohort. Must proceed rather than reject.
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	cfg := config.NewTestConfig(config.WithAllowUnsafeInitialCohort(true))
	action := NewShardInitAction(cfg, coord, ps, memorytopo.NewServer(t.Context(), "cell1"), slog.Default())
	err := action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})

	require.NoError(t, err)
	require.Len(t, coord.appointedCohort, 2)
}

func TestShardInitAction_Execute_Success(t *testing.T) {
	// 3 poolers: AT_LEAST_N(2) is satisfied by 2, but only 3 is failure-safe
	// (survives losing any single member).
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p3", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")
	action := newTestAction(t, coord, ps, ts)

	err := action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})
	require.NoError(t, err)

	require.Len(t, coord.appointedCohort, 3)
	names := []string{coord.appointedCohort[0].Multipooler.Id.Name, coord.appointedCohort[1].Multipooler.Id.Name, coord.appointedCohort[2].Multipooler.Id.Name}
	assert.ElementsMatch(t, []string{"p1", "p2", "p3"}, names)
	assert.Equal(t, testShardInitShardKey, coord.appointedShardKey)
}

func TestShardInitAction_Execute_ClaimAfterCrash(t *testing.T) {
	// Same coordinator already claimed but crashed before appointing.
	// On retry it should win again and proceed with the committed cohort
	// from etcd, NOT the current pooler store contents.
	ps := newPoolerStore(t)
	// Pooler store has all five poolers, but the committed cohort only has prior-p1/p2/p3.
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "prior-p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "prior-p2", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "prior-p3", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")

	// Pre-write the claim with the same coordinator ID but different pooler names
	// than p1/p2. The committed cohort from etcd should take priority over what
	// the current pooler store would freshly select. 3 members so the committed
	// cohort is failure-safe on its own, same as any other bootstrap.
	priorCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "prior-p1"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "prior-p2"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "prior-p3"},
	}
	won, _, err := ts.ClaimShardInitialization(t.Context(), testShardInitShardKey, testCoordinatorID, priorCohort)
	require.NoError(t, err)
	require.True(t, won)

	action := newTestAction(t, coord, ps, ts)
	err = action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})
	require.NoError(t, err)

	// The appointed cohort should use the etcd-committed names, not the pooler store names.
	require.Len(t, coord.appointedCohort, 3)
	names := []string{coord.appointedCohort[0].Multipooler.Id.Name, coord.appointedCohort[1].Multipooler.Id.Name, coord.appointedCohort[2].Multipooler.Id.Name}
	assert.ElementsMatch(t, []string{"prior-p1", "prior-p2", "prior-p3"}, names)
}

func TestShardInitAction_Execute_CommittedCohortNotFailureSafe(t *testing.T) {
	// The freshly-computed cohort (p1/p2/p3) is failure-safe and passes the
	// first check, but the claim was already committed with a smaller,
	// non-failure-safe cohort (p1/p2). ClaimShardInitialization returns that
	// committed cohort regardless, and it's fixed forever — no new pooler can
	// help, so the error should point at an externally-certified rule change
	// rather than suggesting to add one.
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p3", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")

	committedCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p2"},
	}
	won, _, err := ts.ClaimShardInitialization(t.Context(), testShardInitShardKey, testCoordinatorID, committedCohort)
	require.NoError(t, err)
	require.True(t, won)

	action := newTestAction(t, coord, ps, ts)
	err = action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})

	require.EqualError(t, err,
		"committed cohort (2 members, all reachable) satisfies the durability policy but isn't failure-safe and is fixed for this shard's init claim; bootstrap via an externally-certified rule change (multiadmin) instead")
	assert.Empty(t, coord.appointedCohort)
}

func TestShardInitAction_Execute_CommittedCohortMemberUnreachable(t *testing.T) {
	// The committed cohort (prior-p1/p2/p3) is failure-safe on its own, but
	// only prior-p2/p3 are currently reachable in the pooler store — enough to
	// satisfy the durability policy but not failure-safety. prior-p1 might
	// still come back, so the error should say to wait rather than claim the
	// cohort is permanently stuck.
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "prior-p2", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "prior-p3", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{bootstrapPolicy: topoclient.AtLeastN(2)}
	ts := memorytopo.NewServer(t.Context(), "cell1")

	committedCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "prior-p1"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "prior-p2"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "prior-p3"},
	}
	won, _, err := ts.ClaimShardInitialization(t.Context(), testShardInitShardKey, testCoordinatorID, committedCohort)
	require.NoError(t, err)
	require.True(t, won)

	action := newTestAction(t, coord, ps, ts)
	err = action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})

	require.EqualError(t, err,
		"committed cohort (2 of 3 reachable) satisfies the durability policy but isn't failure-safe while a member is unreachable; waiting for it to return")
	assert.Empty(t, coord.appointedCohort)
}

func TestShardInitAction_Execute_ClaimLostToDifferentCoordinator(t *testing.T) {
	// A different coordinator already claimed this shard. We should back off
	// without calling AppointInitialLeader.
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p3", "testdb", "default", "0", true, nil))

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
	err = action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})

	require.NoError(t, err)
	assert.Empty(t, coord.appointedCohort, "AppointInitialLeader must not be called when another coordinator owns the claim")
}

func TestShardInitAction_Execute_AppointInitialLeaderError(t *testing.T) {
	ps := newPoolerStore(t)
	store.SeedCache(t, ps, makePoolerState("cell1", "p1", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p2", "testdb", "default", "0", true, nil))
	store.SeedCache(t, ps, makePoolerState("cell1", "p3", "testdb", "default", "0", true, nil))

	coord := &mockCoordinator{
		bootstrapPolicy:         topoclient.AtLeastN(2),
		appointInitialLeaderErr: errors.New("consensus failed"),
	}
	action := newTestAction(t, coord, ps, memorytopo.NewServer(t.Context(), "cell1"))

	err := action.Execute(t.Context(), types.RecheckedProblem{Problem: types.Problem{ShardKey: testShardInitShardKey}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to appoint initial leader")
	assert.Contains(t, err.Error(), "consensus failed")
}
