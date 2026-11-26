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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// switchableConsensus is a StateManager consensus injector whose committed
// rule names testPoolerID as the leader at a term the test can change.
type switchableConsensus struct {
	term atomic.Int64
}

func (c *switchableConsensus) status() *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: testPoolerID,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: c.term.Load()},
				LeaderId:   testPoolerID,
			}},
		},
	}
}

// hookRecorder is a beforeServingPrimary hook that records its calls and
// fails while err is set.
type hookRecorder struct {
	mu      sync.Mutex
	targets []servingstate.State
	err     error
}

func (h *hookRecorder) hook(_ context.Context, target servingstate.State) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.targets = append(h.targets, target)
	return h.err
}

func (h *hookRecorder) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.targets)
}

func (h *hookRecorder) setErr(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.err = err
}

func (h *hookRecorder) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.targets = nil
}

func mutateTo(mode pgmode.Mode, status clustermetadatapb.PoolerServingStatus) func(*servingStateMutation) {
	return func(s *servingStateMutation) {
		s.PostgresMode = mode
		s.ServingStatus = status
	}
}

// TestBeforeServingPrimary_RunsOnlyOnTransitionToServingPrimary pins when the
// hook runs: on a transition whose target is PRIMARY and SERVING from a state
// that was not both, and on nothing else.
func TestBeforeServingPrimary_RunsOnlyOnTransitionToServingPrimary(t *testing.T) {
	const (
		serving  = clustermetadatapb.PoolerServingStatus_SERVING
		draining = clustermetadatapb.PoolerServingStatus_DRAINING
		disabled = clustermetadatapb.PoolerServingStatus_DISABLED
	)
	tests := []struct {
		name string
		// setup runs before the transition under test; nil leaves nothing
		// fanned out yet.
		setup    func(*servingStateMutation)
		bumpTerm bool
		target   func(*servingStateMutation)
		wantRun  bool
	}{
		{name: "from nothing to primary serving", target: mutateTo(pgmode.Primary, serving), wantRun: true},
		{name: "from replica to primary serving", setup: mutateTo(pgmode.InRecovery, serving), target: mutateTo(pgmode.Primary, serving), wantRun: true},
		{name: "from primary draining to primary serving", setup: mutateTo(pgmode.Primary, draining), target: mutateTo(pgmode.Primary, serving), wantRun: true},
		{name: "from primary disabled to primary serving", setup: mutateTo(pgmode.Primary, disabled), target: mutateTo(pgmode.Primary, serving), wantRun: true},
		{name: "primary serving with a new rule", setup: mutateTo(pgmode.Primary, serving), bumpTerm: true, target: mutateTo(pgmode.Primary, serving)},
		{name: "to replica serving", target: mutateTo(pgmode.InRecovery, serving)},
		{name: "to primary draining", target: mutateTo(pgmode.Primary, draining)},
		{name: "to primary disabled", target: mutateTo(pgmode.Primary, disabled)},
		{name: "from primary serving to draining", setup: mutateTo(pgmode.Primary, serving), target: mutateTo(pgmode.Primary, draining)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := &switchableConsensus{}
			cs.term.Store(1)
			comp := &testComponent{}
			ssm := NewStateManager(newTestLogger(), newTestRecord(clustermetadatapb.PoolerType_REPLICA, disabled), cs.status, comp)
			h := &hookRecorder{}
			ssm.SetBeforeServingPrimary(h.hook)

			ctx := newActionLockedCtx(t)
			if tt.setup != nil {
				require.NoError(t, ssm.Mutate(ctx, tt.setup))
			}
			h.reset()
			fanOuts := comp.callCount
			if tt.bumpTerm {
				cs.term.Store(2)
			}
			require.NoError(t, ssm.Mutate(ctx, tt.target))

			if !tt.wantRun {
				assert.Zero(t, h.calls())
				if tt.bumpTerm {
					assert.Equal(t, fanOuts+1, comp.callCount, "a rule-only bump still fans out")
				}
				return
			}
			require.Equal(t, 1, h.calls())
			got := h.targets[0]
			assert.Equal(t, servingstate.RoutingRolePrimary, got.Routing.Role)
			assert.Equal(t, serving, got.ServingStatus)
			assert.Equal(t, int64(1), got.Routing.Rule.GetCoordinatorTerm(), "the hook receives the target's rule")
			assert.Equal(t, fanOuts+1, comp.callCount, "the transition fans out after the hook succeeds")
		})
	}
}

// countingHandler counts the records logged at each level whose message
// contains a substring.
type countingHandler struct {
	mu     sync.Mutex
	counts map[slog.Level]int
	substr string
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.substr) {
		h.mu.Lock()
		h.counts[r.Level]++
		h.mu.Unlock()
	}
	return nil
}
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }

func (h *countingHandler) count(level slog.Level) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[level]
}

// TestBeforeServingPrimary_FailureBlocksUntilFixDriftSucceeds checks that a
// failing hook changes nothing, that the monitor's drift check still sees the
// drift so it retries, and that a later fixDrift completes the transition once
// the hook succeeds.
func TestBeforeServingPrimary_FailureBlocksUntilFixDriftSucceeds(t *testing.T) {
	logs := &countingHandler{counts: map[slog.Level]int{}, substr: "not serving as primary"}
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING)
	ssm := NewStateManager(slog.New(logs), r, selfLeaderConsensusStatus, comp)
	h := &hookRecorder{}
	ssm.SetBeforeServingPrimary(h.hook)
	ctx := newActionLockedCtx(t)

	// Serving as a replica first, as a pooler that opened in recovery.
	require.NoError(t, ssm.Mutate(ctx, mutateTo(pgmode.InRecovery, clustermetadatapb.PoolerServingStatus_SERVING)))
	require.Equal(t, 1, comp.callCount)
	require.False(t, ssm.hasDrift(pgmode.InRecovery, false, false))

	// Promotion: postgres is out of recovery, the hook fails.
	h.setErr(errors.New("catalog lock timeout"))
	err := ssm.Mutate(ctx, mutateTo(pgmode.Primary, clustermetadatapb.PoolerServingStatus_SERVING))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not serving as primary: catalog lock timeout")

	// Nothing moved: no fan-out, the replica state is still the last one
	// delivered, the record and the cached recovery mode are unchanged.
	assert.Equal(t, 1, comp.callCount)
	assert.Equal(t, servingstate.RoutingRoleReplica, ssm.lastFannedOut.Routing.Role)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, r.Type())
	assert.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA, ssm.RoutingRole())
	// The monitor still sees the drift, which is what makes it retry.
	assert.True(t, ssm.hasDrift(pgmode.Primary, false, false))

	// The monitor retries every tick; the same failure is logged once.
	for range 3 {
		require.Error(t, ssm.fixDrift(ctx, pgmode.Primary, false, false))
	}
	assert.Equal(t, 1, logs.count(slog.LevelError), "a repeated failure is logged once")
	h.setErr(errors.New("catalog_state does not exist"))
	require.Error(t, ssm.fixDrift(ctx, pgmode.Primary, false, false))
	assert.Equal(t, 2, logs.count(slog.LevelError), "a different failure is logged again")
	assert.Equal(t, 1, comp.callCount)
	assert.Equal(t, 5, h.calls())

	// The hook succeeds: the next drift fix completes the transition.
	h.setErr(nil)
	require.True(t, ssm.hasDrift(pgmode.Primary, false, false))
	require.NoError(t, ssm.fixDrift(ctx, pgmode.Primary, false, false))
	assert.Equal(t, 2, comp.callCount)
	assert.Equal(t, servingstate.RoutingRolePrimary, comp.lastRole)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, comp.lastStatus)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.False(t, ssm.hasDrift(pgmode.Primary, false, false))
	assert.Equal(t, 6, h.calls())
}

// TestBeforeServingPrimary_HoldReleaseRetries covers the release of a serving
// hold: PRIMARY + DRAINING back to SERVING runs the hook, and a failure leaves
// the pooler draining with the drift still reported.
func TestBeforeServingPrimary_HoldReleaseRetries(t *testing.T) {
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp)
	h := &hookRecorder{}
	ssm.SetBeforeServingPrimary(h.hook)
	ctx := newActionLockedCtx(t)

	require.NoError(t, ssm.Mutate(ctx, mutateTo(pgmode.Primary, clustermetadatapb.PoolerServingStatus_SERVING)))
	require.NoError(t, ssm.ForceMigrationHold(ctx))
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, r.ServingStatus())

	h.setErr(errors.New("boom"))
	require.Error(t, ssm.ReconcileMigrationHold(ctx, false))
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, r.ServingStatus())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, comp.lastStatus)
	assert.True(t, ssm.hasDrift(pgmode.Primary, false, false))

	h.setErr(nil)
	require.NoError(t, ssm.fixDrift(ctx, pgmode.Primary, false, false))
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
	assert.Equal(t, 3, h.calls())
}
