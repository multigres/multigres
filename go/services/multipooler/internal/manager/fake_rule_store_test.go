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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
)

// testBootstrapPolicy returns a minimal valid durability policy for use in tests.
func testBootstrapPolicy() *clustermetadatapb.DurabilityPolicy {
	return &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "AT_LEAST_2",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
		RequiredCount: 2,
	}
}

// noopSyncStandbyManager is a consensus.SyncStandbyManager test double that does nothing.
type noopSyncStandbyManager struct{}

func (noopSyncStandbyManager) SetPolicy(_ context.Context, _ commonconsensus.PolicyWithCohort) error {
	return nil
}

func (noopSyncStandbyManager) Clear(_ context.Context) error {
	return nil
}

func (noopSyncStandbyManager) NeedsApply(_ context.Context, _ commonconsensus.PolicyWithCohort) (bool, error) {
	return false, nil
}

// fakeRuleStore is a test double for consensus.RuleStorer that returns a preset position
// without hitting postgres. Both ObservePosition and UpdateRule return pos
// (or observeErr/updateErr when set). UpdateRule records all calls in updates.
//
// If posSequence is non-empty, ObservePosition returns positions from the
// sequence in order (consuming each entry), then falls back to pos once
// the sequence is exhausted. This is useful for simulating a position that
// changes between calls (e.g., Recruit's sanity check vs. post-stop check).
type fakeRuleStore struct {
	mu           sync.Mutex
	pos          *clustermetadatapb.PoolerPosition
	posSequence  []*clustermetadatapb.PoolerPosition
	lsn          *clustermetadatapb.PoolerLsn
	observeCalls int
	observeErr   error
	// observeErrAtCall, if non-zero, makes ObservePosition return observeErr
	// only on the call whose 1-indexed number matches it, succeeding on every
	// other call — for exercising a failure at one specific ObservePosition
	// call site among several in the same code path.
	observeErrAtCall   int
	updateErr          error
	updateErrAfterHook error
	updates            []*consensus.RuleUpdateBuilder
	inconsistentGUC    bool
	reconcileGUCCalled bool
	reconcileGUCErr    error
	clearSyncCalled    bool
	clearSyncErr       error
}

func (f *fakeRuleStore) observePositionCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.observeCalls
}

// armObserveErrAfterCalls resets the ObservePosition call counter and arms
// observeErr to fire on the nth ObservePosition call from this point on —
// use right before invoking the operation under test, so unrelated
// ObservePosition calls made during test/manager setup don't throw off the
// count.
func (f *fakeRuleStore) armObserveErrAfterCalls(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeCalls = 0
	f.observeErrAtCall = n
	f.observeErr = err
}

func (f *fakeRuleStore) ObservePosition(_ context.Context) (*clustermetadatapb.PoolerPosition, *clustermetadatapb.PoolerLsn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeCalls++
	// observeErrAtCall, when armed, fully overrides the usual observeErr
	// semantics: only the targeted call fails; every other call succeeds
	// regardless of observeErr (which armObserveErrAfterCalls also sets, for
	// the targeted call to return).
	if f.observeErrAtCall != 0 {
		if f.observeCalls == f.observeErrAtCall {
			return nil, nil, f.observeErr
		}
		return f.pos, f.lsnOrDefault(f.pos), nil
	}
	if len(f.posSequence) > 0 {
		pos := f.posSequence[0]
		f.posSequence = f.posSequence[1:]
		if pos == nil && f.observeErr == nil {
			return nil, nil, errors.New("fakeRuleStore: no position set")
		}
		return pos, f.lsnOrDefault(pos), f.observeErr
	}
	if f.pos == nil && f.observeErr == nil {
		return nil, nil, errors.New("fakeRuleStore: no position set")
	}
	return f.pos, f.lsnOrDefault(f.pos), f.observeErr
}

// lsnOrDefault returns f.lsn when the test explicitly set it (for exercising
// checkRecruitLsnDrift), or otherwise a PoolerLsn synthesized from pos's
// FlushedLsn — most tests don't care about the flushed/applied distinction
// and just want a consistent, parseable "current LSN".
func (f *fakeRuleStore) lsnOrDefault(pos *clustermetadatapb.PoolerPosition) *clustermetadatapb.PoolerLsn {
	if f.lsn != nil {
		return f.lsn
	}
	return &clustermetadatapb.PoolerLsn{FlushedLsn: pos.GetFlushedLsn(), AppliedLsn: pos.GetFlushedLsn()}
}

func (f *fakeRuleStore) CreateRuleTables(_ context.Context, _ *clustermetadatapb.DurabilityPolicy, _ *clustermetadatapb.ID) error {
	return nil
}

func (f *fakeRuleStore) CachedPosition() *clustermetadatapb.PoolerPosition {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pos
}

func (f *fakeRuleStore) UpdateRule(ctx context.Context, update *consensus.RuleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
	f.mu.Lock()
	f.updates = append(f.updates, update)
	updateErr := f.updateErr
	updateErrAfterHook := f.updateErrAfterHook
	pos := f.pos
	f.mu.Unlock()

	if updateErr != nil {
		return nil, updateErr
	}
	// Mirror the real rule store's pre-hook durability policy validation.
	if dp := update.GetDurabilityPolicy(); dp != nil {
		if dp.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN || dp.RequiredCount <= 0 {
			return nil, fmt.Errorf("durability policy has missing or invalid fields: quorum_type=%v required_count=%d",
				dp.QuorumType, dp.RequiredCount)
		}
	}
	if hook := update.GetPromotionHook(); hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
	if updateErrAfterHook != nil {
		return nil, updateErrAfterHook
	}

	// Mirror the real rule store: a successful write makes the written rule the
	// new observable current position. Consumers derive from CachedPosition (e.g.
	// CachedConsensusStatus, which the StateManager reads to compute the routing
	// role after a promote), so the fake must advance it or the derived state
	// would never reflect the new rule. The LSN is carried over from the prior
	// position (the write does not move the WAL cursor here).
	newPos := &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			LeaderId:      update.GetLeaderID(),
			CoordinatorId: update.GetCoordinatorID(),
			RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: update.GetTermNumber()},
			CohortMembers: update.GetCohortMembers(),
		}},
	}
	if pos != nil {
		newPos.FlushedLsn = pos.GetFlushedLsn()
	}
	f.mu.Lock()
	f.pos = newPos
	f.mu.Unlock()
	return newPos, nil
}

func (f *fakeRuleStore) HasInconsistentGUC(_ context.Context) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inconsistentGUC
}

func (f *fakeRuleStore) ReconcileGUC(_ context.Context, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconcileGUCCalled = true
	return f.reconcileGUCErr
}

func (f *fakeRuleStore) ClearSyncStandby(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clearSyncCalled = true
	return f.clearSyncErr
}

// assertPromoteRecorded asserts that exactly one UpdateRule call was made with
// eventType "promotion" and returns the update so callers can inspect its fields.
func (f *fakeRuleStore) assertPromoteRecorded(t *testing.T) *consensus.RuleUpdateBuilder {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Len(t, f.updates, 1, "expected exactly one UpdateRule call for promotion")
	assert.Equal(t, "promotion", f.updates[0].GetEventType())
	return f.updates[0]
}
