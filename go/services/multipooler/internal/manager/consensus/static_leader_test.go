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

package consensus

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

func TestStaticLeaderStatus_IsActiveLeader(t *testing.T) {
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}

	status := staticLeaderStatus(id)

	assert.True(t, commonconsensus.IsActiveLeader(status))
	assert.Equal(t, commonconsensus.ConsensusRoleLeader, commonconsensus.SelfConsensusRole(status))
}

func TestStaticLeaderStatus_OtherPoolerIsNotLeader(t *testing.T) {
	self := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	other := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"}

	status := staticLeaderStatus(self)
	status.Id = other

	assert.False(t, commonconsensus.IsActiveLeader(status))
}

func newTestConsensusManager(t *testing.T, id *clustermetadatapb.ID, static bool) *ConsensusManager {
	t.Helper()
	promises := NewConsensusPromises(t.TempDir(), id)
	rules := &fakeRuleStoreForStaticLeaderTest{}
	if static {
		return NewStaticLeaderManagerForTesting(t, id, promises, rules, nil)
	}
	return NewManagerForTesting(t, id, promises, rules, nil)
}

func TestCachedConsensusStatus_StaticLeader(t *testing.T) {
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "self"}

	cm := newTestConsensusManager(t, id, false)
	assert.False(t, commonconsensus.IsActiveLeader(cm.CachedConsensusStatus()), "without a static leader, no rule is cached, so no one is leader")

	static := newTestConsensusManager(t, id, true)
	assert.True(t, commonconsensus.IsActiveLeader(static.CachedConsensusStatus()))
}

func TestStartsAsPrimary(t *testing.T) {
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "self"}

	assert.False(t, newTestConsensusManager(t, id, false).StartsAsPrimary())
	assert.True(t, newTestConsensusManager(t, id, true).StartsAsPrimary())
}

func TestLeaderInRecoveryAction(t *testing.T) {
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "self"}

	tests := []struct {
		name   string
		static bool
		mode   pgmode.Mode
		want   LeaderRecoveryAction
	}{
		{
			// The bootstrap restore leaves postgres a standby; nothing else will promote it.
			name:   "static leader with standby self-promotes",
			static: true,
			mode:   pgmode.InRecovery,
			want:   LeaderRecoveryActionSelfPromote,
		},
		{
			// The mode could not be read this tick: do not act on a guess.
			name:   "static leader with unknown mode waits",
			static: true,
			mode:   pgmode.Unknown,
			want:   LeaderRecoveryActionNone,
		},
		{
			// Without a static leader and with no prior resignation, signal one so a
			// coordinator re-elects, as in Multigres.
			name:   "consensus pooler with standby resigns",
			static: false,
			mode:   pgmode.InRecovery,
			want:   LeaderRecoveryActionResign,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := newTestConsensusManager(t, id, tt.static)
			assert.Equal(t, tt.want, cm.LeaderInRecoveryAction(tt.mode))
		})
	}
}

func TestResignGuard(t *testing.T) {
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "self"}

	assert.NoError(t, newTestConsensusManager(t, id, false).ResignGuard())

	err := newTestConsensusManager(t, id, true).ResignGuard()
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "static leader")
}

// fakeRuleStoreForStaticLeaderTest is a RuleStorer with no cached position, so
// CachedConsensusStatus's non-static branch returns nil (no one is leader) —
// this test file only needs that one behavior, not the full fakeRuleStore
// used by manager_test.go.
type fakeRuleStoreForStaticLeaderTest struct {
	RuleStorer
}

func (fakeRuleStoreForStaticLeaderTest) CachedPosition() *clustermetadatapb.PoolerPosition {
	return nil
}
