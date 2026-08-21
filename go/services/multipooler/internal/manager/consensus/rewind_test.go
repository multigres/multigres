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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus/consensustest"
)

func rewindTestSelfID() *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "self"}
}

// recordedPrimaryWithLeader builds a ReplicationPrimary naming leader with the
// given contact address, as RecordTermPrimary would store it.
func recordedPrimaryWithLeader(leader *clustermetadatapb.ID, addr *clustermetadatapb.PoolerAddress) *clustermetadatapb.ReplicationPrimary {
	return &clustermetadatapb.ReplicationPrimary{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
			LeaderId:   leader,
		}},
		Primary: addr,
	}
}

func TestConsensusManager_RewindTarget(t *testing.T) {
	self := rewindTestSelfID()
	other := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "other"}

	tests := []struct {
		name       string
		record     *clustermetadatapb.ReplicationPrimary // nil = nothing recorded
		revocation *clustermetadatapb.TermRevocation     // nil = no revocation seeded
		wantOK     bool
		wantHost   string
		wantPort   int32
	}{
		{
			name:     "different_leader_is_target",
			record:   recordedPrimaryWithLeader(other, primaryAt("other", "other-host", 5432)),
			wantOK:   true,
			wantHost: "other-host",
			wantPort: 5432,
		},
		{
			// The recorded leader is us: nothing to diverge from.
			name:   "leader_is_self_no_target",
			record: recordedPrimaryWithLeader(self, primaryAt("self", "self-host", 5432)),
			wantOK: false,
		},
		{
			name:   "no_recorded_primary_no_target",
			record: nil,
			wantOK: false,
		},
		{
			// A recorded primary whose rule names no leader is not actionable.
			name:   "no_leader_in_rule_no_target",
			record: recordedPrimaryWithLeader(nil, primaryAt("other", "other-host", 5432)),
			wantOK: false,
		},
		{
			// A recorded leader with no usable contact address is not actionable.
			name: "no_address_no_target",
			record: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
					LeaderId:   other,
				}},
			},
			wantOK: false,
		},
		{
			// The recorded rule (term 5) is revoked by our term revocation, so we
			// must not rewind toward it — we promised to abandon that leader.
			name:   "revoked_rule_no_target",
			record: recordedPrimaryWithLeader(other, primaryAt("other", "other-host", 5432)),
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm: 6,
				OutgoingRule:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.revocation != nil {
				consensustest.SeedTerm(t, dir, tt.revocation)
			}
			promises := NewConsensusPromises(dir, self)
			_, err := promises.Load()
			require.NoError(t, err)
			cm := NewManagerForTesting(t, self, promises, nil, nil)
			if tt.record != nil {
				require.NoError(t, cm.RecordTermPrimary(actionLockCtx(t), tt.record))
			}
			host, port, ok := cm.RewindTarget()
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantPort, port)
		})
	}
}

func TestConsensusManager_RewindBackoff(t *testing.T) {
	cm := NewManagerForTesting(t, rewindTestSelfID(), nil, nil, nil)

	// Fresh: no attempt recorded yet, so the backoff has elapsed and a rewind may
	// be attempted immediately.
	assert.True(t, cm.RewindBackoffElapsed(), "fresh manager should report backoff elapsed")

	// After recording an attempt, the next-attempt window is pushed into the
	// future (floored at the minimum), so the backoff has not elapsed.
	cm.RecordRewindAttempt()
	assert.False(t, cm.RewindBackoffElapsed(), "backoff should not have elapsed right after an attempt")

	// A second attempt keeps it pending (exponential growth above the floor).
	cm.RecordRewindAttempt()
	assert.False(t, cm.RewindBackoffElapsed(), "backoff should still be pending after a second attempt")

	// Reset (after a successful rewind) clears the window.
	cm.ResetRewindBackoff()
	assert.True(t, cm.RewindBackoffElapsed(), "reset should make the backoff elapsed again")
}
