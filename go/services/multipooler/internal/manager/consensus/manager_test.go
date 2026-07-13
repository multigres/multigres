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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
)

// actionLockCtx returns a context holding a freshly-acquired action lock, for
// tests that exercise ConsensusManager mutators (which assert the lock is held).
func actionLockCtx(t *testing.T) context.Context {
	t.Helper()
	al := actionlock.NewActionLock()
	ctx, err := al.Acquire(t.Context(), "test")
	require.NoError(t, err)
	t.Cleanup(func() { al.Release(ctx) })
	return ctx
}

func ruleAt(term, subterm int64) *clustermetadatapb.ShardRule {
	return &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{
			CoordinatorTerm: term,
			LeaderSubterm:   subterm,
		},
	}
}

func primaryAt(name, host string, port int32) *clustermetadatapb.PoolerAddress {
	return &clustermetadatapb.PoolerAddress{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		},
		Host:         host,
		PostgresPort: port,
	}
}

func TestRecordTermPrimary(t *testing.T) {
	tests := []struct {
		name                 string
		seedRule             *clustermetadatapb.ShardRule
		seedPrimary          *clustermetadatapb.PoolerAddress
		callRule             *clustermetadatapb.ShardRule
		callPrimary          *clustermetadatapb.PoolerAddress
		wantRule             *clustermetadatapb.ShardRule
		wantPrimary          *clustermetadatapb.PoolerAddress
		wantPrimaryUnchanged bool // primary should still equal seedPrimary
	}{
		{
			name:        "FirstSetPrimary_RecordsBoth",
			callRule:    ruleAt(5, 0),
			callPrimary: primaryAt("p1", "hostA", 5432),
			wantRule:    ruleAt(5, 0),
			wantPrimary: primaryAt("p1", "hostA", 5432),
		},
		{
			name:        "HigherRule_UpdatesBoth",
			seedRule:    ruleAt(5, 0),
			seedPrimary: primaryAt("p1", "hostA", 5432),
			callRule:    ruleAt(7, 0),
			callPrimary: primaryAt("p2", "hostB", 5433),
			wantRule:    ruleAt(7, 0),
			wantPrimary: primaryAt("p2", "hostB", 5433),
		},
		{
			name:        "HigherSubterm_UpdatesBoth",
			seedRule:    ruleAt(5, 2),
			seedPrimary: primaryAt("p1", "hostA", 5432),
			callRule:    ruleAt(5, 3),
			callPrimary: primaryAt("p1b", "hostA2", 5432),
			wantRule:    ruleAt(5, 3),
			wantPrimary: primaryAt("p1b", "hostA2", 5432),
		},
		{
			name:        "LowerRule_IgnoredEntirely",
			seedRule:    ruleAt(7, 0),
			seedPrimary: primaryAt("p2", "hostB", 5433),
			callRule:    ruleAt(5, 0),
			callPrimary: primaryAt("p1", "hostA", 5432),
			wantRule:    ruleAt(7, 0),
			wantPrimary: primaryAt("p2", "hostB", 5433),
		},
		{
			name:        "SameRule_DifferentHost_UpdatesPrimary",
			seedRule:    ruleAt(7, 0),
			seedPrimary: primaryAt("p1", "hostA", 5432),
			callRule:    ruleAt(7, 0),
			callPrimary: primaryAt("p1", "hostA-renamed", 5432),
			wantRule:    ruleAt(7, 0),
			wantPrimary: primaryAt("p1", "hostA-renamed", 5432),
		},
		{
			name:        "SameRule_DifferentPort_UpdatesPrimary",
			seedRule:    ruleAt(7, 0),
			seedPrimary: primaryAt("p1", "hostA", 5432),
			callRule:    ruleAt(7, 0),
			callPrimary: primaryAt("p1", "hostA", 6432),
			wantRule:    ruleAt(7, 0),
			wantPrimary: primaryAt("p1", "hostA", 6432),
		},
		{
			name:                 "SameRule_SamePrimary_NoOp",
			seedRule:             ruleAt(7, 0),
			seedPrimary:          primaryAt("p1", "hostA", 5432),
			callRule:             ruleAt(7, 0),
			callPrimary:          primaryAt("p1", "hostA", 5432),
			wantRule:             ruleAt(7, 0),
			wantPrimary:          primaryAt("p1", "hostA", 5432),
			wantPrimaryUnchanged: true,
		},
		{
			name:        "HigherRule_NilPrimary_KeepsExistingPrimary",
			seedRule:    ruleAt(5, 0),
			seedPrimary: primaryAt("p1", "hostA", 5432),
			callRule:    ruleAt(7, 0),
			callPrimary: nil,
			wantRule:    ruleAt(7, 0),
			wantPrimary: primaryAt("p1", "hostA", 5432),
		},
		{
			name:        "NilRule_NoOp",
			seedRule:    ruleAt(5, 0),
			seedPrimary: primaryAt("p1", "hostA", 5432),
			callRule:    nil,
			callPrimary: primaryAt("p2", "hostB", 5433),
			wantRule:    ruleAt(5, 0),
			wantPrimary: primaryAt("p1", "hostA", 5432),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := actionLockCtx(t)
			cm := NewManagerForTesting(t, nil, nil, nil, nil)
			if tt.seedRule != nil {
				require.NoError(t, cm.RecordTermPrimary(ctx, &clustermetadatapb.ReplicationPrimary{Position: &clustermetadatapb.RulePosition{Decision: tt.seedRule}, Primary: tt.seedPrimary}))
			}
			require.NoError(t, cm.RecordTermPrimary(ctx, &clustermetadatapb.ReplicationPrimary{Position: &clustermetadatapb.RulePosition{Decision: tt.callRule}, Primary: tt.callPrimary}))

			got := cm.GetReplicationPrimary()
			if tt.wantRule == nil && tt.wantPrimary == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)

			if tt.wantRule == nil {
				assert.Nil(t, got.GetPosition().GetDecision())
			} else {
				require.NotNil(t, got.GetPosition().GetDecision())
				assert.True(t, proto.Equal(tt.wantRule, got.GetPosition().GetDecision()),
					"rule mismatch:\nwant %v\ngot  %v", tt.wantRule, got.GetPosition().GetDecision())
			}
			if tt.wantPrimary == nil {
				assert.Nil(t, got.GetPrimary())
			} else {
				require.NotNil(t, got.GetPrimary())
				assert.True(t, proto.Equal(tt.wantPrimary, got.GetPrimary()),
					"primary mismatch:\nwant %v\ngot  %v", tt.wantPrimary, got.GetPrimary())
			}
		})
	}
}

// TestRecordTermPrimary_ReturnsCopies guards against callers mutating internal state
// by holding the returned pointer.
func TestRecordTermPrimary_ReturnsCopies(t *testing.T) {
	cm := NewManagerForTesting(t, nil, nil, nil, nil)
	require.NoError(t, cm.RecordTermPrimary(actionLockCtx(t), &clustermetadatapb.ReplicationPrimary{Position: &clustermetadatapb.RulePosition{Decision: ruleAt(5, 0)}, Primary: primaryAt("p1", "hostA", 5432)}))

	got := cm.GetReplicationPrimary()
	require.NotNil(t, got)
	require.NotNil(t, got.GetPrimary())
	got.Primary.Host = "tampered"

	got2 := cm.GetReplicationPrimary()
	require.NotNil(t, got2)
	require.NotNil(t, got2.GetPrimary())
	assert.Equal(t, "hostA", got2.GetPrimary().GetHost(),
		"mutating the returned pointer must not affect internal state")
}

// poolerPosAt builds a PoolerPosition with the given decision/proposal rule
// numbers and LSN, for exercising recruitPositionFloorIfOutstanding directly.
func poolerPosAt(decisionTerm, proposalTerm int64, lsn string) *clustermetadatapb.PoolerPosition {
	pos := &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: ruleAt(decisionTerm, 0)},
		Lsn:      lsn,
	}
	if proposalTerm != 0 {
		pos.Position.Proposal = ruleAt(proposalTerm, 0)
	}
	return pos
}

func TestRecruitPositionFloorIfOutstanding(t *testing.T) {
	floorAt := func(decisionTerm, proposalTerm int64, lsn string) *clustermetadatapb.LsnPosition {
		floor := &clustermetadatapb.LsnPosition{
			Position: &clustermetadatapb.RuleNumberPosition{Decision: &clustermetadatapb.RuleNumber{CoordinatorTerm: decisionTerm}},
			Lsn:      lsn,
		}
		if proposalTerm != 0 {
			floor.Position.Proposal = &clustermetadatapb.RuleNumber{CoordinatorTerm: proposalTerm}
		}
		return floor
	}

	tests := []struct {
		name            string
		floor           *clustermetadatapb.LsnPosition
		pos             *clustermetadatapb.PoolerPosition
		wantOutstanding bool
	}{
		{
			name:            "NoFloor_Satisfied",
			floor:           nil,
			pos:             poolerPosAt(5, 0, "0/1000"),
			wantOutstanding: false,
		},
		{
			name:            "DecisionAheadOfFloor_Satisfied",
			floor:           floorAt(5, 0, "0/2000"),
			pos:             poolerPosAt(6, 0, "0/1000"),
			wantOutstanding: false,
		},
		{
			name:            "DecisionBehindFloor_Outstanding",
			floor:           floorAt(5, 0, "0/2000"),
			pos:             poolerPosAt(4, 0, "0/9999"),
			wantOutstanding: true,
		},
		{
			// A proposal, when present, is always at or beyond its own
			// position's decision (UpdateRule rejects writing below the
			// current highest known rule, and finalizing a proposal clears
			// it to nil rather than leaving it equal to the new decision) —
			// so realistic proposal values here are >= the decision term.
			name:            "DecisionTied_ProposalAhead_Satisfied",
			floor:           floorAt(5, 6, "0/2000"),
			pos:             poolerPosAt(5, 7, "0/1000"),
			wantOutstanding: false,
		},
		{
			name:            "DecisionTied_ProposalBehind_Outstanding",
			floor:           floorAt(5, 7, "0/2000"),
			pos:             poolerPosAt(5, 6, "0/9999"),
			wantOutstanding: true,
		},
		{
			name:            "RulePositionTied_LSNAtFloor_Satisfied",
			floor:           floorAt(5, 0, "0/2000"),
			pos:             poolerPosAt(5, 0, "0/2000"),
			wantOutstanding: false,
		},
		{
			name:            "RulePositionTied_LSNAheadOfFloor_Satisfied",
			floor:           floorAt(5, 0, "0/2000"),
			pos:             poolerPosAt(5, 0, "0/3000"),
			wantOutstanding: false,
		},
		{
			name:            "RulePositionTied_LSNBehindFloor_Outstanding",
			floor:           floorAt(5, 0, "0/2000"),
			pos:             poolerPosAt(5, 0, "0/1000"),
			wantOutstanding: true,
		},
		{
			name:            "RulePositionTied_UnparseableCurrentLSN_FailsClosed",
			floor:           floorAt(5, 0, "0/2000"),
			pos:             poolerPosAt(5, 0, "not-an-lsn"),
			wantOutstanding: true,
		},
		{
			name:            "RulePositionTied_UnparseableFloorLSN_FailsClosed",
			floor:           floorAt(5, 0, "not-an-lsn"),
			pos:             poolerPosAt(5, 0, "0/2000"),
			wantOutstanding: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			promises := NewConsensusPromises(t.TempDir(), nil)
			_, err := promises.Load()
			require.NoError(t, err)
			if tt.floor != nil {
				require.NoError(t, promises.SetRecruitBlockedUntil(actionLockCtx(t), tt.floor))
			}
			cm := NewManagerForTesting(t, nil, promises, nil, nil)

			got := cm.recruitPositionFloorIfOutstanding(tt.pos)
			if tt.wantOutstanding {
				require.NotNil(t, got, "expected the floor to still be outstanding")
				assert.True(t, proto.Equal(tt.floor, got), "returned floor should match the recorded one")
			} else {
				assert.Nil(t, got, "expected the floor to be satisfied")
			}
		})
	}
}
