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
			cm := NewManagerForTesting(t, nil, nil, nil)
			if tt.seedRule != nil {
				require.NoError(t, cm.RecordTermPrimary(ctx, &clustermetadatapb.ReplicationPrimary{Rule: tt.seedRule, Primary: tt.seedPrimary}))
			}
			require.NoError(t, cm.RecordTermPrimary(ctx, &clustermetadatapb.ReplicationPrimary{Rule: tt.callRule, Primary: tt.callPrimary}))

			got := cm.GetReplicationPrimary()
			if tt.wantRule == nil && tt.wantPrimary == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)

			if tt.wantRule == nil {
				assert.Nil(t, got.GetRule())
			} else {
				require.NotNil(t, got.GetRule())
				assert.True(t, proto.Equal(tt.wantRule, got.GetRule()),
					"rule mismatch:\nwant %v\ngot  %v", tt.wantRule, got.GetRule())
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
	cm := NewManagerForTesting(t, nil, nil, nil)
	require.NoError(t, cm.RecordTermPrimary(actionLockCtx(t), &clustermetadatapb.ReplicationPrimary{Rule: ruleAt(5, 0), Primary: primaryAt("p1", "hostA", 5432)}))

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
