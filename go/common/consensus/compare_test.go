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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func rn(term, subterm int64) *clustermetadatapb.RuleNumber {
	return &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm}
}

func pos(term int64, lsn string) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Rule: &clustermetadatapb.ShardRule{RuleNumber: rn(term, 0)},
		Lsn:  lsn,
	}
}

func TestCompareRuleNumbers(t *testing.T) {
	tests := []struct {
		name string
		a, b *clustermetadatapb.RuleNumber
		want int
	}{
		{"equal", rn(5, 3), rn(5, 3), 0},
		{"higher term wins", rn(6, 0), rn(5, 99), 1},
		{"lower term loses", rn(4, 99), rn(5, 0), -1},
		{"same term higher subterm", rn(5, 4), rn(5, 3), 1},
		{"same term lower subterm", rn(5, 2), rn(5, 3), -1},
		{"nil treated as zero", nil, rn(0, 0), 0},
		{"nil less than non-zero", nil, rn(1, 0), -1},
		{"both nil", nil, nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareRuleNumbers(tt.a, tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestComparePosition(t *testing.T) {
	tests := []struct {
		name string
		a, b *clustermetadatapb.PoolerPosition
		want int
	}{
		// Rule number takes absolute precedence over LSN.
		{"higher rule wins regardless of LSN", pos(4, "0/100"), pos(3, "0/9999999"), 1},
		{"lower rule loses regardless of LSN", pos(3, "0/9999999"), pos(4, "0/100"), -1},

		// Within the same rule, LSN is the tiebreaker.
		{"same rule higher LSN wins", pos(3, "0/200"), pos(3, "0/100"), 1},
		{"same rule lower LSN loses", pos(3, "0/100"), pos(3, "0/200"), -1},
		{"same rule equal LSNs", pos(3, "0/100"), pos(3, "0/100"), 0},

		// A valid LSN is more authoritative than an invalid or missing one at the
		// same rule: the node with no LSN cannot prove its position.
		{"same rule valid LSN beats empty LSN", pos(3, "0/100"), pos(3, ""), 1},
		{"same rule empty LSN loses to valid LSN", pos(3, ""), pos(3, "0/100"), -1},
		{"same rule valid LSN beats unparseable LSN", pos(3, "0/100"), pos(3, "not-an-lsn"), 1},

		// Both positions invalid at the same rule — neither is ahead.
		{"same rule both empty LSN", pos(3, ""), pos(3, ""), 0},
		{"same rule both unparseable LSN", pos(3, "bad"), pos(3, "also-bad"), 0},

		// Nil positions are treated as the minimum.
		{"both nil", nil, nil, 0},
		{"nil less than non-nil", nil, pos(1, "0/100"), -1},
		{"non-nil greater than nil", pos(1, "0/100"), nil, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := comparePosition(tt.a, tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}
