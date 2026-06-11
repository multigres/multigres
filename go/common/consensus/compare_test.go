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

func TestMostAdvancedPosition(t *testing.T) {
	mkID := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		}
	}
	status := func(name string, p *clustermetadatapb.PoolerPosition) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{Id: mkID(name), CurrentPosition: p}
	}

	t.Run("empty input returns nil", func(t *testing.T) {
		got := MostAdvancedPosition(nil)
		assert.Nil(t, got)
	})

	t.Run("all statuses have unparsable LSN: returns nil", func(t *testing.T) {
		got := MostAdvancedPosition([]*clustermetadatapb.ConsensusStatus{
			status("a", pos(3, "")),
			status("b", pos(3, "bad-lsn")),
		})
		assert.Nil(t, got)
	})

	t.Run("highest rule wins regardless of LSN", func(t *testing.T) {
		got := MostAdvancedPosition([]*clustermetadatapb.ConsensusStatus{
			status("a", pos(2, "0/9000000")),
			status("b", pos(4, "0/100")),
			status("c", pos(3, "0/500000")),
		})
		assert.Equal(t, int64(4), got.GetRule().GetRuleNumber().GetCoordinatorTerm())
		assert.Equal(t, "0/100", got.GetLsn())
	})

	t.Run("same rule highest LSN wins", func(t *testing.T) {
		got := MostAdvancedPosition([]*clustermetadatapb.ConsensusStatus{
			status("a", pos(3, "0/100")),
			status("b", pos(3, "0/300")),
			status("c", pos(3, "0/200")),
		})
		assert.Equal(t, "0/300", got.GetLsn())
	})

	t.Run("skips statuses with unparsable LSN", func(t *testing.T) {
		got := MostAdvancedPosition([]*clustermetadatapb.ConsensusStatus{
			status("a", pos(5, "bad-lsn")),
			status("b", pos(3, "0/100")),
		})
		// pooler-a's rule is higher (5) but its LSN is unparsable, so it's
		// filtered out and pooler-b wins despite the lower rule.
		assert.Equal(t, int64(3), got.GetRule().GetRuleNumber().GetCoordinatorTerm())
		assert.Equal(t, "0/100", got.GetLsn())
	})
}

func TestReplicationPrimaryMatches(t *testing.T) {
	mkID := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		}
	}
	mkAddr := func(name, host string, pgPort int32) *clustermetadatapb.PoolerAddress {
		return &clustermetadatapb.PoolerAddress{
			Id:           mkID(name),
			Host:         host,
			PostgresPort: pgPort,
		}
	}
	mkRule := func(term int64) *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{RuleNumber: rn(term, 0)}
	}
	mkRP := func(rule *clustermetadatapb.ShardRule, primary *clustermetadatapb.PoolerAddress) *clustermetadatapb.ReplicationPrimary {
		return &clustermetadatapb.ReplicationPrimary{Rule: rule, Primary: primary}
	}

	target := mkAddr("primary-1", "host-a", 5432)
	targetRule := mkRule(3)

	t.Run("nil rp returns false", func(t *testing.T) {
		assert.False(t, ReplicationPrimaryMatches(nil, target, targetRule))
	})

	t.Run("nil target returns false", func(t *testing.T) {
		rp := mkRP(targetRule, target)
		assert.False(t, ReplicationPrimaryMatches(rp, nil, targetRule))
	})

	t.Run("nil targetRule returns false", func(t *testing.T) {
		rp := mkRP(targetRule, target)
		assert.False(t, ReplicationPrimaryMatches(rp, target, nil))
	})

	t.Run("published rule older than targetRule returns false", func(t *testing.T) {
		rp := mkRP(mkRule(2), target)
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetRule))
	})

	t.Run("published primary missing returns false", func(t *testing.T) {
		rp := mkRP(targetRule, nil)
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetRule))
	})

	t.Run("primary id mismatch returns false", func(t *testing.T) {
		rp := mkRP(targetRule, mkAddr("primary-2", "host-a", 5432))
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetRule))
	})

	t.Run("primary id different cell returns false", func(t *testing.T) {
		other := mkAddr("primary-1", "host-a", 5432)
		other.Id.Cell = "zone2"
		rp := mkRP(targetRule, other)
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetRule))
	})

	t.Run("primary id different component returns false", func(t *testing.T) {
		other := mkAddr("primary-1", "host-a", 5432)
		other.Id.Component = clustermetadatapb.ID_MULTIGATEWAY
		rp := mkRP(targetRule, other)
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetRule))
	})

	t.Run("primary hostname mismatch returns false", func(t *testing.T) {
		rp := mkRP(targetRule, mkAddr("primary-1", "host-b", 5432))
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetRule))
	})

	t.Run("primary port mismatch returns false", func(t *testing.T) {
		rp := mkRP(targetRule, mkAddr("primary-1", "host-a", 5433))
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetRule))
	})

	t.Run("exact match returns true", func(t *testing.T) {
		rp := mkRP(targetRule, target)
		assert.True(t, ReplicationPrimaryMatches(rp, target, targetRule))
	})

	t.Run("published rule newer than targetRule still matches", func(t *testing.T) {
		// Coordinator's targetRule may lag the pooler's published rule —
		// "no older than" means published >= target.
		rp := mkRP(mkRule(5), target)
		assert.True(t, ReplicationPrimaryMatches(rp, target, targetRule))
	})
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
		{"same rule valid LSN beats unparsable LSN", pos(3, "0/100"), pos(3, "not-an-lsn"), 1},

		// Both positions invalid at the same rule — neither is ahead.
		{"same rule both empty LSN", pos(3, ""), pos(3, ""), 0},
		{"same rule both unparsable LSN", pos(3, "bad"), pos(3, "also-bad"), 0},

		// Nil positions are treated as the minimum.
		{"both nil", nil, nil, 0},
		{"nil less than non-nil", nil, pos(1, "0/100"), -1},
		{"non-nil greater than nil", pos(1, "0/100"), nil, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComparePosition(tt.a, tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func obs(name string, term, subterm int64) *clustermetadatapb.LeaderObservation {
	return &clustermetadatapb.LeaderObservation{
		LeaderId:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name},
		LeaderRuleNumber: rn(term, subterm),
	}
}

func TestMostAuthoritativeObservation(t *testing.T) {
	a := obs("a", 5, 0)
	b := obs("b", 6, 0)
	c := obs("c", 6, 1)

	t.Run("all nil returns nil", func(t *testing.T) {
		assert.Nil(t, MostAuthoritativeObservation(nil, nil))
		assert.Nil(t, MostAuthoritativeObservation())
	})

	t.Run("skips nil entries", func(t *testing.T) {
		assert.Same(t, a, MostAuthoritativeObservation(nil, a, nil))
	})

	t.Run("highest rule number wins", func(t *testing.T) {
		assert.Same(t, b, MostAuthoritativeObservation(a, b))
		assert.Same(t, c, MostAuthoritativeObservation(a, b, c)) // subterm breaks the term tie
	})

	t.Run("first wins on an exact rule tie", func(t *testing.T) {
		tie := obs("tie", 6, 1)
		assert.Same(t, c, MostAuthoritativeObservation(c, tie))
	})
}
