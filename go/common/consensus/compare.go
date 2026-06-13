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

// Package consensus provides utilities for working with consensus types.
package consensus

import (
	"fmt"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// CompareRuleNumbers compares two RuleNumbers lexicographically.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// A nil RuleNumber is treated as zero (the smallest possible value).
func CompareRuleNumbers(a, b *clustermetadatapb.RuleNumber) int {
	aTerm := int64(0)
	aSubterm := int64(0)
	if a != nil {
		aTerm = a.GetCoordinatorTerm()
		aSubterm = a.GetLeaderSubterm()
	}

	bTerm := int64(0)
	bSubterm := int64(0)
	if b != nil {
		bTerm = b.GetCoordinatorTerm()
		bSubterm = b.GetLeaderSubterm()
	}

	if aTerm != bTerm {
		if aTerm < bTerm {
			return -1
		}
		return 1
	}
	if aSubterm != bSubterm {
		if aSubterm < bSubterm {
			return -1
		}
		return 1
	}
	return 0
}

// FormatRuleNumber renders a RuleNumber for human-readable logs as
// "coordinator_term.leader_subterm" (e.g. "7.2"). The proto's default string
// form is verbose and not log-friendly, so prefer this helper wherever a rule
// number is logged. A nil RuleNumber renders as "none".
func FormatRuleNumber(rn *clustermetadatapb.RuleNumber) string {
	if rn == nil {
		return "none"
	}
	return fmt.Sprintf("%d.%d", rn.GetCoordinatorTerm(), rn.GetLeaderSubterm())
}

// MostAuthoritativeObservation returns the LeaderObservation with the highest
// rule number among those given (nil entries are skipped). Used to merge leader
// observations from different sources — a topology record's self_leadership and
// a health-stream report — choosing the one made under the newest rule. Returns
// nil if all inputs are nil.
func MostAuthoritativeObservation(obs ...*clustermetadatapb.LeaderObservation) *clustermetadatapb.LeaderObservation {
	var best *clustermetadatapb.LeaderObservation
	for _, o := range obs {
		if o == nil {
			continue
		}
		if best == nil || CompareRuleNumbers(o.GetLeaderRuleNumber(), best.GetLeaderRuleNumber()) > 0 {
			best = o
		}
	}
	return best
}

// MostAdvancedPosition returns the highest-ranked PoolerPosition among the
// given statuses. Rule number takes precedence; LSN breaks ties within the
// same rule. Returns nil if no status has a parseable LSN.
//
// This is the cached-snapshot analogue of discoverMostAdvancedTimeline, which
// runs over recruited statuses and returns the eligible-leader set. Callers
// that need to derive an ExternallyCertifiedRevocation from cached cohort
// state — e.g. the bootstrap path before recruitment — use this to obtain the
// outgoing rule number and frozen LSN.
func MostAdvancedPosition(statuses []*clustermetadatapb.ConsensusStatus) *clustermetadatapb.PoolerPosition {
	var best *clustermetadatapb.PoolerPosition
	for _, cs := range statuses {
		pos := cs.GetCurrentPosition()
		if _, err := pgutil.ParseLSN(pos.GetLsn()); err != nil {
			continue
		}
		if best == nil || ComparePosition(pos, best) > 0 {
			best = pos
		}
	}
	return best
}

// HighestKnownRule returns the ShardRule with the greatest rule number known to
// any of the given consensus statuses. For each status it considers both the
// rule the pooler is currently positioned at and the rule under which its
// replication primary holds leadership — a follower can learn of a newer leader
// via its replication primary before its own position advances. Unlike
// MostAdvancedPosition, ranking is purely by rule number (no LSN tiebreak, no
// LSN requirement), since leader identity is a function of the rule alone.
// Returns nil when no status carries a rule.
//
// This is the single way leader identity is determined across the system: the
// highest known rule names the consensus leader (GetLeaderId()).
//
// TODO: detect equivocation. Two rules sharing the same rule number but naming
// different leaders is a protocol-invariant violation (a rule number is assigned
// by a single coordinator and must name exactly one leader) — i.e. split brain.
// Today such a tie is resolved silently by keeping the first-seen rule. A future
// enhancement should surface this as an error rather than papering over it.
func HighestKnownRule(statuses []*clustermetadatapb.ConsensusStatus) *clustermetadatapb.ShardRule {
	var best *clustermetadatapb.ShardRule
	for _, cs := range statuses {
		for _, rule := range []*clustermetadatapb.ShardRule{
			cs.GetCurrentPosition().GetRule(),
			cs.GetReplicationPrimary().GetRule(),
		} {
			if rule == nil {
				continue
			}
			if best == nil || CompareRuleNumbers(rule.GetRuleNumber(), best.GetRuleNumber()) > 0 {
				best = rule
			}
		}
	}
	return best
}

// ReplicationPrimaryMatches reports whether a pooler's published
// ReplicationPrimary already names target as its primary at a rule no older
// than targetRule. Coordinators use this to skip SetPrimary RPCs that
// wouldn't change anything on the pooler.
//
// Returns false when:
//   - rp is nil
//   - the published rule is strictly older than targetRule
//   - the published primary is missing
//   - the published primary's (id, host, postgres port) differs from target's
//
// target and targetRule are required; passing nil for either returns false.
func ReplicationPrimaryMatches(rp *clustermetadatapb.ReplicationPrimary, target *clustermetadatapb.PoolerAddress, targetRule *clustermetadatapb.ShardRule) bool {
	if rp == nil || target == nil || targetRule == nil {
		return false
	}
	if CompareRuleNumbers(rp.GetRule().GetRuleNumber(), targetRule.GetRuleNumber()) < 0 {
		return false
	}
	rpPrimary := rp.GetPrimary()
	if rpPrimary == nil {
		return false
	}
	if !idsEqual(rpPrimary.GetId(), target.GetId()) {
		return false
	}
	if rpPrimary.GetHost() != target.GetHost() {
		return false
	}
	if rpPrimary.GetPostgresPort() != target.GetPostgresPort() {
		return false
	}
	return true
}

func idsEqual(a, b *clustermetadatapb.ID) bool {
	return a.GetComponent() == b.GetComponent() &&
		a.GetCell() == b.GetCell() &&
		a.GetName() == b.GetName()
}

// ComparePosition returns negative, zero, or positive based on whether a is
// behind, equal to, or ahead of b. Rule number takes precedence; LSN breaks
// ties within the same rule. A missing or unparsable LSN is treated as less
// than any valid LSN.
func ComparePosition(a, b *clustermetadatapb.PoolerPosition) int {
	if cmp := CompareRuleNumbers(a.GetRule().GetRuleNumber(), b.GetRule().GetRuleNumber()); cmp != 0 {
		return cmp
	}
	lsnA, errA := pgutil.ParseLSN(a.GetLsn())
	lsnB, errB := pgutil.ParseLSN(b.GetLsn())
	okA := errA == nil
	okB := errB == nil
	switch {
	case !okA && !okB:
		return 0
	case !okA:
		return -1
	case !okB:
		return 1
	case lsnA < lsnB:
		return -1
	case lsnA > lsnB:
		return 1
	default:
		return 0
	}
}
