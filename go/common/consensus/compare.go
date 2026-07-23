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

// FormatRulePosition renders a RulePosition for human-readable logs as its
// decision, plus " proposal=<n>" appended when a proposal is present (e.g.
// "5.0" or "5.0 proposal=6.0"). Prefer this helper wherever a rule position
// is logged.
func FormatRulePosition(pos *clustermetadatapb.RulePosition) string {
	s := FormatRuleNumber(pos.GetDecision().GetRuleNumber())
	if proposalRN := pos.GetProposal().GetRuleNumber(); !ruleNumberIsUnset(proposalRN) {
		s += " proposal=" + FormatRuleNumber(proposalRN)
	}
	return s
}

// ruleNumberIsUnset reports whether rn is nil or the phantom zero value
// (coordinator_term == 0 && leader_subterm == 0) — the sentinel this codebase
// uses for "no rule established yet" (proto3 cannot distinguish an unset
// message field from one explicitly written with zero-valued contents, and
// some code paths do the latter).
func ruleNumberIsUnset(rn *clustermetadatapb.RuleNumber) bool {
	return max(rn.GetCoordinatorTerm(), rn.GetLeaderSubterm()) == 0
}

// IsRuleDecided reports whether pos has no outstanding undecided proposal —
// its decision already reflects the most advanced rule this position knows.
func IsRuleDecided(pos *clustermetadatapb.RulePosition) bool {
	return ruleNumberIsUnset(pos.GetProposal().GetRuleNumber())
}

// PossiblyUndecidedRule returns pos's proposal if one is outstanding — real
// WAL content, but not yet confirmed decided — otherwise its decision. Nil if
// pos carries neither.
func PossiblyUndecidedRule(pos *clustermetadatapb.RulePosition) *clustermetadatapb.ShardRule {
	if !IsRuleDecided(pos) {
		return pos.GetProposal()
	}
	return pos.GetDecision()
}

// RuleNumberPosition is RulePosition reduced to bare rule numbers, without
// the full ShardRule content (cohort members, durability policy, leader,
// etc.). Useful for expressing a target/expected position when that extra
// content isn't known or doesn't apply — e.g. a coordinator's expected
// outgoing rule, which is only ever a bare RuleNumber
// (revocation.outgoing_rule), never a full ShardRule with its own proposal.
//
// TODO: clustermetadatapb.RuleNumberPosition (proto) is the same concept,
// added later for a case that needed to serialize/pass it across an RPC
// boundary (ConsensusManager's recruit-position-floor stub). Consider
// unifying — see that message's TODO for options.
type RuleNumberPosition struct {
	Decision *clustermetadatapb.RuleNumber
	Proposal *clustermetadatapb.RuleNumber
}

// RuleNumberPositionOf reduces a full RulePosition to its bare rule numbers.
func RuleNumberPositionOf(pos *clustermetadatapb.RulePosition) RuleNumberPosition {
	return RuleNumberPosition{
		Decision: pos.GetDecision().GetRuleNumber(),
		Proposal: pos.GetProposal().GetRuleNumber(),
	}
}

// Compare returns negative, zero, or positive based on whether p is behind,
// equal to, or ahead of other. The decision takes precedence over
// everything else — a confirmed rule always outranks another position's
// unconfirmed proposal alone, even one with a higher rule number. Only when
// both decisions agree does each side's proposal break the tie (further
// beyond a shared decision is more advanced).
func (p RuleNumberPosition) Compare(other RuleNumberPosition) int {
	if cmp := CompareRuleNumbers(p.Decision, other.Decision); cmp != 0 {
		return cmp
	}
	return CompareRuleNumbers(p.Proposal, other.Proposal)
}

// CompareRulePosition returns negative, zero, or positive based on whether a
// is behind, equal to, or ahead of b. The decision takes precedence over
// everything else — a confirmed rule always outranks another position's
// unconfirmed proposal alone, even one with a higher rule number. Only when
// both decisions agree does each side's proposal break the tie (further
// beyond a shared decision is more advanced).
func CompareRulePosition(a, b *clustermetadatapb.RulePosition) int {
	return RuleNumberPositionOf(a).Compare(RuleNumberPositionOf(b))
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
		if best == nil || ComparePoolerPosition(pos, best) > 0 {
			best = pos
		}
	}
	return best
}

// ReplicationPrimaryOrNil returns cs's replication primary, or nil when it
// carries no established leader to replicate from.
//
// proto3 cannot distinguish an unset ReplicationPrimary from one explicitly
// written with a zero-valued rule. RuleNumber{0,0} is reserved codebase-wide
// as the "no rule recorded" sentinel (see ruleNumberIsUnset) — a follower
// that has never learned of a leader reports its ReplicationPrimary at that
// zero value, which is reported as absent here. Every real rule (including
// the initial bootstrap row, written at {0,1} by CreateRuleTables) is
// strictly greater than {0,0}, so a real replication primary is never
// dropped.
//
// Always use this instead of ConsensusStatus.GetReplicationPrimary() (enforced
// by ruleguard) so a phantom 0/0 entry never gets mistaken for a real one.
func ReplicationPrimaryOrNil(cs *clustermetadatapb.ConsensusStatus) *clustermetadatapb.ReplicationPrimary {
	rp := cs.GetReplicationPrimary()
	position := rp.GetPosition()
	if ruleNumberIsUnset(position.GetDecision().GetRuleNumber()) && ruleNumberIsUnset(position.GetProposal().GetRuleNumber()) {
		return nil
	}
	return rp
}

// HighestKnownRule returns the RulePosition with the greatest rule known to
// any of the given consensus statuses. For each status it considers both the
// position the pooler is currently at and the position under which its
// replication primary holds leadership — a follower can learn of a newer
// leader (possibly still undecided on the leader's own side) via its
// replication primary before its own position advances. Unlike
// MostAdvancedPosition, ranking is purely by position (no LSN tiebreak, no
// LSN requirement), since leader identity is a function of the rule alone.
// Returns nil when no status carries a rule.
//
// A phantom 0/0 replication primary (see ReplicationPrimaryOrNil) is ignored so
// it never shadows a real rule.
//
// TODO: detect equivocation. Two rules sharing the same rule number but naming
// different leaders is a protocol-invariant violation (a rule number is assigned
// by a single coordinator and must name exactly one leader) — i.e. split brain.
// Today such a tie is resolved silently by keeping the first-seen rule. A future
// enhancement should surface this as an error rather than papering over it.
func HighestKnownRule(statuses []*clustermetadatapb.ConsensusStatus) *clustermetadatapb.RulePosition {
	var best *clustermetadatapb.RulePosition
	for _, cs := range statuses {
		for _, position := range []*clustermetadatapb.RulePosition{
			cs.GetCurrentPosition().GetPosition(),
			ReplicationPrimaryOrNil(cs).GetPosition(),
		} {
			if best == nil || CompareRulePosition(position, best) > 0 {
				best = position
			}
		}
	}
	return best
}

// ReplicationPrimaryMatches reports whether a pooler's published
// ReplicationPrimary already names target as its primary at a position no
// older than targetPosition. Coordinators use this to skip SetPrimary RPCs
// that wouldn't change anything on the pooler.
//
// Returns false when:
//   - rp is nil
//   - the published position is strictly older than targetPosition
//   - the published primary is missing
//   - the published primary's (id, host, postgres port) differs from target's
//
// target and targetPosition are required; passing nil for either returns false.
func ReplicationPrimaryMatches(rp *clustermetadatapb.ReplicationPrimary, target *clustermetadatapb.PoolerAddress, targetPosition *clustermetadatapb.RulePosition) bool {
	if rp == nil || target == nil || targetPosition == nil {
		return false
	}
	if CompareRulePosition(rp.GetPosition(), targetPosition) < 0 {
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

// RuleNamesLeader reports whether rule names id as its leader. Returns false
// when rule, its leader ID, or id is absent — two absent IDs must not be treated
// as a match (idsEqual(nil, nil) is true), or a pooler with no ID would be
// classified as the leader of a leaderless bootstrap rule.
func RuleNamesLeader(rule *clustermetadatapb.ShardRule, id *clustermetadatapb.ID) bool {
	leader := rule.GetLeaderId()
	if leader == nil || id == nil {
		return false
	}
	return idsEqual(leader, id)
}

// ComparePoolerPosition returns negative, zero, or positive based on whether a is
// behind, equal to, or ahead of b. The decision takes precedence over
// everything else — a confirmed rule always outranks another position's
// unconfirmed proposal, even one with a higher rule number. Only when both
// decisions agree does each side's proposal break the tie (further beyond a
// shared decision is more advanced), and only when both agree on the
// resulting rule does LSN break the tie. A missing or unparsable LSN is
// treated as less than any valid LSN.
func ComparePoolerPosition(a, b *clustermetadatapb.PoolerPosition) int {
	if cmp := CompareRulePosition(a.GetPosition(), b.GetPosition()); cmp != 0 {
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
