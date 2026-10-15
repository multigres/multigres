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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// RuleTransition is a RulePosition read as a transition: a settled decision and,
// while a rule change is in flight, the proposal that will replace it. Its
// methods make the caller say which half it means, instead of reaching into the
// proto — Decision() and PossiblyUndecidedRule() answer different questions,
// and mixing them up is how a leader gets named before it has been installed.
//
// By convention a RuleTransition is built from an intended-level source: the
// highest rule anyone has told us of, which includes proposals and positions
// only advertised through a ReplicationPrimary (see HighestKnownRule). A raw
// *RulePosition taken from a pooler's own PoolerPosition is the recorded
// (in-the-WAL) view; wrapping one is an explicit statement that it is being used
// as an intended rule.
//
// The zero value is an empty transition; every method is nil-safe.
type RuleTransition struct {
	pos *clustermetadatapb.RulePosition
}

// NewRuleTransition wraps pos. pos may be nil.
func NewRuleTransition(pos *clustermetadatapb.RulePosition) RuleTransition {
	return RuleTransition{pos: pos}
}

// IsEmpty reports whether no rule has been established: neither the decision nor
// the proposal carries a real rule number. Rule numbers start above {0,0}, which
// is the sentinel for "none yet" (see ruleNumberIsUnset), so a position holding
// only that is empty, just as a nil one is.
func (t RuleTransition) IsEmpty() bool {
	return ruleNumberIsUnset(t.pos.GetDecision().GetRuleNumber()) &&
		ruleNumberIsUnset(t.pos.GetProposal().GetRuleNumber())
}

// Position unwraps the underlying RulePosition, for proto fields and
// compare-and-swap anchors that take one.
func (t RuleTransition) Position() *clustermetadatapb.RulePosition {
	return t.pos
}

// IsDecided reports whether there is no outstanding proposal: the decision is
// already the most advanced rule known.
func (t RuleTransition) IsDecided() bool {
	return IsRuleDecided(t.pos)
}

// Decision is the last settled rule.
func (t RuleTransition) Decision() *clustermetadatapb.ShardRule {
	return t.pos.GetDecision()
}

// Proposal is the outstanding undecided rule, or nil.
func (t RuleTransition) Proposal() *clustermetadatapb.ShardRule {
	return t.pos.GetProposal()
}

// PossiblyUndecidedRule is the proposal if one is outstanding, otherwise the
// decision. It is the rule the shard is heading to, which may not have been
// confirmed decided yet.
func (t RuleTransition) PossiblyUndecidedRule() *clustermetadatapb.ShardRule {
	return PossiblyUndecidedRule(t.pos)
}

// Compare returns negative, zero or positive as t is behind, equal to, or ahead
// of other. See CompareRulePosition for the ordering.
func (t RuleTransition) Compare(other RuleTransition) int {
	return CompareRulePosition(t.pos, other.pos)
}

// IsRevokedBy reports whether revocation revokes this transition's rule. See
// IsRuleRevoked.
func (t RuleTransition) IsRevokedBy(revocation *clustermetadatapb.TermRevocation) bool {
	return IsRuleRevoked(t.pos, revocation)
}

// String renders the transition for logs, e.g. "5.0" or "5.0 proposal=6.0".
func (t RuleTransition) String() string {
	return FormatRulePosition(t.pos)
}
