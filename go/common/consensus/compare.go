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

	"google.golang.org/protobuf/proto"

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
	return formatDecisionAndProposal(pos.GetDecision().GetRuleNumber(), pos.GetProposal().GetRuleNumber())
}

// FormatRuleNumberPosition is FormatRulePosition for a bare RuleNumberPosition
// (decision/proposal already reduced to RuleNumber, without the full
// ShardRule content) — e.g. ConsensusStatus.RecruitBlockedUntil's position.
func FormatRuleNumberPosition(pos *clustermetadatapb.RuleNumberPosition) string {
	return formatDecisionAndProposal(pos.GetDecision(), pos.GetProposal())
}

// formatDecisionAndProposal is the shared rendering behind FormatRulePosition
// and FormatRuleNumberPosition: the decision, plus " proposal=<n>" when an
// undecided proposal is present.
func formatDecisionAndProposal(decision, proposal *clustermetadatapb.RuleNumber) string {
	s := FormatRuleNumber(decision)
	if !ruleNumberIsUnset(proposal) {
		s += " proposal=" + FormatRuleNumber(proposal)
	}
	return s
}

// FormatLsnPosition renders an LsnPosition (a rule-number position paired with
// an LSN, e.g. ConsensusStatus.RecruitBlockedUntil) as "<rule>@<lsn>" (e.g.
// "1.0@0/6000000", or "1.0 proposal=2.0@0/6000000" mid-transition). Rule
// position leads because comparisons check it first — the LSN only matters
// once rules match (see CompareWALProgress). A nil LsnPosition renders as
// "none".
func FormatLsnPosition(pos *clustermetadatapb.LsnPosition) string {
	if pos == nil {
		return "none"
	}
	return fmt.Sprintf("%s@%s", FormatRuleNumberPosition(pos.GetPosition()), pos.GetLsn())
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

// LatestWALRule returns the newest rule the position records in its WAL: the
// proposal if one is outstanding, otherwise the decision.
func (p RuleNumberPosition) LatestWALRule() *clustermetadatapb.RuleNumber {
	if ruleNumberIsUnset(p.Proposal) {
		return p.Decision
	}
	return p.Proposal
}

// CompareAuthority returns negative, zero, or positive based on whether p carries
// less, the same, or more authority than other. See the CompareAuthority function.
func (p RuleNumberPosition) CompareAuthority(other RuleNumberPosition) int {
	if cmp := CompareRuleNumbers(p.Decision, other.Decision); cmp != 0 {
		return cmp
	}
	return CompareRuleNumbers(p.Proposal, other.Proposal)
}

// Positions are compared for two questions:
//   - Authority: which rule is in force (CompareAuthority, decision first). Use it to
//     accept or refuse a request and to choose which rule to act on. Promises are part
//     of authority but are ordered by term alone: a node promised to term T refuses a
//     recruitment below T, whatever rule it names (ValidateRevocation).
//   - WAL: who holds more of what must survive (CompareWALProgress, latest rule then
//     LSN). Use it to choose a leader, freeze an LSN or judge the recusal floor.
//
// Recruit uses both: its promise and revocation scope (IsRuleRevoked) by authority,
// and its recusal floor by WAL. A known decision must never be forgotten, so
// authority ranks it first; it says nothing about holding the data, so the WAL order
// ignores it.

// CompareAuthority returns negative, zero, or positive based on whether a carries
// less, the same, or more authority than b: which rule is in force. A decided rule
// outranks another position's proposal, even one with a higher rule number; only
// when both decisions agree does each side's proposal break the tie.
//
// Use it to ask which rule should be in effect: whether a request supersedes what
// a node holds, whether a node's rule has been superseded, which intended rule to
// follow. Do not use it to ask which node is further along: a node can hold more
// WAL without knowing a rule is decided. Use CompareWALProgress for that.
//
// TODO: a proposal built on a recruited quorum of promises could be called an
// "authorized proposal".
func CompareAuthority(a, b *clustermetadatapb.RulePosition) int {
	return RuleNumberPositionOf(a).CompareAuthority(RuleNumberPositionOf(b))
}

// MostAdvancedWALPosition returns the PoolerPosition holding the most WAL among the
// given statuses (see CompareWALProgress). Returns nil if no status has a
// parseable LSN.
//
// This is the cached-snapshot analogue of minimumLeaderPosition, which runs
// over recruited statuses. Callers
// that need to derive an ExternallyCertifiedRevocation from cached cohort
// state — e.g. the bootstrap path before recruitment — use this to obtain the
// outgoing rule number and frozen LSN.
func MostAdvancedWALPosition(statuses []*clustermetadatapb.ConsensusStatus) *clustermetadatapb.PoolerPosition {
	var best *clustermetadatapb.PoolerPosition
	for _, cs := range statuses {
		pos := cs.GetCurrentPosition()
		if _, err := pgutil.ParseLSN(pos.GetLsn()); err != nil {
			continue
		}
		if best == nil || CompareWALProgress(pos, best) > 0 {
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

// HighestIntendedRule returns the RulePosition with the greatest rule any of the
// given consensus statuses has told us of. Rules are named at three levels, from
// most to least settled; name a "highest X" for its level, never just "known":
//   - decided: the decision half of a pooler's own position (HighestDecidedRule);
//   - recorded: a pooler's own position, which may also carry an undecided
//     proposal ("position" always means this level, as in MostAdvancedWALPosition);
//   - intended: recorded, plus rules only advertised through a ReplicationPrimary,
//     which no pooler's WAL need hold yet. This function ranks at this level.
//
// For each status it considers both the
// position the pooler is currently at and the position under which its
// replication primary holds leadership — a follower can learn of a newer
// leader (possibly still undecided on the leader's own side) via its
// replication primary before its own position advances. Unlike
// MostAdvancedWALPosition, it ranks by authority (CompareAuthority) with no
// LSN, since which leader is intended depends on the rule alone.
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
func HighestIntendedRule(statuses []*clustermetadatapb.ConsensusStatus) *clustermetadatapb.RulePosition {
	var best *clustermetadatapb.RulePosition
	for _, cs := range statuses {
		for _, position := range []*clustermetadatapb.RulePosition{
			cs.GetCurrentPosition().GetPosition(),
			ReplicationPrimaryOrNil(cs).GetPosition(),
		} {
			if best == nil || CompareAuthority(position, best) > 0 {
				best = position
			}
		}
	}
	return best
}

// HighestDecidedRule returns the highest decided rule number across the given
// consensus statuses, considering only each status's own CurrentPosition —
// unlike HighestIntendedRule, not ReplicationPrimary, which a follower can learn
// of before its own position replays it. This is the exact baseline
// NewTermRevocation computes as a new attempt's ReplaceDecision; anything
// comparing against that value must use this, not HighestIntendedRule, or the
// two can disagree.
func HighestDecidedRule(statuses []*clustermetadatapb.ConsensusStatus) *clustermetadatapb.RuleNumber {
	var decision *clustermetadatapb.RuleNumber
	for _, cs := range statuses {
		if d := cs.GetCurrentPosition().GetPosition().GetDecision().GetRuleNumber(); decision == nil || CompareRuleNumbers(d, decision) > 0 {
			decision = d
		}
	}
	return decision
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
	if CompareAuthority(rp.GetPosition(), targetPosition) < 0 {
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

// MergeReplicationPrimary folds a newly observed candidate into the
// previously recorded current value. It returns the ReplicationPrimary that
// should be recorded now and whether that's actually different from current
// (false if candidate carries no rule or isn't at least as advanced).
//
// This is a merge, not a replace: primary keeps current's contact info when
// candidate doesn't supply one, and rewind_ready is carried forward
// (false -> true, never the reverse) whenever candidate and current share a
// coordinator_term — including when candidate's position is a decision
// replacing current's still-outstanding proposal at that same term, which
// CompareAuthority ranks as strictly more advanced. Folding rewind_ready
// only within cmp==0 would miss exactly that transition and silently
// regress a rewind_ready flip already observed.
func MergeReplicationPrimary(current, candidate *clustermetadatapb.ReplicationPrimary) (result *clustermetadatapb.ReplicationPrimary, changed bool) {
	if PossiblyUndecidedRule(candidate.GetPosition()) == nil {
		return current, false
	}
	rewindReady := candidate.GetRewindReady()
	if PossiblyUndecidedRule(candidate.GetPosition()).GetRuleNumber().GetCoordinatorTerm() ==
		PossiblyUndecidedRule(current.GetPosition()).GetRuleNumber().GetCoordinatorTerm() {
		rewindReady = rewindReady || current.GetRewindReady()
	}
	cmp := CompareAuthority(candidate.GetPosition(), current.GetPosition())
	if cmp < 0 {
		return current, false
	}
	primary := current.GetPrimary()
	if candidate.GetPrimary() != nil {
		primary = proto.Clone(candidate.GetPrimary()).(*clustermetadatapb.PoolerAddress)
	}
	if cmp == 0 && primaryAddressEqual(primary, current.GetPrimary()) && rewindReady == current.GetRewindReady() {
		return current, false
	}
	position := current.GetPosition()
	if cmp > 0 {
		position = proto.Clone(candidate.GetPosition()).(*clustermetadatapb.RulePosition)
	}
	return &clustermetadatapb.ReplicationPrimary{
		Position:    position,
		Primary:     primary,
		RewindReady: rewindReady,
	}, true
}

// primaryAddressEqual reports whether two PoolerAddresses name the same
// pooler at the same contact info (id, host, postgres port).
func primaryAddressEqual(a, b *clustermetadatapb.PoolerAddress) bool {
	return idsEqual(a.GetId(), b.GetId()) &&
		a.GetHost() == b.GetHost() &&
		a.GetPostgresPort() == b.GetPostgresPort()
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

// CompareWALProgress returns negative, zero, or positive based on whether a holds
// less, the same, or more WAL than b: the latest rule it records, then LSN. An
// undecided proposal counts, since it is real WAL. Which rules a position knows to
// be decided plays no part.
//
// Use it to ask who holds more of the transactions that must survive: ranking
// leader candidates, setting a certificate's frozen LSN. It only ranks; whether a
// candidate is safe is decided by the recruitment conditions, not by this order.
// Do not use it to ask which rule is in force; use CompareAuthority for that.
//
// A missing or unparsable LSN counts as the lowest. That suits ranking candidates;
// CompareLsnPositions is the strict version, for a promise that must fail closed.
func CompareWALProgress(a, b *clustermetadatapb.PoolerPosition) int {
	if cmp, err := CompareLsnPositions(LsnPositionOf(a), LsnPositionOf(b)); err == nil {
		return cmp
	}
	// The rules tie and an LSN can't be parsed: one that can't is behind.
	_, errA := pgutil.ParseLSN(a.GetLsn())
	_, errB := pgutil.ParseLSN(b.GetLsn())
	switch {
	case errA != nil && errB != nil:
		return 0
	case errA != nil:
		return -1
	default:
		return 1
	}
}

// LsnPositionOf reduces a PoolerPosition to the bare rule numbers and LSN an
// LsnPosition carries.
func LsnPositionOf(pos *clustermetadatapb.PoolerPosition) *clustermetadatapb.LsnPosition {
	rules := RuleNumberPositionOf(pos.GetPosition())
	return &clustermetadatapb.LsnPosition{
		Position: &clustermetadatapb.RuleNumberPosition{Decision: rules.Decision, Proposal: rules.Proposal},
		Lsn:      pos.GetLsn(),
	}
}

// CompareLsnPositions is the strict WAL order (see CompareWALProgress): the latest
// rule, then LSN. An LSN that can't be parsed is an error rather than the lowest
// value, so a caller holding a promise, such as the recusal floor
// (recruit_blocked_until), can fail closed. LSNs are parsed only to break a rule tie.
//
// On a standby the rule comes from replayed WAL and the LSN from received WAL, so
// the two agree only once replay has caught up, as Recruit waits for.
//
// TODO: callers of CompareWALProgress already drop unparsable positions first
// (filterByValidPosition, MostAdvancedPosition), so it could be strict too.
//
// TODO: LSNs are only comparable along one timeline, and an equal latest rule does
// not guarantee the same timeline, for example when Promote decides a stuck rule on
// its own timeline first.
func CompareLsnPositions(a, b *clustermetadatapb.LsnPosition) (int, error) {
	latest := func(p *clustermetadatapb.LsnPosition) *clustermetadatapb.RuleNumber {
		return RuleNumberPosition{Decision: p.GetPosition().GetDecision(), Proposal: p.GetPosition().GetProposal()}.LatestWALRule()
	}
	if cmp := CompareRuleNumbers(latest(a), latest(b)); cmp != 0 {
		return cmp, nil
	}
	lsnA, err := pgutil.ParseLSN(a.GetLsn())
	if err != nil {
		return 0, err
	}
	lsnB, err := pgutil.ParseLSN(b.GetLsn())
	if err != nil {
		return 0, err
	}
	switch {
	case lsnA < lsnB:
		return -1, nil
	case lsnA > lsnB:
		return 1, nil
	}
	return 0, nil
}
