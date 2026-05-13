// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consensus

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ParseUserSpecifiedDurabilityPolicy converts a policy name string into a DurabilityPolicy message.
// TODO: generalize to support AT_LEAST_N and MULTI_CELL_AT_LEAST_N for arbitrary N by parsing the number
// from the suffix (e.g. "AT_LEAST_3", "MULTI_CELL_AT_LEAST_4") instead of enumerating each case.
func ParseUserSpecifiedDurabilityPolicy(name string) (*clustermetadatapb.DurabilityPolicy, error) {
	switch name {
	case "AT_LEAST_2":
		return topoclient.AtLeastN(2), nil
	case "MULTI_CELL_AT_LEAST_2":
		return topoclient.MultiCellAtLeastN(2), nil
	default:
		return nil, fmt.Errorf("unsupported durability policy %q (supported: AT_LEAST_2, MULTI_CELL_AT_LEAST_2)", name)
	}
}

// DurabilityPolicy captures the quorum semantics of a single durability rule.
//
// It exposes two checks a coordinator needs to safely appoint a new leader
// under the generalized-consensus model:
//
//  1. Achievability: a pre-flight feasibility gate. Checks if the proposed cohort could
//     satisfy this policy's durability conditions.
//  2. Sufficient recruitment: the recruited subset of a recorded cohort can
//     form a fresh quorum (candidacy) AND intersects every other quorum the
//     cohort could form (revocation).
type DurabilityPolicy interface {
	// CheckAchievable returns nil if the proposed cohort could satisfy
	// this policy. Used as a pre-flight feasibility gate before attempting
	// recruitment.
	CheckAchievable(proposedCohort []*clustermetadatapb.ID) error

	// CheckSufficientRecruitment returns nil if recruited is sufficient to
	// safely establish a new leader. This has two obligations:
	//
	//   - Candidacy: recruited can form a fresh quorum under this policy, so
	//     the new leader has forward progress.
	//   - Revocation: recruited intersects every other quorum the cohort
	//     could form under this policy, so no parallel quorum can still
	//     commit outside our recruitment.
	CheckSufficientRecruitment(cohort, recruited []*clustermetadatapb.ID) error

	// BuildSyncReplicationConfig returns the Postgres-level config the primary
	// must apply to satisfy this policy's durability obligations.
	//
	// cohort is the full set of poolers participating in the term, including
	// the primary. The method derives the eligible standby set per policy
	// (e.g., MultiCellPolicy excludes the primary's cell). Passing the full
	// cohort keeps the caller-side contract simple.
	//
	// On success the config is always non-nil — every promotion explicitly
	// rewires Postgres replication, so the caller can blindly translate it
	// to a ConfigureSynchronousReplicationRequest. For policies trivially
	// satisfied without sync replication (RequiredCount==1), the config
	// uses SYNCHRONOUS_COMMIT_LOCAL with an empty SyncStandbyIDs, which
	// causes Postgres to clear synchronous_standby_names — explicitly
	// dropping any stale sync configuration the new primary may have
	// inherited from a prior role. Returns an error when the cohort cannot
	// satisfy the policy's num_sync requirement.
	BuildSyncReplicationConfig(
		logger *slog.Logger,
		cohort []*clustermetadatapb.ID,
		primary *clustermetadatapb.ID,
	) (*SyncReplicationConfig, error)

	// Description returns a human-readable summary of the policy.
	Description() string
}

// SyncReplicationConfig is the Postgres-level configuration a primary must
// apply to satisfy a durability policy.
//
// It captures only the durability-meaningful outputs of the policy — the
// commit level, the standby acknowledgement method, the count, and the
// eligible standby set. RPC plumbing concerns (reload-vs-restart, etc.) live
// at the call site that translates this into a wire request.
type SyncReplicationConfig struct {
	SyncCommit     multipoolermanagerdatapb.SynchronousCommitLevel
	SyncMethod     multipoolermanagerdatapb.SynchronousMethod
	NumSync        int
	SyncStandbyIDs []*clustermetadatapb.ID
}

// NewPolicyFromProto converts a proto DurabilityPolicy into a concrete
// DurabilityPolicy implementation.
func NewPolicyFromProto(policy *clustermetadatapb.DurabilityPolicy) (DurabilityPolicy, error) {
	if policy == nil {
		return nil, errors.New("durability policy is nil")
	}

	switch policy.QuorumType {
	case clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N:
		// N=0 would make revocation (|missing| < N) unsatisfiable for any recruitment.
		if policy.RequiredCount < 1 {
			return nil, fmt.Errorf("AT_LEAST_N requires RequiredCount >= 1, got %d", policy.RequiredCount)
		}
		return AtLeastNPolicy{N: int(policy.RequiredCount)}, nil
	case clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N:
		// N=0 would make revocation (|uncovered cells| < N) unsatisfiable for any recruitment.
		if policy.RequiredCount < 1 {
			return nil, fmt.Errorf("MULTI_CELL_AT_LEAST_N requires RequiredCount >= 1, got %d", policy.RequiredCount)
		}
		return MultiCellPolicy{N: int(policy.RequiredCount)}, nil
	default:
		return nil, fmt.Errorf("unsupported quorum type: %v", policy.QuorumType)
	}
}

// keysOf returns the set of distinct keyFn-keys present in poolers.
func keysOf(poolers []*clustermetadatapb.ID, keyFn func(*clustermetadatapb.ID) string) map[string]struct{} {
	out := make(map[string]struct{}, len(poolers))
	for _, p := range poolers {
		out[keyFn(p)] = struct{}{}
	}
	return out
}

// poolerKeysOf returns the set of cluster-unique pooler keys.
func poolerKeysOf(poolers []*clustermetadatapb.ID) map[string]struct{} {
	return keysOf(poolers, topoclient.ClusterIDString)
}

// cohortIntersect returns the IDs of nodes (from statuses) that are members of
// cohort. statuses is assumed to be already deduplicated by ID.
func cohortIntersect(cohort []*clustermetadatapb.ID, statuses []*clustermetadatapb.ConsensusStatus) []*clustermetadatapb.ID {
	cohortKeys := poolerKeysOf(cohort)
	result := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, cs := range statuses {
		id := cs.GetId()
		if id == nil {
			continue
		}
		if _, inCohort := cohortKeys[topoclient.ClusterIDString(id)]; inCohort {
			result = append(result, id)
		}
	}
	return result
}

// PolicyWithCohort bundles a DurabilityPolicy with the cohort it applies to.
type PolicyWithCohort struct {
	Policy DurabilityPolicy
	Cohort []*clustermetadatapb.ID
}

// NewPolicyWithCohort constructs a PolicyWithCohort from a proto DurabilityPolicy and cohort.
// Returns an error if the proto cannot be converted to a concrete policy implementation.
func NewPolicyWithCohort(cohort []*clustermetadatapb.ID, dp *clustermetadatapb.DurabilityPolicy) (PolicyWithCohort, error) {
	policy, err := NewPolicyFromProto(dp)
	if err != nil {
		return PolicyWithCohort{}, fmt.Errorf("invalid durability policy: %w", err)
	}
	return PolicyWithCohort{Policy: policy, Cohort: cohort}, nil
}

// PolicyTransition holds the computed GUC policies for a leader-led rule change.
// Both is applied before the WAL write, satisfying both the old and new policy
// simultaneously. Incoming is applied after the WAL write.
type PolicyTransition struct {
	Both     PolicyWithCohort
	Incoming PolicyWithCohort
}

// intersectStandbys returns the IDs from a that also appear in b.
func intersectStandbys(a, b []*clustermetadatapb.ID) []*clustermetadatapb.ID {
	bKeys := poolerKeysOf(b)
	result := make([]*clustermetadatapb.ID, 0, len(a))
	for _, id := range a {
		if _, ok := bKeys[topoclient.ClusterIDString(id)]; ok {
			result = append(result, id)
		}
	}
	return result
}

// cohortIsSubsetOf reports whether every element of a appears in b.
func cohortIsSubsetOf(a, b []*clustermetadatapb.ID) bool {
	return len(intersectStandbys(a, b)) == len(a)
}

// BuildPolicyTransition computes the PolicyTransition for a leader-led rule
// change where only N or the cohort changes, not both simultaneously.
//
// Returns an error for unsupported transitions: mixed policy types, both N and
// cohort changing at once, or cohorts with no subset relationship.
//
// For same-N transitions Both uses the smaller cohort (the subset), ensuring any
// ack satisfies both the old and new policy. For same-cohort transitions Both uses
// the larger N, ensuring the WAL record is acknowledged under the stricter policy.
// When outgoing and incoming are identical Both matches incoming.
//
// Both AtLeastNPolicy and MultiCellPolicy are supported; mixing the two types
// returns an error.
func BuildPolicyTransition(outgoing, incoming PolicyWithCohort) (*PolicyTransition, error) {
	outN, outFamily := policyFamily(outgoing.Policy)
	inN, inFamily := policyFamily(incoming.Policy)
	if outFamily == "" || inFamily == "" {
		return nil, fmt.Errorf("unsupported leader-led rule change: policies must be AtLeastN or MultiCellAtLeastN (got %T and %T)", outgoing.Policy, incoming.Policy)
	}
	if outFamily != inFamily {
		return nil, fmt.Errorf("unsupported leader-led rule change: policy types must match (got %T and %T)", outgoing.Policy, incoming.Policy)
	}

	cohortSame := sameCohort(outgoing.Cohort, incoming.Cohort)
	nSame := outN == inN

	if cohortSame && nSame {
		return &PolicyTransition{Both: incoming, Incoming: incoming}, nil
	}
	if !cohortSame && !nSame {
		return nil, errors.New("unsupported leader-led rule change: both N and cohort changed simultaneously")
	}

	if nSame {
		// Cohort changed: Both uses the subset cohort.
		if cohortIsSubsetOf(incoming.Cohort, outgoing.Cohort) {
			return &PolicyTransition{Both: incoming, Incoming: incoming}, nil
		}
		if cohortIsSubsetOf(outgoing.Cohort, incoming.Cohort) {
			return &PolicyTransition{Both: outgoing, Incoming: incoming}, nil
		}
		return nil, errors.New("unsupported leader-led rule change: neither cohort is a subset of the other")
	}

	// Same cohort, N changed: Both uses the larger N.
	if inN > outN {
		return &PolicyTransition{Both: incoming, Incoming: incoming}, nil
	}
	return &PolicyTransition{Both: outgoing, Incoming: incoming}, nil
}

// policyFamily returns the RequiredCount and a string tag identifying the policy
// family for AtLeastNPolicy and MultiCellPolicy. Returns (0, "") for unsupported types.
func policyFamily(p DurabilityPolicy) (n int, family string) {
	switch v := p.(type) {
	case AtLeastNPolicy:
		return v.N, "at_least_n"
	case MultiCellPolicy:
		return v.N, "multi_cell_at_least_n"
	default:
		return 0, ""
	}
}

// sameCohort reports whether a and b represent the same set of pooler IDs.
func sameCohort(a, b []*clustermetadatapb.ID) bool {
	if len(a) != len(b) {
		return false
	}
	aKeys := poolerKeysOf(a)
	for _, id := range b {
		if _, ok := aKeys[topoclient.ClusterIDString(id)]; !ok {
			return false
		}
	}
	return true
}

// validateRecruitedSubset returns an error if any recruited pooler is not a
// member of the cohort. All durability policies assume recruited ⊆ cohort so
// that candidacy counts reflect only policy-eligible poolers. This is a
// defensive invariant check; call sites should already enforce it upstream.
func validateRecruitedSubset(cohort, recruited []*clustermetadatapb.ID) error {
	cohortKeys := poolerKeysOf(cohort)
	for _, p := range recruited {
		key := topoclient.ClusterIDString(p)
		if _, ok := cohortKeys[key]; !ok {
			return fmt.Errorf("recruited pooler %s is not in cohort", key)
		}
	}
	return nil
}

// validateMajority returns nil if recruited forms a strict majority of cohort
// (|recruited| >= cohort/2 + 1). This guarantees recruitment-set intersection:
// any two recruitments that each clear a majority must share at least one
// pooler, because if they were disjoint their union would exceed the cohort
// size. Shared intersection + "one accept per term" at the pooler level is
// what makes concurrent recruitments mutually exclusive.
func validateMajority(cohort, recruited []*clustermetadatapb.ID) error {
	majority := len(cohort)/2 + 1
	if len(recruited) < majority {
		return fmt.Errorf("majority not satisfied: recruited %d of %d cohort poolers, need at least %d",
			len(recruited), len(cohort), majority)
	}
	return nil
}

// unrecruitedKeyCount returns the number of distinct keyFn-keys of cohort
// poolers that are not present in recruited. Membership is always determined
// at the pooler level (ClusterIDString); keyFn controls the dimension of
// aggregation for the counted set.
//
// If the count is N or more, those entries could form a rogue quorum on
// their own.
//
//   - MultiCell passes GetCell as keyFn, so multiple un-recruited poolers in the
//     same cell collapse into a single entry — the count is the number of
//     cells with at least one un-recruited pooler.
func unrecruitedKeyCount(cohort, recruited []*clustermetadatapb.ID, keyFn func(*clustermetadatapb.ID) string) int {
	recruitedPoolers := poolerKeysOf(recruited)
	uncovered := make(map[string]struct{})
	for _, p := range cohort {
		if _, ok := recruitedPoolers[topoclient.ClusterIDString(p)]; ok {
			continue
		}
		uncovered[keyFn(p)] = struct{}{}
	}
	return len(uncovered)
}
