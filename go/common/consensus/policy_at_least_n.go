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
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// AtLeastNPolicy requires any N poolers from the cohort to acknowledge writes.
type AtLeastNPolicy struct {
	N int
}

// CheckAchievable returns nil iff the proposed cohort has at least N poolers.
func (p AtLeastNPolicy) CheckAchievable(proposedCohort []*clustermetadatapb.ID) error {
	if len(proposedCohort) < p.N {
		return fmt.Errorf("durability not achievable: proposed cohort has %d poolers, required %d",
			len(proposedCohort), p.N)
	}
	return nil
}

// CheckSufficientRecruitment enforces two proposal-agnostic invariants:
//   - Revocation: fewer than N cohort poolers are absent from recruited, so no
//     N-subset of the cohort avoids our recruitment — no parallel quorum can
//     still form outside our recruited set.
//   - Majority: recruited is a strict majority of cohort, so any two
//     concurrent recruitments must share at least one pooler and, via
//     "one accept per term", cannot both complete.
//
// Candidacy (whether the recruited set satisfies the policy's quorum under
// the *proposed* leadership change) is not checked here — that is a
// proposal-specific concern handled by the leader-appointment layer via
// CheckAchievable.
func (p AtLeastNPolicy) CheckSufficientRecruitment(cohort, recruited []*clustermetadatapb.ID) error {
	if err := validateRecruitedSubset(cohort, recruited); err != nil {
		return err
	}

	// The two obligations below combine into a single binding threshold:
	//
	//   |recruited| >= max(
	//     len(cohort)/2 + 1,   // majority — any two recruitments must share a pooler
	//     len(cohort) - N + 1, // revocation — un-recruited cannot form an N-quorum
	//   )
	//
	// We check each separately so failures report the specific reason.

	// Majority: prevents two coordinators from recruiting disjoint sets at the same term.
	if err := validateMajority(cohort, recruited); err != nil {
		return err
	}

	// Revocation: if N or more cohort poolers are un-recruited, they could form a
	// quorum on their own. Example: AT_LEAST_N with N=2 and a cohort of 5 poolers.
	// After recruiting pooler-1, pooler-2, and pooler-3, we still need to recruit
	// pooler-4 OR pooler-5 — otherwise the 2 unrecruited poolers could form their
	// own 2-pooler quorum that still satisfies the durability policy.
	//
	// Relies on validateRecruitedSubset above: recruited ⊆ cohort, so the
	// length difference equals the un-recruited count.
	unrecruited := len(cohort) - len(recruited)
	if unrecruited >= p.N {
		return fmt.Errorf("revocation not satisfied: %d cohort poolers not recruited, another possible quorum could be formed of %d",
			unrecruited, p.N)
	}
	return nil
}

// BuildLeaderDurabilityPostgresConfig returns the Postgres-level config the
// new leader must apply to satisfy AT_LEAST_N. The standby list is the full
// cohort — AT_LEAST_N is cell-agnostic and including the leader is harmless
// (Postgres ignores its own entry; num_sync = N-1 already accounts for the
// leader's local write counting as 1).
//
// Errors when the cohort is too small to satisfy num_sync.
func (p AtLeastNPolicy) BuildLeaderDurabilityPostgresConfig(
	logger *slog.Logger,
	cohort []*clustermetadatapb.ID,
	leader *clustermetadatapb.ID,
) (*LeaderDurabilityPostgresConfig, error) {
	// N==1 means the primary alone satisfies durability — return an explicit
	// "no sync standbys" config so the new primary clears any stale
	// synchronous_standby_names instead of silently inheriting them.
	if p.N == 1 {
		logger.Info("Configuring leader for local-only durability",
			"policy", "AT_LEAST_N",
			"required_count", p.N)
		return &LeaderDurabilityPostgresConfig{
			SyncCommit:     multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL,
			SyncMethod:     multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:        1,
			SyncStandbyIDs: nil,
		}, nil
	}

	// num_sync = required_count - 1: the primary's own write counts as 1 ack.
	requiredNumSync := p.N - 1
	if requiredNumSync > len(cohort) {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("cannot establish synchronous replication: insufficient cohort members (required %d standbys, available %d)",
				requiredNumSync, len(cohort)))
	}

	logger.Info("Configuring synchronous replication",
		"policy", "AT_LEAST_N",
		"required_count", p.N,
		"num_sync", requiredNumSync,
		"standbys", len(cohort))

	return &LeaderDurabilityPostgresConfig{
		SyncCommit:     multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
		SyncMethod:     multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:        requiredNumSync,
		SyncStandbyIDs: cohort,
	}, nil
}

// Description returns a human-readable summary of the policy.
func (p AtLeastNPolicy) Description() string {
	return fmt.Sprintf("AT_LEAST_N(N=%d)", p.N)
}
