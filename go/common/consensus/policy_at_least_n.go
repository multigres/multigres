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
	// AsyncFallback governs what BuildLeaderDurabilityPostgresConfig does when
	// no eligible standbys are available to satisfy synchronous replication.
	// REJECT (or unset) refuses to promote without sync; ALLOW degrades to
	// async-only.
	AsyncFallback clustermetadatapb.AsyncReplicationFallbackMode
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

// BuildLeaderDurabilityPostgresConfig returns the leader-side Postgres config
// needed to satisfy AT_LEAST_N. The standby list is the full cohort —
// AT_LEAST_N is cell-agnostic and the candidate is part of the list (Postgres
// ignores the primary's own entry for ack purposes; num_sync = N-1 already
// accounts for the primary's local write counting as 1).
func (p AtLeastNPolicy) BuildLeaderDurabilityPostgresConfig(
	logger *slog.Logger,
	cohort []*clustermetadatapb.ID,
	candidate *clustermetadatapb.ID,
) (*LeaderDurabilityPostgresConfig, error) {
	asyncFallback := p.AsyncFallback
	if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_UNKNOWN {
		asyncFallback = clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT
	}

	// N==1 means the primary alone satisfies durability; async is sufficient.
	if p.N == 1 {
		logger.Info("Skipping synchronous replication configuration",
			"policy", "AT_LEAST_N",
			"required_count", p.N,
			"reason", "async replication sufficient for quorum")
		return nil, nil
	}

	if len(cohort) == 0 {
		if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				fmt.Sprintf("cannot establish synchronous replication: no cohort members available (required %d standbys, async_fallback=REJECT)",
					p.N-1))
		}
		logger.Info("Skipping synchronous replication configuration",
			"policy", "AT_LEAST_N",
			"required_count", p.N,
			"reason", "no cohort members available, async fallback enabled")
		return nil, nil
	}

	// num_sync = required_count - 1: the primary's own write counts as 1 ack.
	requiredNumSync := p.N - 1
	if requiredNumSync > len(cohort) {
		if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				fmt.Sprintf("cannot establish synchronous replication: insufficient cohort members (required %d standbys, available %d, async_fallback=REJECT)",
					requiredNumSync, len(cohort)))
		}
		logger.Warn("Not enough cohort members for required sync count, using all available",
			"policy", "AT_LEAST_N",
			"required_num_sync", requiredNumSync,
			"available_standbys", len(cohort))
	}

	numSync := min(requiredNumSync, len(cohort))

	logger.Info("Configuring synchronous replication",
		"policy", "AT_LEAST_N",
		"required_count", p.N,
		"num_sync", numSync,
		"standbys", len(cohort))

	return &LeaderDurabilityPostgresConfig{
		SyncCommit:     multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
		SyncMethod:     multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:        numSync,
		SyncStandbyIDs: cohort,
	}, nil
}

// Description returns a human-readable summary of the policy.
func (p AtLeastNPolicy) Description() string {
	return fmt.Sprintf("AT_LEAST_N(N=%d)", p.N)
}
