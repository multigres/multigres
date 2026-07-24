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

// SatisfiedBy returns nil iff poolers has at least N distinct members.
func (p AtLeastNPolicy) SatisfiedBy(poolers []*clustermetadatapb.ID) error {
	poolers = normalizeIDs(poolers)
	if len(poolers) < p.N {
		return fmt.Errorf("durability not satisfied: %d poolers, required %d",
			len(poolers), p.N)
	}
	return nil
}

// BuildSyncReplicationConfig returns the Postgres-level config the primary
// must apply to satisfy AT_LEAST_N. The standby list is the full cohort —
// AT_LEAST_N is cell-agnostic and including the primary is harmless (Postgres
// ignores its own entry; num_sync = N-1 already accounts for the primary's
// local write counting as 1).
//
// Errors when the cohort is too small to satisfy num_sync.
func (p AtLeastNPolicy) BuildSyncReplicationConfig(
	logger *slog.Logger,
	cohort []*clustermetadatapb.ID,
	primary *clustermetadatapb.ID,
) (*SyncReplicationConfig, error) {
	// N==1 means the primary alone satisfies durability — return an explicit
	// "no sync standbys" config so the new primary clears any stale
	// synchronous_standby_names instead of silently inheriting them.
	if p.N == 1 {
		logger.Info("Configuring leader for local-only durability",
			"policy", "AT_LEAST_N",
			"required_count", p.N)
		return &SyncReplicationConfig{
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

	return &SyncReplicationConfig{
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
