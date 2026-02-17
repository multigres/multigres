// Copyright 2025 Supabase, Inc.
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
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// BuildSyncReplicationConfig creates synchronous replication configuration based on the quorum policy.
// Returns nil if synchronous replication should not be configured (required_count=1 or no standbys).
// For MULTI_CELL_ANY_N policies, excludes standbys in the same cell as the candidate (primary).
func BuildSyncReplicationConfig(
	logger *slog.Logger,
	quorumRule *clustermetadatapb.QuorumRule,
	standbys []*multiorchdatapb.PoolerHealthState,
	candidate *multiorchdatapb.PoolerHealthState,
) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest, error) {
	requiredCount := int(quorumRule.RequiredCount)

	// Determine async fallback mode (default to REJECT if unset)
	asyncFallback := quorumRule.AsyncFallback
	if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_UNKNOWN {
		asyncFallback = clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT
	}

	// If required_count is 1, don't configure synchronous replication
	// With required_count=1, only the primary itself is needed for quorum, so async replication is sufficient
	if requiredCount == 1 {
		logger.Info("Skipping synchronous replication configuration",
			"required_count", requiredCount,
			"standbys_count", len(standbys),
			"reason", "async replication sufficient for quorum")
		return nil, nil
	}

	// If there are no standbys, check async fallback mode
	if len(standbys) == 0 {
		if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				fmt.Sprintf("cannot establish synchronous replication: no standbys available (required %d standbys, async_fallback=REJECT)",
					requiredCount-1))
		}
		logger.Info("Skipping synchronous replication configuration",
			"required_count", requiredCount,
			"standbys_count", 0,
			"reason", "async replication sufficient for quorum")
		return nil, nil
	}

	// For MULTI_CELL_ANY_N, filter out standbys in the same cell as the primary
	// This ensures that synchronous replication requires acknowledgment from different cells
	eligibleStandbys := standbys
	if quorumRule.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N {
		candidateCell := candidate.MultiPooler.Id.Cell
		filtered := make([]*multiorchdatapb.PoolerHealthState, 0, len(standbys))
		for _, standby := range standbys {
			if standby.MultiPooler.Id.Cell != candidateCell {
				filtered = append(filtered, standby)
			}
		}
		eligibleStandbys = filtered

		logger.Info("Filtered standbys for MULTI_CELL_ANY_N",
			"candidate_cell", candidate.MultiPooler.Id.Cell,
			"total_standbys", len(standbys),
			"eligible_standbys", len(eligibleStandbys),
			"excluded_same_cell", len(standbys)-len(eligibleStandbys))

		// If no eligible standbys remain after filtering, check async fallback mode
		if len(eligibleStandbys) == 0 {
			if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT {
				return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
					fmt.Sprintf("cannot establish synchronous replication: no eligible standbys in different cells (candidate_cell=%s, async_fallback=REJECT)",
						candidate.MultiPooler.Id.Cell))
			}
			logger.Warn("No eligible standbys in different cells, using async replication",
				"candidate_cell", candidate.MultiPooler.Id.Cell,
				"total_standbys", len(standbys))
			return nil, nil
		}
	}

	// Calculate num_sync: required_count - 1 (since primary counts as 1)
	// This ensures that primary + num_sync standbys = required_count total nodes/cells
	requiredNumSync := requiredCount - 1

	// Check if we have enough standbys to meet the requirement
	if requiredNumSync > len(eligibleStandbys) {
		if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				fmt.Sprintf("cannot establish synchronous replication: insufficient standbys (required %d standbys, available %d, async_fallback=REJECT)",
					requiredNumSync, len(eligibleStandbys)))
		}
		logger.Warn("Not enough standbys for required sync count, using all available",
			"required_num_sync", requiredNumSync,
			"available_standbys", len(eligibleStandbys))
	}

	// Cap num_sync at the number of available eligible standbys
	numSync := int32(requiredNumSync)
	if int(numSync) > len(eligibleStandbys) {
		numSync = int32(len(eligibleStandbys))
	}

	// Convert standby nodes to IDs
	standbyIDs := make([]*clustermetadatapb.ID, len(eligibleStandbys))
	for i, standby := range eligibleStandbys {
		standbyIDs[i] = standby.MultiPooler.Id
	}

	logger.Info("Configuring synchronous replication",
		"quorum_type", quorumRule.QuorumType,
		"required_count", requiredCount,
		"num_sync", numSync,
		"total_standbys", len(eligibleStandbys))

	return &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
		SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
		SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:           numSync,
		StandbyIds:        standbyIDs,
		ReloadConfig:      true,
	}, nil
}
