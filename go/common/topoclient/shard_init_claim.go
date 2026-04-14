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

package topoclient

import (
	"context"
	"errors"
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func shardInitClaimPath(shardKey types.ShardKey) string {
	return path.Join(DatabasesPath, shardKey.Database, shardKey.TableGroup, shardKey.Shard, ShardInitClaimFile)
}

// ClaimShardInitialization atomically claims the right to initialize a shard
// using a create-if-not-exists write to the global topology.
//
// The claim persists both the claimer's identity and the proposed cohort so that
// a crash-retry reuses the same cohort for idempotency.
//
// Returns won=true and the committed cohort if this caller created the claim or
// is resuming its own prior claim. Returns won=false if a different coordinator
// already owns the initialization.
func (ts *store) ClaimShardInitialization(ctx context.Context, shardKey types.ShardKey, claimerID *clustermetadatapb.ID, proposedCohort []*clustermetadatapb.ID) (bool, []*clustermetadatapb.ID, error) {
	claim := &clustermetadatapb.ShardInitClaim{
		ClaimerId:     claimerID,
		CohortMembers: proposedCohort,
	}

	data, err := proto.Marshal(claim)
	if err != nil {
		return false, nil, mterrors.Wrapf(err, "failed to marshal shard init claim for %s", shardKey)
	}

	filePath := shardInitClaimPath(shardKey)

	_, err = ts.globalTopo.Create(ctx, filePath, data)
	if err == nil {
		return true, proposedCohort, nil
	}

	if !errors.Is(err, &TopoError{Code: NodeExists}) {
		return false, nil, mterrors.Wrapf(err, "failed to claim shard initialization for %s", shardKey)
	}

	// Claim already exists — read it back and check ownership.
	existing, _, err := ts.globalTopo.Get(ctx, filePath)
	if err != nil {
		return false, nil, mterrors.Wrapf(err, "failed to read shard init claim for %s", shardKey)
	}

	committed := &clustermetadatapb.ShardInitClaim{}
	if err := proto.Unmarshal(existing, committed); err != nil {
		return false, nil, mterrors.Wrapf(err, "failed to unmarshal shard init claim for %s", shardKey)
	}

	if ClusterIDString(committed.ClaimerId) == ClusterIDString(claimerID) {
		return true, committed.CohortMembers, nil
	}

	return false, nil, nil
}
