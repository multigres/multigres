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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/types"
)

func shardInitClaimPath(shardKey types.ShardKey) string {
	return path.Join(DatabasesPath, shardKey.Database, shardKey.TableGroup, shardKey.Shard, ShardInitClaimFile)
}

// ClaimShardInitialization atomically claims the right to initialize a shard
// using a create-if-not-exists write to the global topology.
//
// The first caller creates the claim and gets back won=true.
// Any subsequent caller checks whether the existing claim belongs to them
// (matching claimedBy), returning won=true for a retry after crash, or
// won=false if a different coordinator already owns the initialization.
func (ts *store) ClaimShardInitialization(ctx context.Context, shardKey types.ShardKey, claimedBy string) (bool, error) {
	filePath := shardInitClaimPath(shardKey)

	_, err := ts.globalTopo.Create(ctx, filePath, []byte(claimedBy))
	if err == nil {
		return true, nil
	}

	if !errors.Is(err, &TopoError{Code: NodeExists}) {
		return false, mterrors.Wrapf(err, "failed to claim shard initialization for %s", shardKey)
	}

	data, _, err := ts.globalTopo.Get(ctx, filePath)
	if err != nil {
		return false, mterrors.Wrapf(err, "failed to read shard init claim for %s", shardKey)
	}

	return string(data) == claimedBy, nil
}
