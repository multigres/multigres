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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func bootstrapMarkerPath(shardRef *clustermetadatapb.ShardRef) string {
	return path.Join(DatabasesPath, shardRef.Database, shardRef.TableGroup, shardRef.Shard, BootstrapMarkerFile)
}

// MarkBootstrapInProgress records in the global topology that holder is running
// the shard's first-backup bootstrap. The marker stays until ClearBootstrapMarker.
func (ts *store) MarkBootstrapInProgress(ctx context.Context, shardRef *clustermetadatapb.ShardRef, holder string) error {
	if _, err := ts.globalTopo.Update(ctx, bootstrapMarkerPath(shardRef), []byte(holder), nil); err != nil {
		return mterrors.Wrapf(err, "failed to write bootstrap marker for %s", types.FormatShardRef(shardRef))
	}
	return nil
}

// BootstrapInProgress reports whether a bootstrap marker exists for the shard.
func (ts *store) BootstrapInProgress(ctx context.Context, shardRef *clustermetadatapb.ShardRef) (bool, error) {
	_, _, err := ts.globalTopo.Get(ctx, bootstrapMarkerPath(shardRef))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, &TopoError{Code: NoNode}):
		return false, nil
	default:
		return false, mterrors.Wrapf(err, "failed to read bootstrap marker for %s", types.FormatShardRef(shardRef))
	}
}

// ClearBootstrapMarker removes the shard's bootstrap marker. A missing marker is not an error.
func (ts *store) ClearBootstrapMarker(ctx context.Context, shardRef *clustermetadatapb.ShardRef) error {
	err := ts.globalTopo.Delete(ctx, bootstrapMarkerPath(shardRef), nil)
	if err != nil && !errors.Is(err, &TopoError{Code: NoNode}) {
		return mterrors.Wrapf(err, "failed to clear bootstrap marker for %s", types.FormatShardRef(shardRef))
	}
	return nil
}
