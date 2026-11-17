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

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// checkBootstrapMarker verifies the bootstrap marker is per shard, can be set
// again by another holder, and clearing it is idempotent.
func checkBootstrapMarker(t *testing.T, ctx context.Context, ts topoclient.Store) {
	shardRef := &clustermetadatapb.ShardRef{Database: "markerdb", TableGroup: "tg0", Shard: "0"}
	otherShard := &clustermetadatapb.ShardRef{Database: "markerdb", TableGroup: "tg0", Shard: "1"}

	t.Log("===      no marker before one is set")
	present, err := ts.BootstrapInProgress(ctx, shardRef)
	require.NoError(t, err)
	assert.False(t, present)

	t.Log("===      marker is set and visible")
	require.NoError(t, ts.MarkBootstrapInProgress(ctx, shardRef, "pooler-1"))
	present, err = ts.BootstrapInProgress(ctx, shardRef)
	require.NoError(t, err)
	assert.True(t, present)

	t.Log("===      marker is per shard")
	present, err = ts.BootstrapInProgress(ctx, otherShard)
	require.NoError(t, err)
	assert.False(t, present)

	t.Log("===      another holder can set it again")
	require.NoError(t, ts.MarkBootstrapInProgress(ctx, shardRef, "pooler-2"))
	present, err = ts.BootstrapInProgress(ctx, shardRef)
	require.NoError(t, err)
	assert.True(t, present)

	t.Log("===      clear removes it, and clearing again is not an error")
	require.NoError(t, ts.ClearBootstrapMarker(ctx, shardRef))
	present, err = ts.BootstrapInProgress(ctx, shardRef)
	require.NoError(t, err)
	assert.False(t, present)
	require.NoError(t, ts.ClearBootstrapMarker(ctx, shardRef))
}
