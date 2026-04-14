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
	"github.com/multigres/multigres/go/common/types"
)

// checkShardInitClaim verifies ClaimShardInitialization semantics:
// first caller wins, different caller loses, same caller after crash wins.
func checkShardInitClaim(t *testing.T, ctx context.Context, ts topoclient.Store) {
	shardKey := types.ShardKey{Database: "claimdb", TableGroup: "tg0", Shard: "0"}

	t.Log("===      first caller wins the claim")
	won, err := ts.ClaimShardInitialization(ctx, shardKey, "coordinator-1")
	require.NoError(t, err)
	assert.True(t, won, "first caller should win")

	t.Log("===      same caller wins again (crash recovery)")
	won, err = ts.ClaimShardInitialization(ctx, shardKey, "coordinator-1")
	require.NoError(t, err)
	assert.True(t, won, "same caller should win on retry")

	t.Log("===      different caller loses")
	won, err = ts.ClaimShardInitialization(ctx, shardKey, "coordinator-2")
	require.NoError(t, err)
	assert.False(t, won, "different caller should lose")

	t.Log("===      independent shard can be claimed separately")
	shardKey2 := types.ShardKey{Database: "claimdb", TableGroup: "tg0", Shard: "1"}
	won, err = ts.ClaimShardInitialization(ctx, shardKey2, "coordinator-2")
	require.NoError(t, err)
	assert.True(t, won, "different shard should be claimable independently")
}
