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

package topoclient_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/common/types"
)

func TestClaimShardInitialization(t *testing.T) {
	shardKey := types.ShardKey{Database: "mydb", TableGroup: "tg0", Shard: "0"}

	t.Run("first caller wins the claim", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		won, err := ts.ClaimShardInitialization(ctx, shardKey, "coordinator-1")
		require.NoError(t, err)
		require.True(t, won)
	})

	t.Run("second caller with different ID loses", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		won, err := ts.ClaimShardInitialization(ctx, shardKey, "coordinator-1")
		require.NoError(t, err)
		require.True(t, won)

		won, err = ts.ClaimShardInitialization(ctx, shardKey, "coordinator-2")
		require.NoError(t, err)
		require.False(t, won)
	})

	t.Run("same caller after crash wins again", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		won, err := ts.ClaimShardInitialization(ctx, shardKey, "coordinator-1")
		require.NoError(t, err)
		require.True(t, won)

		// Simulated crash and retry with same ID
		won, err = ts.ClaimShardInitialization(ctx, shardKey, "coordinator-1")
		require.NoError(t, err)
		require.True(t, won)
	})
}
