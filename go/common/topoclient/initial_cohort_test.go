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

func TestClaimInitialCohort(t *testing.T) {
	shardKey := types.ShardKey{Database: "mydb", TableGroup: "tg0", Shard: "0"}

	t.Run("first caller gets back sorted proposed IDs", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		proposed := []string{"pooler-c", "pooler-a", "pooler-b"}
		committed, err := ts.ClaimInitialCohort(ctx, shardKey, proposed)
		require.NoError(t, err)
		require.Equal(t, []string{"pooler-a", "pooler-b", "pooler-c"}, committed)
	})

	t.Run("second caller gets back the first caller's committed IDs", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		// First caller wins
		proposed1 := []string{"pooler-z", "pooler-a"}
		committed1, err := ts.ClaimInitialCohort(ctx, shardKey, proposed1)
		require.NoError(t, err)
		require.Equal(t, []string{"pooler-a", "pooler-z"}, committed1)

		// Second caller (racing) gets back the first caller's IDs, not its own proposal
		proposed2 := []string{"pooler-x", "pooler-y", "pooler-z"}
		committed2, err := ts.ClaimInitialCohort(ctx, shardKey, proposed2)
		require.NoError(t, err)
		require.Equal(t, []string{"pooler-a", "pooler-z"}, committed2)
		require.NotEqual(t, proposed2, committed2, "second caller must not get its own proposal back")
	})

	t.Run("caller after simulated crash gets the committed IDs", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		// First call establishes the cohort
		proposed := []string{"pooler-2", "pooler-1"}
		committed, err := ts.ClaimInitialCohort(ctx, shardKey, proposed)
		require.NoError(t, err)
		require.Equal(t, []string{"pooler-1", "pooler-2"}, committed)

		// Simulated crash and retry: same proposal, but record is already committed
		retry, err := ts.ClaimInitialCohort(ctx, shardKey, proposed)
		require.NoError(t, err)
		require.Equal(t, []string{"pooler-1", "pooler-2"}, retry)
	})
}
