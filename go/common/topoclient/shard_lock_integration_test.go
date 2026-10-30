// Copyright 2024 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modifications Copyright 2025 Supabase, Inc.

package topoclient_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const testLockTimeout = 100 * time.Millisecond

// TestTopoShardLock tests shard lock operations.
func TestTopoShardLock(t *testing.T) {
	ctx := t.Context()

	// Create a config with a short lock timeout for faster tests
	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(testLockTimeout)

	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	defer ts.Close()

	shardRef1 := &clustermetadatapb.ShardRef{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "0"}
	shardRef2 := &clustermetadatapb.ShardRef{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "1"}

	origCtx := ctx
	ctx, unlock, err := ts.LockShard(origCtx, shardRef1, "db/default/0")
	require.NoError(t, err)

	// locking the same key again, without unlocking, should return an error
	_, _, err2 := ts.LockShard(ctx, shardRef1, "db/default/0")
	require.ErrorContains(t, err2, "already held")

	// Check that we have the shard lock shouldn't return an error
	err = topoclient.CheckShardLocked(ctx, shardRef1)
	require.NoError(t, err)

	// Check that we have the shard lock for the other shard should return an error
	err = topoclient.CheckShardLocked(ctx, shardRef2)
	require.ErrorContains(t, err, "is not locked")

	// Check we can acquire a shard lock for the other shard
	ctx2, unlock2, err := ts.LockShard(ctx, shardRef2, "db/default/1")
	require.NoError(t, err)
	defer unlock2(&err)

	// Unlock the first shard
	unlock(&err)

	// Check shard locked output for both shards
	err = topoclient.CheckShardLocked(ctx2, shardRef1)
	require.ErrorContains(t, err, "is not locked")
	err = topoclient.CheckShardLocked(ctx2, shardRef2)
	require.NoError(t, err)

	// confirm that the lock can be re-acquired after unlocking
	_, unlock, err = ts.TryLockShard(origCtx, shardRef1, "db/default/0")
	require.NoError(t, err)
	defer unlock(&err)
}
