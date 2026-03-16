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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/common/types"
)

func TestTryLockBackup(t *testing.T) {
	ctx := t.Context()

	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(100 * time.Millisecond)

	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	defer ts.Close()

	shardKey := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "0"}

	// Acquire backup lock
	lockCtx, unlock, err := ts.TryLockBackup(ctx, shardKey, "backup by pooler-1")
	require.NoError(t, err)

	// Verify the lock is held
	err = topoclient.AssertBackupLockHeld(lockCtx, shardKey)
	require.NoError(t, err)

	// Second acquire should fail immediately (not block)
	_, _, err = ts.TryLockBackup(ctx, shardKey, "backup by pooler-2")
	require.Error(t, err)

	// Different shard should succeed
	shardKey2 := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "1"}
	_, unlock2, err := ts.TryLockBackup(ctx, shardKey2, "backup by pooler-1")
	require.NoError(t, err)
	var unlockErr error
	unlock2(&unlockErr)
	require.NoError(t, unlockErr)

	// Release first lock
	unlock(&unlockErr)
	require.NoError(t, unlockErr)

	// Should be acquirable again after release
	_, unlock3, err := ts.TryLockBackup(ctx, shardKey, "backup by pooler-2")
	require.NoError(t, err)
	unlock3(&unlockErr)
	require.NoError(t, unlockErr)
}

func TestForceUnlockBackup(t *testing.T) {
	ctx := t.Context()

	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(100 * time.Millisecond)

	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	defer ts.Close()

	shardKey := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "0"}

	// Acquire backup lock
	_, _, err := ts.TryLockBackup(ctx, shardKey, "backup by pooler-1")
	require.NoError(t, err)

	// Force unlock should succeed
	err = ts.ForceUnlockBackup(ctx, shardKey)
	require.NoError(t, err)

	// Should be acquirable again after force unlock
	_, unlock, err := ts.TryLockBackup(ctx, shardKey, "backup by pooler-2")
	require.NoError(t, err)
	var unlockErr error
	unlock(&unlockErr)
	require.NoError(t, unlockErr)

	// Force unlock when no lock exists should be a no-op
	err = ts.ForceUnlockBackup(ctx, shardKey)
	require.NoError(t, err)
}

func TestAssertBackupLockHeld(t *testing.T) {
	ctx := t.Context()

	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(100 * time.Millisecond)

	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	defer ts.Close()

	shardKey := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "0"}

	// Assert should fail when no lock is held
	err := topoclient.AssertBackupLockHeld(ctx, shardKey)
	require.Error(t, err)

	// Acquire and check
	lockCtx, unlock, err := ts.TryLockBackup(ctx, shardKey, "backup")
	require.NoError(t, err)

	err = topoclient.AssertBackupLockHeld(lockCtx, shardKey)
	require.NoError(t, err)

	// Force unlock — assert should fail on memorytopo after force unlock
	err = ts.ForceUnlockBackup(ctx, shardKey)
	require.NoError(t, err)

	err = topoclient.AssertBackupLockHeld(lockCtx, shardKey)
	require.Error(t, err)

	// Clean up the lock descriptor
	var unlockErr error
	unlock(&unlockErr)
}
