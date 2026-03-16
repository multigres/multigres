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
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/types"
)

// BackupsPath is the path component for backup locks in the topology hierarchy.
const BackupsPath = "backups"

// BackupLeaseTTL is the TTL for backup locks. This is a safety net for crash
// recovery — if a holder crashes without releasing, the lock expires after this
// duration. KeepAlive refreshes the lease automatically during normal operation.
const BackupLeaseTTL = 30 * time.Second

type backupLock struct {
	types.ShardKey
}

var _ iTopoLock = (*backupLock)(nil)

func (b *backupLock) Type() string {
	return "backup"
}

func (b *backupLock) ResourceName() string {
	return b.ShardKey.String()
}

func (b *backupLock) Path() string {
	return fmt.Sprintf("%s/%s/%s/%s/lock", BackupsPath, b.Database, b.TableGroup, b.Shard)
}

// AssertBackupLockHeld checks that the backup lock is held for the given shard
// in the provided context. Returns an error if it is not.
func AssertBackupLockHeld(ctx context.Context, shardKey types.ShardKey) error {
	return checkLocked(ctx, &backupLock{ShardKey: shardKey})
}

// TryLockBackup attempts to acquire a backup lock on the specified shard without
// blocking. If a backup lock already exists (another backup operation is running),
// it returns an error immediately.
//
// The lock uses a 30s TTL with KeepAlive as a safety net for crash recovery.
// Normal release is done by calling the returned unlock function.
func (ts *store) TryLockBackup(ctx context.Context, shardKey types.ShardKey, action string) (context.Context, func(*error), error) {
	bl := &backupLock{ShardKey: shardKey}
	// Use NamedNonBlocking semantics (fail-fast, creates path) with custom TTL
	return ts.internalLock(ctx, bl, action,
		WithType(NamedNonBlockingWithTTL),
		WithTTL(BackupLeaseTTL),
	)
}

// ForceUnlockBackup forcefully removes the backup lock for the specified shard,
// regardless of who holds it. This is used by the steal protocol during failover
// or manual intervention. The old holder's KeepAlive channel will be closed,
// triggering process cleanup on their side.
//
// Returns nil if no lock exists.
func (ts *store) ForceUnlockBackup(ctx context.Context, shardKey types.ShardKey) error {
	bl := &backupLock{ShardKey: shardKey}
	if ts.globalTopo == nil {
		return errors.New("no global cell connection on the topo server")
	}
	return ts.globalTopo.RevokeLockEphemeral(ctx, bl.Path())
}
