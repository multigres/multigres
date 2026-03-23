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

// backup_lock.go provides distributed lease-based locking for pgbackrest write
// operations (backup and stanza-create).
//
// pgbackrest does not support safe concurrent writes to the same backup repository.
// Since multiple multipooler instances serve the same shard, and any of them may
// initiate a backup, a distributed lock is needed to ensure at most one
// pgbackrest write operation runs per shard at any time.
//
// Locks are stored in the global topology (etcd) with a TTL-based lease. The
// holder's lease is kept alive automatically; if the holder crashes, the TTL
// expires and the lock is released. A background monitor polls the lock state
// and cancels the holder's context on lease loss (ErrLeaseLost).
//
// Two acquisition modes are supported:
//   - WithBackupLease: fails immediately if the lock is already held.
//   - WithStolenBackupLease: revokes the existing holder's lease before
//     acquiring, with a grace period so the old holder can terminate its
//     pgbackrest process cleanly. This is used for on-demand backups where the
//     most recent request should always win.
package topoclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/types"
)

// BackupsPath is the path component for backup locks in the topology hierarchy.
const BackupsPath = "backups"

// BackupLeaseTTL is the TTL for backup locks. This is a safety net for crash
// recovery — if a holder crashes without releasing, the lock expires after this
// duration. KeepAlive refreshes the lease automatically during normal operation.
const BackupLeaseTTL = 30 * time.Second

// BackupLeaseStealGracePeriod is how long a stealer waits after revoking the
// old holder's lease before proceeding. This gives the old holder time to
// terminate its pgbackrest process gracefully.
const BackupLeaseStealGracePeriod = 5 * time.Second

// BackupLeaseCheckInterval is how often the lease monitor polls to detect
// lease loss.
const BackupLeaseCheckInterval = 10 * time.Second

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

// RevokeBackup forcefully removes the backup lock for the specified shard,
// regardless of who holds it. This is used by the steal protocol during failover
// or manual intervention. The old holder's KeepAlive channel will be closed,
// triggering process cleanup on their side.
//
// Returns nil if no lock exists.
func (ts *store) RevokeBackup(ctx context.Context, shardKey types.ShardKey) error {
	bl := &backupLock{ShardKey: shardKey}
	if ts.globalTopo == nil {
		return errors.New("no global cell connection on the topo server")
	}
	return ts.globalTopo.RevokeLockWithLease(ctx, bl.Path())
}

// WithBackupLease acquires a backup lease, runs fn, and releases the lease
// when fn returns. The context passed to fn is cancelled immediately if the
// lease is lost; check context.Cause(ctx) == ErrLeaseLost to distinguish.
//
// Fails immediately if the lease is already held. Use WithStolenBackupLease
// to steal.
func (ts *store) WithBackupLease(
	ctx context.Context,
	shardKey types.ShardKey,
	holderID string,
	operation string,
	logger *slog.Logger,
	fn func(ctx context.Context) error,
) error {
	action := operation + " by " + holderID
	acquire, revoke, check := ts.backupLeaseOps(shardKey)

	logger.InfoContext(ctx, "Acquiring backup lease",
		"shard", shardKey.String(), "holder", holderID, "operation", operation)

	err := WithLease(ctx, action, acquire, revoke, check, fn,
		WithLeaseCheckInterval(BackupLeaseCheckInterval),
	)
	if err != nil {
		logger.InfoContext(ctx, "Backup lease operation completed with error",
			"shard", shardKey.String(), "holder", holderID, "error", err)
	} else {
		logger.InfoContext(ctx, "Backup lease operation completed",
			"shard", shardKey.String(), "holder", holderID, "operation", operation)
	}
	return err
}

// WithStolenBackupLease acquires a backup lease (stealing if necessary), runs
// fn, and releases the lease when fn returns. The most recent backup request
// always wins — if another holder has the lease, it is revoked.
//
// We do *not* retry stealing of leases. In a situation where there are multiple
// contenders for the lease, retries of steals would cause thrashing of the lease.
func (ts *store) WithStolenBackupLease(
	ctx context.Context,
	shardKey types.ShardKey,
	stealerID string,
	operation string,
	logger *slog.Logger,
	fn func(ctx context.Context) error,
) error {
	action := operation + " by " + stealerID
	acquire, revoke, check := ts.backupLeaseOps(shardKey)

	logger.InfoContext(ctx, "Acquiring backup lease (steal-enabled)",
		"shard", shardKey.String(), "stealer", stealerID, "operation", operation)

	err := WithLease(ctx, action, acquire, revoke, check, fn,
		WithStealGracePeriod(BackupLeaseStealGracePeriod),
		WithLeaseCheckInterval(BackupLeaseCheckInterval),
	)
	if err != nil {
		logger.InfoContext(ctx, "Backup lease operation completed with error",
			"shard", shardKey.String(), "stealer", stealerID, "error", err)
	} else {
		logger.InfoContext(ctx, "Backup lease operation completed",
			"shard", shardKey.String(), "stealer", stealerID, "operation", operation)
	}
	return err
}

// backupLeaseOps returns the acquire/revoke/check closures for backup lease operations.
func (ts *store) backupLeaseOps(shardKey types.ShardKey) (LeaseAcquirer, LeaseRevoker, LeaseChecker) {
	return func(ctx context.Context, action string) (context.Context, func(*error), error) {
			return ts.TryLockBackup(ctx, shardKey, action)
		},
		func(ctx context.Context) error { return ts.RevokeBackup(ctx, shardKey) },
		func(ctx context.Context) error { return AssertBackupLockHeld(ctx, shardKey) }
}
