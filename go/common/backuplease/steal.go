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

package backuplease

import (
	"context"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// Steal acquires a backup lease for the given shard. If no lock is held, it
// acquires immediately. If another holder has the lock, it revokes the existing
// lock, waits for the old holder to clean up, and then acquires a new lease.
//
// This is the normal path for backups — the most recent backup request always wins.
//
// Protocol when a lock exists:
//  1. Revoke existing lock via ForceUnlockBackup
//  2. Wait StealGracePeriod (5s) for old holder to finish killing its pgbackrest process
//  3. Acquire new lease via TryLockBackup
//  4. If acquisition fails (another process beat us), return error — do not retry
func Steal(
	ctx context.Context,
	store topoclient.Store,
	shardKey types.ShardKey,
	stealerID string,
	operation string,
	logger *slog.Logger,
) (context.Context, *Lease, error) {
	// Fast path: try to acquire without stealing. If no lock is held, this
	// succeeds immediately without the 5s grace period.
	lockCtx, lease, err := Acquire(ctx, store, shardKey, stealerID, operation, logger)
	if err == nil {
		return lockCtx, lease, nil
	}

	// Lock is held by someone else — steal it.
	logger.InfoContext(ctx, "Backup lease held by another process, stealing",
		"shard", shardKey.String(),
		"stealer", stealerID,
		"operation", operation)

	// Step 1: Revoke existing lock
	if err := store.ForceUnlockBackup(ctx, shardKey); err != nil {
		logger.WarnContext(ctx, "Failed to force-unlock backup lease during steal",
			"shard", shardKey.String(),
			"stealer", stealerID,
			"error", err)
		return ctx, nil, err
	}

	eventlog.Emit(ctx, logger, eventlog.Started, eventlog.BackupLeaseStolen{
		Stealer: stealerID,
	})

	// Step 2: Wait for old holder to clean up
	logger.InfoContext(ctx, "Waiting for old holder cleanup",
		"shard", shardKey.String(),
		"grace_period", StealGracePeriod)

	if err := telemetry.WithSpan(ctx, "backup-lease/steal-wait", func(ctx context.Context) error {
		select {
		case <-time.After(StealGracePeriod):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		return ctx, nil, err
	}

	// Step 3: Acquire new lease
	lockCtx, lease, err = Acquire(ctx, store, shardKey, stealerID, operation, logger)
	if err != nil {
		logger.WarnContext(ctx, "Failed to acquire backup lease after steal",
			"shard", shardKey.String(),
			"stealer", stealerID,
			"error", err)
		return ctx, nil, err
	}

	logger.InfoContext(ctx, "Successfully stole backup lease",
		"shard", shardKey.String(),
		"stealer", stealerID,
		"operation", operation)

	return lockCtx, lease, nil
}
