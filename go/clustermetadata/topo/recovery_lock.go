// Copyright 2025 Supabase, Inc.
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

package topo

import (
	"context"
	"fmt"
	"time"
)

// RecoveryLockPath returns the canonical lock path for shard-level recovery operations.
// This centralizes the lock path format to ensure all recovery actions use consistent naming.
func RecoveryLockPath(database, tableGroup, shard string) string {
	return fmt.Sprintf("recovery/%s/%s/%s", database, tableGroup, shard)
}

// LockShardForRecovery acquires a distributed lock for recovery operations on a shard.
// It uses LockName which doesn't require the path to exist and has a 24-hour TTL safety net.
//
// Parameters:
//   - ctx: Parent context (used for cancellation, but lock acquisition has its own timeout)
//   - database, tableGroup, shard: Identify the shard to lock
//   - purpose: Description of the recovery operation (for debugging)
//   - lockTimeout: Maximum time to wait for lock acquisition (should be < total operation timeout)
//
// Returns a LockDescriptor that MUST be unlocked when done, even if the operation fails.
// Use a defer with background context for unlock to ensure cleanup:
//
//	lockDesc, err := ts.LockShardForRecovery(ctx, db, tg, shard, "bootstrap", 15*time.Second)
//	if err != nil { return err }
//	defer func() {
//	    unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	    defer cancel()
//	    lockDesc.Unlock(unlockCtx)
//	}()
func (ts *store) LockShardForRecovery(ctx context.Context, database, tableGroup, shard, purpose string, lockTimeout time.Duration) (LockDescriptor, error) {
	lockPath := RecoveryLockPath(database, tableGroup, shard)
	lockContents := fmt.Sprintf("%s for shard %s/%s/%s", purpose, database, tableGroup, shard)

	// Create a timeout context for lock acquisition
	lockCtx, cancel := context.WithTimeout(ctx, lockTimeout)
	defer cancel()

	conn, err := ts.ConnForCell(lockCtx, GlobalCell)
	if err != nil {
		return nil, fmt.Errorf("failed to get topo connection for recovery lock: %w", err)
	}

	lockDesc, err := conn.LockName(lockCtx, lockPath, lockContents)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire recovery lock on %s: %w", lockPath, err)
	}

	return lockDesc, nil
}
