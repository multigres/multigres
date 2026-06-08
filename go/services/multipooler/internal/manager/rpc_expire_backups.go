// Copyright 2026 Supabase, Inc.
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

package manager

import (
	"context"
)

// ExpireBackups runs pgbackrest expire to remove backups that exceed the
// configured retention policy. This is safe to call at any time.
// Returns the IDs of backups that were removed.
func (pm *MultiPoolerManager) ExpireBackups(ctx context.Context, overrides map[string]string) ([]string, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "ExpireBackups")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Acquire the distributed backup lease (non-stealing).
	// Expire must not run concurrently with backup/stanza-create on any node.
	// Uses WithBackupLease (not WithStolenBackupLease) because expire should
	// not preempt an in-progress backup — it can wait or fail fast.
	var expiredIDs []string
	err = pm.topoClient.WithBackupLease(ctx, pm.shardKey(), pm.record.Id().Name, "expire", pm.logger, func(ctx context.Context) error {
		var expireErr error
		expiredIDs, expireErr = pm.backup.Expire(ctx, overrides)
		return expireErr
	})
	return expiredIDs, err
}
