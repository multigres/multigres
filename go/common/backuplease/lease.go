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

// Package backuplease provides distributed lease-based locking for pgbackrest
// write operations (backup and stanza-create). It ensures at most one pgbackrest
// write operation runs per shard at any time, preventing concurrent access to
// the backup repository.
package backuplease

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// Lease represents an active backup lease. The holder must monitor the Lost()
// channel during the backup operation and stop immediately if the lease is lost.
type Lease struct {
	shardKey  types.ShardKey
	holderID  string
	operation string
	logger    *slog.Logger

	unlock   func(*error)
	cancel   context.CancelFunc
	span     trace.Span
	lost     chan struct{}
	lostOnce sync.Once
}

// Lost returns a channel that is closed when the lease is lost (e.g., stolen
// by another process or expired due to connectivity loss).
func (l *Lease) Lost() <-chan struct{} {
	return l.lost
}

// Release releases the lease normally. This should be called when the backup
// operation completes (successfully or with error).
func (l *Lease) Release(ctx context.Context) {
	l.cancel() // stop the monitor goroutine
	l.span.End()
	var err error
	l.unlock(&err)
	if err != nil {
		l.logger.WarnContext(ctx, "Failed to release backup lease",
			"shard", l.shardKey.String(),
			"holder", l.holderID,
			"error", err)
	} else {
		l.logger.InfoContext(ctx, "Released backup lease",
			"shard", l.shardKey.String(),
			"holder", l.holderID,
			"operation", l.operation)
	}
}

// Acquire acquires a backup lease for the given shard. If a lease already exists
// (another backup operation is running), it returns an error immediately — it does
// not steal. Use Steal for that.
//
// The returned context contains the lock information and should be used by callers
// so that CheckBackupLocked can verify the lease is held. The returned Lease starts
// a background goroutine that monitors the lease health. If the lease is lost
// (e.g., due to network partition or steal), the Lost() channel is closed.
func Acquire(
	ctx context.Context,
	store topoclient.Store,
	shardKey types.ShardKey,
	holderID string,
	operation string,
	logger *slog.Logger,
) (context.Context, *Lease, error) {
	logger.InfoContext(ctx, "Acquiring backup lease",
		"shard", shardKey.String(),
		"holder", holderID,
		"operation", operation)

	lockCtx, unlock, err := store.TryLockBackup(ctx, shardKey, operation+" by "+holderID)
	if err != nil {
		logger.InfoContext(ctx, "Failed to acquire backup lease",
			"shard", shardKey.String(),
			"holder", holderID,
			"error", err)
		return ctx, nil, err
	}

	logger.InfoContext(ctx, "Acquired backup lease",
		"shard", shardKey.String(),
		"holder", holderID,
		"operation", operation)

	// Start a span covering the full lease lifetime (acquire → release/revocation).
	lockCtx, span := telemetry.Tracer().Start(lockCtx, "backup-lease/held",
		trace.WithAttributes(
			attribute.String("shard", shardKey.String()),
			attribute.String("holder", holderID),
			attribute.String("operation", operation),
		))

	monitorCtx, cancel := context.WithCancel(lockCtx)
	lease := &Lease{
		shardKey:  shardKey,
		holderID:  holderID,
		operation: operation,
		logger:    logger,
		unlock:    unlock,
		cancel:    cancel,
		span:      span,
		lost:      make(chan struct{}),
	}

	go lease.monitor(monitorCtx)

	return lockCtx, lease, nil
}

// monitor periodically checks if the lease is still held. If the check fails,
// it closes the lost channel to signal the holder.
func (l *Lease) monitor(ctx context.Context) {
	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Use a fresh context for the check — we don't want the parent
			// context's cancellation to interfere with lease monitoring.
			checkCtx := context.WithoutCancel(ctx)
			if err := topoclient.AssertBackupLockHeld(checkCtx, l.shardKey); err != nil {
				l.logger.WarnContext(ctx, "Backup lease lost",
					"shard", l.shardKey.String(),
					"holder", l.holderID,
					"error", err)
				eventlog.Emit(ctx, l.logger, eventlog.Failed, eventlog.BackupLeaseLost{
					Holder: l.holderID,
				})
				l.span.End()
				l.lostOnce.Do(func() { close(l.lost) })
				return
			}
		}
	}
}
