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

// Package heartbeat is responsible for reading and writing heartbeats
// to the heartbeat table.
package heartbeat

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/multipooler/executor"
)

// Make these modifiable for testing.
var (
	defaultHeartbeatInterval = 1 * time.Second
)

// Writer runs on primary databases and writes heartbeats to the heartbeat
// table at regular intervals.
type Writer struct {
	queryService executor.InternalQueryService
	logger       *slog.Logger
	shardID      []byte
	poolerID     string
	interval     time.Duration
	now          func() time.Time

	mu     sync.Mutex
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
	timer  *time.Timer
	wg     sync.WaitGroup

	writes      atomic.Int64
	writeErrors atomic.Int64
}

// NewWriter creates a new heartbeat writer.
//
// We do not support on-demand or disabled heartbeats at this time.
func NewWriter(queryService executor.InternalQueryService, logger *slog.Logger, shardID []byte, poolerID string, intervalMs int) *Writer {
	interval := time.Duration(intervalMs) * time.Millisecond
	if intervalMs <= 0 {
		interval = defaultHeartbeatInterval
	}
	return &Writer{
		queryService: queryService,
		logger:       logger,
		shardID:      shardID,
		poolerID:     poolerID,
		interval:     interval,
		now:          time.Now,
	}
}

// Open starts the heartbeat writer.
func (w *Writer) Open() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.timer != nil {
		return // Already open
	}

	w.logger.Info("Heartbeat Writer: opening")

	//nolint:gocritic // TODO: use ctxutil.Detach() after #393 merges
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.closed = false
	w.scheduleNextWrite()
}

// scheduleNextWrite schedules the next heartbeat write.
// Must be called while holding w.mu.
func (w *Writer) scheduleNextWrite() {
	w.timer = time.AfterFunc(w.interval, w.writeHeartbeat)
}

// Close stops the heartbeat writer. After Close returns, no more heartbeat
// writes will be made and any in-flight write has completed.
func (w *Writer) Close() {
	w.mu.Lock()
	if w.timer == nil {
		w.mu.Unlock()
		return // Already closed or never opened
	}

	w.logger.Info("Heartbeat Writer: closing")

	// Mark as closed and cancel context to unblock any in-flight write
	w.closed = true
	if w.cancel != nil {
		w.cancel()
	}

	// Stop the timer to prevent new writes from being scheduled
	w.timer.Stop()
	w.timer = nil
	w.ctx = nil
	w.cancel = nil

	w.mu.Unlock()

	// Wait for any in-flight write to complete
	w.wg.Wait()

	w.logger.Info("Heartbeat Writer: closed")
}

// IsOpen returns true if the writer is open.
func (w *Writer) IsOpen() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.timer != nil && !w.closed
}

// writeHeartbeat updates the heartbeat row with the current time in nanoseconds.
func (w *Writer) writeHeartbeat() {
	w.mu.Lock()

	if w.closed || w.ctx == nil {
		w.mu.Unlock()
		return
	}

	// Track this write so Close() can wait for it to complete
	w.wg.Add(1)
	defer w.wg.Done()

	// Create write context while holding the lock to ensure w.ctx is valid
	writeCtx, cancel := context.WithTimeout(w.ctx, w.interval)

	// Release lock during the actual write to avoid blocking Close()
	w.mu.Unlock()

	err := w.write(writeCtx)
	cancel()

	// Re-acquire lock to update state and schedule next write
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	if err != nil {
		w.logger.Error("Failed to write heartbeat", "error", err)
		w.writeErrors.Add(1)
	} else {
		w.writes.Add(1)
		w.logger.Debug("Heartbeat written",
			"shard_id", w.shardID,
			"pooler_id", w.poolerID,
			"ts", w.now().UnixNano())
	}

	// Schedule next write only after this one completes
	w.scheduleNextWrite()
}

// write writes a single heartbeat update.
func (w *Writer) write(ctx context.Context) error {
	tsNano := w.now().UnixNano()

	_, err := w.queryService.QueryArgs(ctx, `
		INSERT INTO multigres.heartbeat (shard_id, leader_id, ts)
		VALUES ($1, $2, $3)
		ON CONFLICT (shard_id) DO UPDATE
		SET leader_id = EXCLUDED.leader_id,
		    ts = EXCLUDED.ts
	`, w.shardID, w.poolerID, tsNano)
	if err != nil {
		return mterrors.Wrap(err, "failed to write heartbeat")
	}

	return nil
}

// Writes returns the number of successful heartbeat writes.
func (w *Writer) Writes() int64 {
	return w.writes.Load()
}

// WriteErrors returns the number of heartbeat write errors.
func (w *Writer) WriteErrors() int64 {
	return w.writeErrors.Load()
}
