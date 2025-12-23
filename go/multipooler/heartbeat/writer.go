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
	"github.com/multigres/multigres/go/tools/timer"
)

// Make these modifiable for testing.
var (
	defaultHeartbeatInterval = 1 * time.Second
)

// Writer runs on primary databases and writes heartbeats to the heartbeat
// table at regular intervals.
type Writer struct {
	querier  executor.InternalQueryService
	logger   *slog.Logger
	shardID  []byte
	poolerID string
	interval time.Duration
	now      func() time.Time

	mu          sync.Mutex
	isOpen      bool
	ticks       *timer.Timer
	writes      atomic.Int64
	writeErrors atomic.Int64

	// For canceling ongoing writes
	writeMu     sync.Mutex
	writeCancel context.CancelFunc
}

// NewWriter creates a new heartbeat writer.
//
// We do not support on-demand or disabled heartbeats at this time.
func NewWriter(querier executor.InternalQueryService, logger *slog.Logger, shardID []byte, poolerID string, intervalMs int) *Writer {
	interval := time.Duration(intervalMs) * time.Millisecond
	if intervalMs <= 0 {
		interval = defaultHeartbeatInterval
	}
	return &Writer{
		querier:  querier,
		logger:   logger,
		shardID:  shardID,
		poolerID: poolerID,
		interval: interval,
		now:      time.Now,
		ticks:    timer.NewTimer(interval),
	}
}

// Open starts the heartbeat writer.
func (w *Writer) Open() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isOpen {
		return
	}
	defer func() {
		w.isOpen = true
	}()

	w.logger.Info("Heartbeat Writer: opening")

	w.enableWrites()
}

// Close stops the heartbeat writer and periodic ticket.
func (w *Writer) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.isOpen {
		return
	}
	defer func() {
		w.isOpen = false
	}()

	w.logger.Info("Heartbeat Writer: closing")

	w.disableWrites()

	w.logger.Info("Heartbeat Writer: closed")
}

// IsOpen returns true if the writer is open.
func (w *Writer) IsOpen() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isOpen
}

// enableWrites activates heartbeat writes
func (w *Writer) enableWrites() {
	// We must combat a potential race condition: the writer is Open, and a request comes
	// to enableWrites(), but simultaneously the writes gets Close()d.
	// We must not send any more ticks while the writer is closed.
	go func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if !w.isOpen {
			return
		}
		w.ticks.Start(w.writeHeartbeat)
	}()
}

// disableWrites deactivates heartbeat writes.
// Order of operations:
//  1. Writer is marked closed (by caller). This prevents new writeHeartbeats from running.
//  2. Cancel the context for any ongoing write to unblock it.
//  3. Stop the ticks and wait for any in-flight callback to complete.
//
// Context cancellation handles query termination automatically via the connection pool.
func (w *Writer) disableWrites() {
	// Cancel any ongoing write to unblock it
	w.writeMu.Lock()
	if w.writeCancel != nil {
		w.writeCancel()
	}
	w.writeMu.Unlock()

	// Stop waits for the callback to complete
	w.ticks.Stop()
}

// writeHeartbeat updates the heartbeat row with the current time in nanoseconds.
func (w *Writer) writeHeartbeat() {
	if !w.IsOpen() {
		return
	}
	if err := w.write(); err != nil {
		w.recordError(err)
	} else {
		w.writes.Add(1)
		w.logger.Debug("Heartbeat written",
			"shard_id", w.shardID,
			"pooler_id", w.poolerID,
			"ts", w.now().UnixNano())
	}
}

// write writes a single heartbeat update.
func (w *Writer) write() error {
	ctx, cancel := context.WithDeadline(context.TODO(), w.now().Add(w.interval))

	// Track this write so it can be canceled
	w.writeMu.Lock()
	w.writeCancel = cancel
	w.writeMu.Unlock()

	// Get current timestamp in nanoseconds
	tsNano := w.now().UnixNano()

	_, err := w.querier.QueryArgs(ctx, `
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

// recordError logs and records an error.
func (w *Writer) recordError(err error) {
	if err == nil {
		return
	}
	w.logger.Error("Failed to write heartbeat", "error", err)
	w.writeErrors.Add(1)
}

// Writes returns the number of successful heartbeat writes.
func (w *Writer) Writes() int64 {
	return w.writes.Load()
}

// WriteErrors returns the number of heartbeat write errors.
func (w *Writer) WriteErrors() int64 {
	return w.writeErrors.Load()
}
