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
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/timer"
)

// Make these modifiable for testing.
var (
	defaultHeartbeatInterval = 1 * time.Second
)

// Writer runs on primary databases and writes heartbeats to the heartbeat
// table at regular intervals.
type Writer struct {
	db *sql.DB // TODO: use connection pooling when it's implemented
	// TODO: this has the potential to be spammy, so we need to throttle this
	// or convert these into alerts.
	logger   *slog.Logger
	shardID  []byte
	poolerID string
	interval time.Duration
	now      func() time.Time

	mu          sync.Mutex
	isOpen      bool
	ticks       *timer.Timer
	writeConnID atomic.Int64
	writes      atomic.Int64
	writeErrors atomic.Int64
}

// NewWriter creates a new heartbeat writer.
//
// We do not support on-demand or disabled heartbeats at this time.
func NewWriter(db *sql.DB, logger *slog.Logger, shardID []byte, poolerID string) *Writer {
	// TODO: use a connection pool when it's implemented
	w := &Writer{
		db:       db,
		logger:   logger,
		shardID:  shardID,
		poolerID: poolerID,
		interval: defaultHeartbeatInterval,
		now:      time.Now,
		ticks:    timer.NewTimer(defaultHeartbeatInterval),
	}
	w.writeConnID.Store(-1)
	return w
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

	// TODO: open connection pools here

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

	// TODO: close connection pools

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

// disableWrites deactivates heartbeat writes
func (w *Writer) disableWrites() {
	// We stop the ticks in a separate go routine because it can block if the write is stuck on full-sync ACKs.
	// At the same time we try and kill the write that is in progress. We use the context and its cancellation
	// for coordination between the two go-routines. In the end we will have guaranteed that the ticks have stopped
	// and no write is in progress.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		w.ticks.Stop()
		cancel()
	}()
	w.killWritesUntilStopped(ctx)
}

// writeHeartbeat updates the heartbeat row with the current time in nanoseconds.
func (w *Writer) writeHeartbeat() {
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
	ctx, cancel := context.WithDeadline(context.Background(), w.now().Add(w.interval))
	defer cancel()

	timestampNs := w.now().UnixNano()

	// Get connection for tracking (for potential kill)
	// TODO: get connection from pool when we have pools
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Query the backend PID for this connection
	var pid int64
	err = conn.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid)
	if err != nil {
		return fmt.Errorf("failed to get backend pid: %w", err)
	}
	w.writeConnID.Store(pid)

	// Clear the connection ID when done
	defer w.writeConnID.Store(-1)

	_, err = conn.ExecContext(ctx, `
		INSERT INTO multigres.heartbeat (shard_id, pooler_id, ts)
		VALUES ($1, $2, $3)
		ON CONFLICT (shard_id) DO UPDATE
		SET pooler_id = EXCLUDED.pooler_id,
		    ts = EXCLUDED.ts
	`, w.shardID, w.poolerID, timestampNs)
	if err != nil {
		return fmt.Errorf("failed to write heartbeat: %w", err)
	}

	return nil
}

// killWritesUntilStopped tries to kill the write in progress until the ticks have stopped.
func (w *Writer) killWritesUntilStopped(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		// Try to kill the query
		// TODO: There is a possible race condition that cause the wrong query
		// to be killed. The Vitess connection pool handles this race condition,
		// so we need to make sure we port that behavior.
		err := w.killWrite()
		w.recordError(err)

		select {
		case <-ctx.Done():
			// If the context has been cancelled, then we know that the ticks have stopped.
			// This guarantees that there are no writes in progress, so there is nothing to kill.
			return
		case <-ticker.C:
			// Continue trying to kill
		}
	}
}

// killWrite kills the write in progress (if any).
func (w *Writer) killWrite() error {
	writeID := w.writeConnID.Load()
	if writeID == -1 {
		return nil // No write in progress
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.interval)
	defer cancel()

	// First try pg_cancel_backend (graceful cancellation)
	_, err := w.db.ExecContext(ctx, "SELECT pg_cancel_backend($1)", writeID)
	if err != nil {
		return fmt.Errorf("failed to cancel backend %d: %w", writeID, err)
	}

	w.logger.Debug("Cancelled write connection", "pid", writeID)

	// Wait briefly to see if the cancellation worked
	time.Sleep(200 * time.Millisecond)

	// Check if the write is still in progress.
	//
	// pg_cancel_backend attempts to gracefully cancel the running query
	// associated with writeID. If the cancellation is successful, the
	// earlier call to write() will set w.writeConnID to -1. If the write
	// finished anyway, there is no query left to kill.
	if w.writeConnID.Load() != writeID {
		return nil
	}

	// If cancel didn't work, escalate to pg_terminate_backend
	w.logger.Debug("Cancel didn't stop write, terminating connection", "pid", writeID)
	_, err = w.db.ExecContext(ctx, "SELECT pg_terminate_backend($1)", writeID)
	if err != nil {
		return fmt.Errorf("failed to terminate backend %d: %w", writeID, err)
	}

	w.logger.Debug("Terminated write connection", "pid", writeID)
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
