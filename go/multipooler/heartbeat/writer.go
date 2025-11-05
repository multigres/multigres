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

	"github.com/multigres/multigres/go/mterrors"
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
	logger     *slog.Logger
	shardID    []byte
	poolerID   string
	leaderTerm atomic.Int64
	interval   time.Duration
	now        func() time.Time

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
func NewWriter(db *sql.DB, logger *slog.Logger, shardID []byte, poolerID string, intervalMs int) *Writer {
	// TODO: use a connection pool when it's implemented
	interval := time.Duration(intervalMs) * time.Millisecond
	if intervalMs <= 0 {
		interval = defaultHeartbeatInterval
	}
	w := &Writer{
		db:       db,
		logger:   logger,
		shardID:  shardID,
		poolerID: poolerID,
		interval: interval,
		now:      time.Now,
		ticks:    timer.NewTimer(interval),
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

	success := false
	defer func() {
		if success {
			w.isOpen = true
		}
	}()

	w.logger.Info("Heartbeat Writer: opening")

	// TODO: open connection pools here

	// Initialize leader_term from the database (source of truth for actual leader appointment)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var existingTerm sql.NullInt64
	err := w.db.QueryRowContext(ctx, "SELECT leader_term FROM multigres.heartbeat WHERE shard_id = $1", w.shardID).Scan(&existingTerm)

	if err != nil && err != sql.ErrNoRows {
		w.logger.Warn("Failed to read existing leader_term from database", "error", err)
	} else if err == sql.ErrNoRows {
		w.logger.Info("No existing heartbeat row found, starting with leader_term = 0")
	} else if existingTerm.Valid {
		currentMemoryTerm := w.GetLeaderTerm()
		if existingTerm.Int64 != currentMemoryTerm {
			w.logger.Info("Initializing leader_term from database",
				"database_term", existingTerm.Int64,
				"memory_term", currentMemoryTerm)
			w.SetLeaderTerm(existingTerm.Int64)
		} else {
			w.logger.Info("Leader_term already matches database", "leader_term", existingTerm.Int64)
		}
	}

	w.enableWrites()
	success = true
}

// Close stops the heartbeat writer and periodic ticker.
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

	// Reset leader_term when closing so reopening initializes fresh from database
	w.leaderTerm.Store(0)

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

	// Get connection for tracking (for potential kill)
	// TODO: get connection from pool when we have pools
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return mterrors.Wrap(err, "failed to get connection")
	}
	defer conn.Close()

	// Query the backend PID for this connection
	var pid int64
	err = conn.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid)
	if err != nil {
		return mterrors.Wrap(err, "failed to get backend pid")
	}
	w.writeConnID.Store(pid)

	// Clear the connection ID when done
	defer w.writeConnID.Store(-1)

	// Get current leader term
	leaderTerm := w.leaderTerm.Load()

	// Get current timestamp in nanoseconds
	tsNano := w.now().UnixNano()

	_, err = conn.ExecContext(ctx, `
		INSERT INTO multigres.heartbeat (shard_id, leader_id, ts, leader_term)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (shard_id) DO UPDATE
		SET leader_id = EXCLUDED.leader_id,
		    ts = EXCLUDED.ts,
		    leader_term = EXCLUDED.leader_term
	`, w.shardID, w.poolerID, tsNano, leaderTerm)
	if err != nil {
		return mterrors.Wrap(err, "failed to write heartbeat")
	}

	return nil
}

// getWALPosition returns the current WAL LSN position
func (w *Writer) getWALPosition(ctx context.Context) (string, error) {
	var lsn string
	err := w.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn)
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get WAL position")
	}
	return lsn, nil
}

// SetLeaderTerm updates the leader term for consensus tracking
func (w *Writer) SetLeaderTerm(term int64) {
	w.leaderTerm.Store(term)
}

// GetLeaderTerm returns the current leader term
func (w *Writer) GetLeaderTerm() int64 {
	return w.leaderTerm.Load()
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

	// If cancel didn't work, escalate to pg_terminate_backend
	//
	// In the future, we could try pg_cancel_backend first, and if that doesn't
	// work, then pg_terminate_backend. There are possible concerns that
	// pg_cancel_backend could interact badly with pipelined queries. To keep
	// things simple and conservative, we only use pg_terminate_backend for now.
	_, err := w.db.ExecContext(ctx, "SELECT pg_terminate_backend($1)", writeID)
	if err != nil {
		return mterrors.Wrap(err, fmt.Sprintf("failed to terminate backend %d", writeID))
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
