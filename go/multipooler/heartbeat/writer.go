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
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/multipooler/executor"
	"github.com/multigres/multigres/go/tools/timer"
)

// Make these modifiable for testing.
var (
	defaultHeartbeatInterval = constants.HeartbeatWriteInterval
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

	runner *timer.PeriodicRunner

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
	runner := timer.NewPeriodicRunner(context.TODO(), interval)
	return &Writer{
		queryService: queryService,
		logger:       logger,
		shardID:      shardID,
		poolerID:     poolerID,
		interval:     interval,
		now:          time.Now,
		runner:       runner,
	}
}

// Open starts the heartbeat writer.
func (w *Writer) Open() {
	w.logger.Info("Heartbeat Writer: opening")
	w.runner.Start(w.writeHeartbeat, nil)
}

// Close stops the heartbeat writer. After Close returns, no more heartbeat
// writes will be made and any in-flight write has completed.
func (w *Writer) Close() {
	w.logger.Info("Heartbeat Writer: closing")
	w.runner.Stop()
	w.logger.Info("Heartbeat Writer: closed")
}

// IsOpen returns true if the writer is open.
func (w *Writer) IsOpen() bool {
	return w.runner.Running()
}

// writeHeartbeat updates the heartbeat row with the current time in nanoseconds.
func (w *Writer) writeHeartbeat(ctx context.Context) {
	writeCtx, cancel := context.WithTimeout(ctx, w.interval)
	defer cancel()

	err := w.write(writeCtx)
	if err != nil {
		w.logger.ErrorContext(ctx, "Failed to write heartbeat", "error", err)
		w.writeErrors.Add(1)
	} else {
		w.writes.Add(1)
		w.logger.DebugContext(ctx, "Heartbeat written",
			"shard_id", w.shardID,
			"pooler_id", w.poolerID,
			"ts", w.now().UnixNano())
	}
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
