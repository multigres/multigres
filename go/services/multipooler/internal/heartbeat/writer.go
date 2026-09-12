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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/switcher"
	"github.com/multigres/multigres/go/tools/pgutil"
	"github.com/multigres/multigres/go/tools/timer"
)

// Make these modifiable for testing.
var (
	defaultHeartbeatInterval = 1 * time.Second
)

// provenWatermark is an (LSN, ts) pair captured pre-commit by a successful
// write, held for one tick before being embedded in the next write's row.
type provenWatermark struct {
	lsn    pgutil.LSN
	tsNano int64
}

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

	// lastProven is the (LSN, ts) pair captured pre-commit by the most recent
	// successful write. Written only from writeHeartbeat's own goroutine (the
	// PeriodicRunner never overlaps ticks), but read from other goroutines via
	// LastProven() (e.g. an RPC handler reporting the leader's own view). Each
	// write replaces the whole pointer rather than mutating fields in place,
	// so a plain atomic pointer swap covers it -- no mutex needed.
	lastProven atomic.Pointer[provenWatermark]

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

// Compile-time check that Writer implements switcher.Toggleable.
var _ switcher.Toggleable = (*Writer)(nil)

// Open starts the heartbeat writer.
func (w *Writer) Open() {
	w.logger.Info("heartbeat Writer: opening")
	w.runner.Start(w.writeHeartbeat, nil)
}

// Close stops the heartbeat writer. After Close returns, no more heartbeat
// writes will be made and any in-flight write has completed.
func (w *Writer) Close() {
	w.logger.Info("heartbeat Writer: closing")
	w.runner.Stop()
	w.logger.Info("heartbeat Writer: closed")
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
		w.logger.ErrorContext(ctx, "failed to write heartbeat", "error", err)
		w.writeErrors.Add(1)
	} else {
		w.writes.Add(1)
		w.logger.DebugContext(ctx, "heartbeat written",
			"shard_id", w.shardID,
			"pooler_id", w.poolerID,
			"ts", w.now().UnixNano())
	}
}

// write writes a single heartbeat update.
//
// quorum_commit_lsn/quorum_commit_ts carry the PREVIOUS write's captured
// values, never this write's own: replica visibility only needs a replayed
// commit record, not quorum ack, so a fast/non-quorum standby can show a row
// as committed before quorum is actually reached. A value is only
// trustworthy once its own write already succeeded (proving quorum) before
// the write embedding it began. Nil (and thus a NULL row) until the second
// successful write, e.g. right after a promotion.
func (w *Writer) write(ctx context.Context) error {
	tsNano := w.now().UnixNano()

	var lsnArg, provenTsArg any
	if lastProven := w.lastProven.Load(); lastProven != nil {
		lsnArg = lastProven.lsn.String()
		provenTsArg = lastProven.tsNano
	}

	result, err := w.queryService.QueryAdminArgs(ctx, `
		INSERT INTO multigres.heartbeat (shard_id, leader_id, ts, quorum_commit_lsn, quorum_commit_ts)
		VALUES ($1, $2, $3, $4::pg_lsn, $5)
		ON CONFLICT (shard_id) DO UPDATE
		SET leader_id = EXCLUDED.leader_id,
		    ts = EXCLUDED.ts,
		    quorum_commit_lsn = EXCLUDED.quorum_commit_lsn,
		    quorum_commit_ts = EXCLUDED.quorum_commit_ts
		RETURNING pg_current_wal_lsn()::text
	`, w.shardID, w.poolerID, tsNano, lsnArg, provenTsArg)
	if err != nil {
		return mterrors.Wrap(err, "failed to write heartbeat")
	}

	// Candidate for the NEXT write's quorum_commit_lsn/quorum_commit_ts, paired
	// with tsNano (also captured pre-commit, above). Best-effort: on failure,
	// keep the previous candidate and retry next tick.
	if result != nil && len(result.StructuredRows()) > 0 {
		if raw, rawErr := executor.GetString(result.StructuredRows()[0], 0); rawErr == nil && raw != "" {
			if lsn, lsnErr := pgutil.ParseLSN(raw); lsnErr == nil {
				w.lastProven.Store(&provenWatermark{lsn: lsn, tsNano: tsNano})
			} else {
				w.logger.DebugContext(ctx, "failed to parse pg_current_wal_lsn", "value", raw, "error", lsnErr)
			}
		}
	}

	return nil
}

// LastProven returns the (LSN, ts) pair captured pre-commit by the most
// recent successful write, and whether one has been observed yet. This is
// the leader's own first-hand view -- available even when no follower is
// reachable to relay the replicated row.
func (w *Writer) LastProven() (lsn pgutil.LSN, tsNano int64, have bool) {
	lastProven := w.lastProven.Load()
	if lastProven == nil {
		return 0, 0, false
	}
	return lastProven.lsn, lastProven.tsNano, true
}

// Writes returns the number of successful heartbeat writes.
func (w *Writer) Writes() int64 {
	return w.writes.Load()
}

// WriteErrors returns the number of heartbeat write errors.
func (w *Writer) WriteErrors() int64 {
	return w.writeErrors.Load()
}
