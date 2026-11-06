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
	lsn pgutil.LSN
	ts  time.Time
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
	// proven write. It is raised from two goroutines: the heartbeat tick, after a
	// write that waited for quorum, and WriteProven, when the rule store acks a
	// proposal. Both go through raiseProven, a compare-and-swap that replaces the
	// whole pointer and never moves it backwards, so no mutex is needed. Other
	// goroutines read it via LastProven() (e.g. an RPC handler reporting the
	// leader's own view).
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

	err := w.write(writeCtx, true)
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
func (w *Writer) write(ctx context.Context, waitForQuorum bool) error {
	now := w.now()

	var lsnArg, provenTsArg any
	if lastProven := w.lastProven.Load(); lastProven != nil {
		lsnArg = lastProven.lsn.String()
		provenTsArg = lastProven.ts
	}

	// Only the forced write changes the commit mode, so a configured
	// synchronous_commit still governs normal ticks. set_config(..., true) is
	// transaction-local and covers this statement's own commit. QueryAdminArgs
	// doesn't support multi-statement queries, so this can't be BEGIN/SET LOCAL/
	// .../COMMIT; one statement is also one round trip, which matters if postgres
	// is ever remote.
	prefix, from := "", ""
	if !waitForQuorum {
		prefix = "WITH async AS (SELECT set_config('synchronous_commit', 'off', true))"
		from = " FROM async"
	}

	result, err := w.queryService.QueryAdminArgs(ctx, prefix+`
		INSERT INTO multigres.heartbeat (shard_id, leader_id, ts, quorum_commit_lsn, quorum_commit_ts)
		SELECT $1::bytea, $2::text, $3::bigint, $4::pg_lsn, $5::timestamptz`+from+`
		ON CONFLICT (shard_id) DO UPDATE
		SET leader_id = EXCLUDED.leader_id,
		    ts = EXCLUDED.ts,
		    quorum_commit_lsn = EXCLUDED.quorum_commit_lsn,
		    quorum_commit_ts = EXCLUDED.quorum_commit_ts
		RETURNING pg_current_wal_lsn()::text
	`, w.shardID, w.poolerID, now.UnixNano(), lsnArg, provenTsArg)
	if err != nil {
		return mterrors.Wrap(err, "failed to write heartbeat")
	}

	// Candidate for the NEXT write's quorum_commit_lsn/quorum_commit_ts, paired
	// with now (also captured pre-commit, above). Best-effort: on failure,
	// keep the previous candidate and retry next tick. A forced write proves
	// nothing about its own LSN, so it is no candidate.
	if waitForQuorum && result != nil && len(result.StructuredRows()) > 0 {
		if raw, rawErr := executor.GetString(result.StructuredRows()[0], 0); rawErr == nil && raw != "" {
			if lsn, lsnErr := pgutil.ParseLSN(raw); lsnErr == nil {
				w.raiseProven(lsn, now)
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
//
// Purely in-memory, so it goes empty right after a process restart. A new
// leader's first proof comes from WriteProven when its rule is decided, so it
// doesn't have to wait for two ticks.
func (w *Writer) LastProven() (lsn pgutil.LSN, ts time.Time, have bool) {
	lastProven := w.lastProven.Load()
	if lastProven == nil {
		return 0, time.Time{}, false
	}
	return lastProven.lsn, lastProven.ts, true
}

// WriteProven raises the proven watermark to a write the caller has proved quorum-acked,
// then writes a heartbeat carrying it immediately. startedAt is when that write began, as
// with a normal tick. The forced write skips the synchronous-commit wait: its WAL precedes
// the decision's commit, which does wait, so the proof replicates with it. The watermark
// never moves backwards.
func (w *Writer) WriteProven(ctx context.Context, lsn pgutil.LSN, startedAt time.Time) error {
	w.raiseProven(lsn, startedAt)
	writeCtx, cancel := context.WithTimeout(ctx, w.interval)
	defer cancel()
	return w.write(writeCtx, false)
}

func (w *Writer) raiseProven(lsn pgutil.LSN, at time.Time) {
	for {
		current := w.lastProven.Load()
		if current != nil && !at.After(current.ts) {
			return
		}
		if w.lastProven.CompareAndSwap(current, &provenWatermark{lsn: lsn, ts: at}) {
			return
		}
	}
}

// Writes returns the number of successful heartbeat writes.
func (w *Writer) Writes() int64 {
	return w.writes.Load()
}

// WriteErrors returns the number of heartbeat write errors.
func (w *Writer) WriteErrors() int64 {
	return w.writeErrors.Load()
}
