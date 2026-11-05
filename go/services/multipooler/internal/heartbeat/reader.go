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
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/switcher"
	"github.com/multigres/multigres/go/tools/pgutil"
	"github.com/multigres/multigres/go/tools/timer"
)

const (
	defaultHeartbeatReadInterval = 1 * time.Second

	// quorumColumnsRetryInterval is how long the Reader reads only the lag columns after
	// finding the quorum-commit columns missing, before trying the full query again.
	quorumColumnsRetryInterval = 30 * time.Second

	// fullHeartbeatQuery reads everything the Reader uses. Only ts is needed for lag; the
	// quorum_commit columns are optional extras (see lagOnlyHeartbeatQuery).
	fullHeartbeatQuery = "SELECT ts, pg_last_wal_receive_lsn()::text, quorum_commit_lsn::text, quorum_commit_ts FROM multigres.heartbeat WHERE shard_id = $1"

	// lagOnlyHeartbeatQuery is what the Reader falls back to on a table from a release
	// that predates the quorum_commit columns. A standby cannot alter the table, so until
	// the primary upgrades it (the change then replicates) this keeps lag and WAL-receive
	// tracking working instead of failing the whole read over two optional columns.
	lagOnlyHeartbeatQuery = "SELECT ts, pg_last_wal_receive_lsn()::text FROM multigres.heartbeat WHERE shard_id = $1"
)

// Reader reads the heartbeat table at a configured interval in order
// to calculate replication lag. It is meant to be run on a replica, and paired
// with a Writer on a primary.
// Lag is calculated by comparing the most recent timestamp in the heartbeat
// table against the current time at read time.
type Reader struct {
	queryService executor.InternalQueryService
	logger       *slog.Logger
	shardID      []byte
	interval     time.Duration
	now          func() time.Time

	runner *timer.PeriodicRunner

	lagMu          sync.Mutex
	lastKnownLag   time.Duration
	lastKnownTime  time.Time
	lastKnownError error

	// lastReceiveLSN / lastReceiveLSNAdvanceTime track WAL streaming progress: the
	// last observed pg_last_wal_receive_lsn() and when it last increased. This is a
	// WAL-progress signal (new WAL streamed from the primary), distinct from the
	// heartbeat lag and from last_msg_receive_time (which advances on keepalives).
	// We stamp the advance time here, at this ~1s tick, and report it — rather than
	// leaving orch to diff the raw LSN across its coarser health snapshots — so a
	// consumer knows the AGE of the last advance at this resolution, independent of
	// the snapshot cadence. That matters when the staleness threshold is only a small
	// multiple of that cadence. Guarded by lagMu; haveReceiveLSN is false until the
	// first value is observed.
	lastReceiveLSN            pgutil.LSN
	lastReceiveLSNAdvanceTime time.Time
	haveReceiveLSN            bool

	// lastQuorumCommitLSN is the primary's most recently observed
	// quorum_commit_lsn -- see QuorumCommitLSN.
	lastQuorumCommitLSN pgutil.LSN
	haveQuorumCommitLSN bool

	// quorumCommitTs is the leader-stamped time paired with quorum_commit_lsn
	// -- see QuorumCommitTs.
	quorumCommitTs     time.Time
	haveQuorumCommitTs bool

	reads      atomic.Int64
	readErrors atomic.Int64

	// readSchemaErrors counts the readErrors caused by an incompatible heartbeat
	// table (see IsSchemaError).
	readSchemaErrors atomic.Int64

	// lagOnlyUntil is when to next try the full query, in Unix nanoseconds; until then
	// only lagOnlyHeartbeatQuery is used. Zero means the full query works.
	lagOnlyUntil atomic.Int64
}

// NewReader returns a new heartbeat reader with the default interval.
func NewReader(queryService executor.InternalQueryService, logger *slog.Logger, shardID []byte) *Reader {
	return newReader(queryService, logger, shardID, defaultHeartbeatReadInterval)
}

// newReader creates a heartbeat reader with a configurable interval.
func newReader(queryService executor.InternalQueryService, logger *slog.Logger, shardID []byte, interval time.Duration) *Reader {
	runner := timer.NewPeriodicRunner(context.TODO(), interval)
	return &Reader{
		queryService: queryService,
		logger:       logger,
		shardID:      shardID,
		now:          time.Now,
		interval:     interval,
		runner:       runner,
	}
}

// Compile-time check that Reader implements switcher.Toggleable.
var _ switcher.Toggleable = (*Reader)(nil)

// Open starts the heartbeat ticker.
func (r *Reader) Open() {
	r.logger.Info("heartbeat Reader: opening")

	r.runner.Start(r.readHeartbeat, func() {
		r.lagMu.Lock()
		r.lastKnownTime = r.now()
		r.lagMu.Unlock()
	})
}

// Close cancels the readHeartbeat periodic ticker. After Close returns,
// no more heartbeat reads will be made and any in-flight read has completed.
func (r *Reader) Close() {
	r.runner.Stop()
	r.logger.Info("heartbeat Reader: closed")
}

// IsOpen returns true if the reader is open.
func (r *Reader) IsOpen() bool {
	return r.runner.Running()
}

// Status returns the most recently recorded lag measurement or error encountered.
func (r *Reader) Status() (time.Duration, error) {
	r.lagMu.Lock()
	defer r.lagMu.Unlock()

	if r.lastKnownError != nil {
		return 0, r.lastKnownError
	}

	// Return an error if we didn't receive a heartbeat for more than two intervals
	if !r.lastKnownTime.IsZero() && r.now().Sub(r.lastKnownTime) > 2*r.interval {
		return 0, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "no heartbeat received in over 2x the heartbeat interval")
	}

	return r.lastKnownLag, nil
}

// readHeartbeat reads from the heartbeat table exactly once, updating
// the last known lag and/or error, the WAL-receive advance tracking, and counters.
func (r *Reader) readHeartbeat(ctx context.Context) {
	readCtx, cancel := context.WithTimeout(ctx, r.interval)
	defer cancel()

	tsNano, receiveLSN, haveReceiveLSN, quorumCommitLSN, haveQuorumCommitLSN, quorumCommitTs, haveQuorumCommitTs, err := r.fetchMostRecentHeartbeat(readCtx)
	if err != nil {
		r.recordError(err)
		return
	}
	// Read the clock once so lag and the advance timestamps reflect the same instant.
	now := r.now()
	lag := now.Sub(time.Unix(0, tsNano))
	r.reads.Add(1)

	r.lagMu.Lock()
	r.lastKnownTime = now
	r.lastKnownLag = lag
	r.lastKnownError = nil
	// First observation, or a genuine increase, marks WAL-receive progress.
	// receive_lsn is monotonic during streaming; a non-increase (idle keepalives)
	// leaves the timestamp untouched so it can age out.
	if haveReceiveLSN && (!r.haveReceiveLSN || receiveLSN > r.lastReceiveLSN) {
		r.lastReceiveLSN = receiveLSN
		r.lastReceiveLSNAdvanceTime = now
		r.haveReceiveLSN = true
	}
	// quorum_commit_lsn/quorum_commit_ts are just the latest observed values,
	// not advance-tracked like receive_lsn above -- quorum_commit_ts is
	// already a leader-stamped instant, not something we'd time ourselves.
	if haveQuorumCommitLSN {
		r.lastQuorumCommitLSN = quorumCommitLSN
		r.haveQuorumCommitLSN = true
	}
	if haveQuorumCommitTs {
		r.quorumCommitTs = quorumCommitTs
		r.haveQuorumCommitTs = true
	}
	r.lagMu.Unlock()

	r.logger.DebugContext(ctx, "heartbeat read",
		"shard_id", r.shardID,
		"lag", lag)
}

// fetchMostRecentHeartbeat fetches the heartbeat row plus the WAL receiver's
// current streamed position in one query. It returns the heartbeat timestamp in
// nanoseconds, the parsed pg_last_wal_receive_lsn() and whether that LSN was
// present (haveReceiveLSN is false when NULL — streaming disabled/not yet
// started), and likewise the row's quorum_commit_lsn and quorum_commit_ts. It
// returns a wrapped error on failure; a missing heartbeat row counts as a
// failure (the writer always maintains it when multigres is healthy).
// receive_lsn advances ONLY via streaming replication from the primary (not
// restore_command/archive replay), so its advancement is a "the primary is
// streaming new WAL to this standby" signal. quorum_commit_lsn/quorum_commit_ts
// are written by the primary itself and ride that same replication, so their
// advancement is a distinct "the primary is durably committing with quorum"
// signal.
func (r *Reader) fetchMostRecentHeartbeat(ctx context.Context) (tsNano int64, receiveLSN pgutil.LSN, haveReceiveLSN bool, quorumCommitLSN pgutil.LSN, haveQuorumCommitLSN bool, quorumCommitTs time.Time, haveQuorumCommitTs bool, err error) {
	result, withQuorum, err := r.queryHeartbeat(ctx)
	if err != nil {
		return 0, 0, false, 0, false, time.Time{}, false, mterrors.Wrap(err, "failed to read most recent heartbeat")
	}
	if result == nil || len(result.StructuredRows()) == 0 {
		return 0, 0, false, 0, false, time.Time{}, false, mterrors.Wrap(errors.New("no heartbeat found"), "failed to read most recent heartbeat")
	}
	row := result.StructuredRows()[0]

	tsNano, err = executor.GetInt64(row, 0)
	if err != nil {
		return 0, 0, false, 0, false, time.Time{}, false, mterrors.Wrap(err, "failed to parse heartbeat timestamp")
	}

	// receive_lsn is best-effort: a NULL/unparsable value just leaves advance
	// tracking untouched; it must not fail the heartbeat-lag read.
	if raw, rawErr := executor.GetString(row, 1); rawErr == nil && raw != "" {
		if lsn, lsnErr := pgutil.ParseLSN(raw); lsnErr != nil {
			r.logger.DebugContext(ctx, "failed to parse pg_last_wal_receive_lsn", "value", raw, "error", lsnErr)
		} else {
			receiveLSN, haveReceiveLSN = lsn, true
		}
	}
	if !withQuorum {
		return tsNano, receiveLSN, haveReceiveLSN, 0, false, time.Time{}, false, nil
	}
	// Same best-effort treatment for quorum_commit_lsn.
	if raw, rawErr := executor.GetString(row, 2); rawErr == nil && raw != "" {
		if lsn, lsnErr := pgutil.ParseLSN(raw); lsnErr != nil {
			r.logger.DebugContext(ctx, "failed to parse quorum_commit_lsn", "value", raw, "error", lsnErr)
		} else {
			quorumCommitLSN, haveQuorumCommitLSN = lsn, true
		}
	}
	// quorum_commit_ts is NULL until the second successful write; GetTime
	// leaves it at its zero value (IsZero) in that case, best-effort like the
	// LSN columns above.
	if ts, tsErr := executor.GetTime(row, 3); tsErr != nil {
		r.logger.DebugContext(ctx, "failed to parse quorum_commit_ts", "error", tsErr)
	} else if !ts.IsZero() {
		quorumCommitTs, haveQuorumCommitTs = ts, true
	}
	return tsNano, receiveLSN, haveReceiveLSN, quorumCommitLSN, haveQuorumCommitLSN, quorumCommitTs, haveQuorumCommitTs, nil
}

// queryHeartbeat runs the heartbeat read, falling back to the lag-only query when the
// table lacks the quorum_commit columns. withQuorum reports which one produced the result.
//
// The fallback read counts as a successful read, so lag keeps flowing, and also as a
// schema error, so the gap stays visible in the error metric and triggers a schema
// re-check. While on the fallback the full query is retried every
// quorumColumnsRetryInterval, which picks up the columns once the primary adds them.
func (r *Reader) queryHeartbeat(ctx context.Context) (result *sqltypes.Result, withQuorum bool, err error) {
	if until := r.lagOnlyUntil.Load(); until != 0 && r.now().UnixNano() < until {
		result, err = r.queryService.QueryAdminArgs(ctx, lagOnlyHeartbeatQuery, r.shardID)
		return result, false, err
	}
	result, err = r.queryService.QueryAdminArgs(ctx, fullHeartbeatQuery, r.shardID)
	if err == nil {
		r.lagOnlyUntil.Store(0)
		return result, true, nil
	}
	if !IsSchemaError(err) {
		return nil, true, err
	}
	// Do not wait out the retry interval on the failure path: lag is read right away.
	result, fallbackErr := r.queryService.QueryAdminArgs(ctx, lagOnlyHeartbeatQuery, r.shardID)
	if fallbackErr != nil {
		return nil, false, fallbackErr
	}
	r.lagOnlyUntil.Store(r.now().Add(quorumColumnsRetryInterval).UnixNano())
	r.readErrors.Add(1)
	r.readSchemaErrors.Add(1)
	r.logger.WarnContext(ctx, "heartbeat table cannot supply quorum-commit columns; reading replication lag only",
		"error", err)
	return result, false, nil
}

// LastReceiveLSNAdvance returns when pg_last_wal_receive_lsn() was last observed
// to increase, and whether any value has been observed yet.
func (r *Reader) LastReceiveLSNAdvance() (time.Time, bool) {
	r.lagMu.Lock()
	defer r.lagMu.Unlock()
	return r.lastReceiveLSNAdvanceTime, r.haveReceiveLSN
}

// QuorumCommitLSN returns the most recently observed quorum_commit_lsn, and
// whether any value has been observed yet. Diagnostic only, e.g. for
// reporting backlog size against last_receive_lsn.
func (r *Reader) QuorumCommitLSN() (pgutil.LSN, bool) {
	r.lagMu.Lock()
	defer r.lagMu.Unlock()
	return r.lastQuorumCommitLSN, r.haveQuorumCommitLSN
}

// QuorumCommitTs returns the leader-stamped time paired with the most
// recently observed quorum_commit_lsn, and whether any value has been
// observed yet -- a single snapshot, at the cost of comparing a leader
// clock to the caller's own.
func (r *Reader) QuorumCommitTs() (time.Time, bool) {
	r.lagMu.Lock()
	defer r.lagMu.Unlock()
	return r.quorumCommitTs, r.haveQuorumCommitTs
}

// recordError keeps track of the lastKnown error for reporting to Status().
func (r *Reader) recordError(err error) {
	r.lagMu.Lock()
	r.lastKnownError = err
	r.lagMu.Unlock()
	r.logger.Error("failed to read heartbeat", "error", err)
	r.readErrors.Add(1)
	if IsSchemaError(err) {
		r.readSchemaErrors.Add(1)
	}
}

// Reads returns the number of successful heartbeat reads.
func (r *Reader) Reads() int64 {
	return r.reads.Load()
}

// ReadErrors returns the number of heartbeat read errors.
func (r *Reader) ReadErrors() int64 {
	return r.readErrors.Load()
}

// ReadSchemaErrors returns the number of heartbeat read errors caused by an
// incompatible heartbeat table. They are also counted in ReadErrors.
func (r *Reader) ReadSchemaErrors() int64 {
	return r.readSchemaErrors.Load()
}

// LeadershipView contains the consensus state and replication lag information
type LeadershipView struct {
	LeaderID       string
	LastHeartbeat  time.Time
	ReplicationLag time.Duration
}

// GetLeadershipView returns both replication lag and consensus state
func (r *Reader) GetLeadershipView() (*LeadershipView, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), r.interval)
	defer cancel()

	result, err := r.queryService.QueryAdminArgs(ctx,
		"SELECT leader_id, ts FROM multigres.heartbeat WHERE shard_id = $1",
		r.shardID)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read leadership view")
	}
	if result == nil || len(result.StructuredRows()) == 0 {
		return nil, mterrors.Wrap(errors.New("no heartbeat found"), "failed to read leadership view")
	}

	row := result.StructuredRows()[0]
	leaderID, err := executor.GetString(row, 0)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse heartbeat leader")
	}
	tsNano, err := executor.GetInt64(row, 1)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse heartbeat timestamp")
	}

	view := &LeadershipView{
		LeaderID:      leaderID,
		LastHeartbeat: time.Unix(0, tsNano),
	}

	// Calculate replication lag
	view.ReplicationLag = r.now().Sub(view.LastHeartbeat)

	return view, nil
}
