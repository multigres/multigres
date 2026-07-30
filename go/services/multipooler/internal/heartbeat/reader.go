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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/tools/pgutil"
	"github.com/multigres/multigres/go/tools/timer"
)

const (
	defaultHeartbeatReadInterval = 1 * time.Second
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

	reads      atomic.Int64
	readErrors atomic.Int64
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

// Open starts the heartbeat ticker.
func (r *Reader) Open() {
	r.logger.Info("Heartbeat Reader: opening")

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
	r.logger.Info("Heartbeat Reader: closed")
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

	tsNano, receiveLSN, haveReceiveLSN, err := r.fetchMostRecentHeartbeat(readCtx)
	if err != nil {
		r.recordError(err)
		return
	}
	// Read the clock once so lag and the advance timestamp reflect the same instant.
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
	r.lagMu.Unlock()

	r.logger.DebugContext(ctx, "Heartbeat read",
		"shard_id", r.shardID,
		"lag", lag)
}

// fetchMostRecentHeartbeat fetches the heartbeat row plus the WAL receiver's
// current streamed position in one query. It returns the heartbeat timestamp in
// nanoseconds, the parsed pg_last_wal_receive_lsn(), and whether that LSN was
// present (haveReceiveLSN is false when NULL — streaming disabled/not yet
// started). It returns a wrapped error on failure; a missing heartbeat row counts
// as a failure (the writer always maintains it when multigres is healthy).
// receive_lsn advances ONLY via streaming replication from the primary (not
// restore_command/archive replay), so its advancement is a "the primary is
// streaming new WAL to this standby" signal.
func (r *Reader) fetchMostRecentHeartbeat(ctx context.Context) (tsNano int64, receiveLSN pgutil.LSN, haveReceiveLSN bool, err error) {
	result, err := r.queryService.QueryAdminArgs(ctx,
		"SELECT ts, pg_last_wal_receive_lsn()::text FROM multigres.heartbeat WHERE shard_id = $1",
		r.shardID)
	if err != nil {
		return 0, 0, false, mterrors.Wrap(err, "failed to read most recent heartbeat")
	}
	if result == nil || len(result.StructuredRows()) == 0 {
		return 0, 0, false, mterrors.Wrap(errors.New("no heartbeat found"), "failed to read most recent heartbeat")
	}
	row := result.StructuredRows()[0]

	tsNano, err = executor.GetInt64(row, 0)
	if err != nil {
		return 0, 0, false, mterrors.Wrap(err, "failed to parse heartbeat timestamp")
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
	return tsNano, receiveLSN, haveReceiveLSN, nil
}

// LastReceiveLSNAdvance returns when pg_last_wal_receive_lsn() was last observed
// to increase, and whether any value has been observed yet.
func (r *Reader) LastReceiveLSNAdvance() (time.Time, bool) {
	r.lagMu.Lock()
	defer r.lagMu.Unlock()
	return r.lastReceiveLSNAdvanceTime, r.haveReceiveLSN
}

// recordError keeps track of the lastKnown error for reporting to Status().
func (r *Reader) recordError(err error) {
	r.lagMu.Lock()
	r.lastKnownError = err
	r.lagMu.Unlock()
	r.logger.Error("Failed to read heartbeat", "error", err)
	r.readErrors.Add(1)
}

// Reads returns the number of successful heartbeat reads.
func (r *Reader) Reads() int64 {
	return r.reads.Load()
}

// ReadErrors returns the number of heartbeat read errors.
func (r *Reader) ReadErrors() int64 {
	return r.readErrors.Load()
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
	tsNano, err := strconv.ParseInt(string(row.Values[1]), 10, 64)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse heartbeat timestamp")
	}

	view := &LeadershipView{
		LeaderID:      string(row.Values[0]),
		LastHeartbeat: time.Unix(0, tsNano),
	}

	// Calculate replication lag
	view.ReplicationLag = r.now().Sub(view.LastHeartbeat)

	return view, nil
}
