// Copyright 2026 Supabase, Inc.
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

package replicationstats

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/switcher"
	"github.com/multigres/multigres/go/tools/pgutil"
	"github.com/multigres/multigres/go/tools/timer"
)

const defaultPollInterval = 10 * time.Second

// statsQuery joins pg_stat_replication (per-connection lag/ack state) with
// pg_replication_slots (per-slot WAL retention), filtered to connections we
// tagged ourselves. Ages are computed in Postgres (EXTRACT(EPOCH FROM ...))
// rather than in Go from a fetched timestamp, avoiding both a timestamptz
// parse and any multipooler/Postgres clock-skew concern. Numeric/interval
// results are cast to text for the same parsing convention used elsewhere
// in this codebase (see heartbeat.Reader's pg_last_wal_receive_lsn()::text).
const statsQuery = `
SELECT
  r.application_name,
  r.usename,
  EXTRACT(EPOCH FROM (clock_timestamp() - r.reply_time))::text AS last_ack_age_seconds,
  r.sent_lsn::text,
  EXTRACT(EPOCH FROM r.replay_lag)::text AS replay_lag_seconds,
  s.slot_name,
  COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn), 0)::bigint AS retained_wal_bytes
FROM pg_stat_replication r
LEFT JOIN pg_replication_slots s ON s.active_pid = r.pid
WHERE r.application_name LIKE '` + constants.LogicalReplicationConnAppNamePrefix + `%'
`

// Poller periodically queries pg_stat_replication/pg_replication_slots for
// logical-replication connections tagged by
// reserved.Pool.NewLogicalReplicationConn and records them as OTel gauges.
// It only produces data while this multipooler fronts the current leader —
// only the primary has active logical-replication walsenders — so it is
// paired with a switcher.NoOp secondary via switcher.RoleSwitcher at the
// call site that constructs it (see manager.startReplicationStats).
type Poller struct {
	queryService executor.InternalQueryService
	metrics      *Metrics
	logger       *slog.Logger
	interval     time.Duration
	now          func() time.Time

	runner *timer.PeriodicRunner

	mu          sync.Mutex
	lastSentLSN map[string]pgutil.LSN
	lastAdvance map[string]time.Time

	polls      atomic.Int64
	pollErrors atomic.Int64
}

// NewPoller creates a poller with the given interval in milliseconds (0 or
// negative selects the built-in default), following the same convention as
// heartbeat.NewWriter.
func NewPoller(queryService executor.InternalQueryService, metrics *Metrics, logger *slog.Logger, intervalMs int) *Poller {
	interval := time.Duration(intervalMs) * time.Millisecond
	if intervalMs <= 0 {
		interval = defaultPollInterval
	}
	return newPoller(queryService, metrics, logger, interval)
}

// newPoller creates a poller with a configurable interval, for use by
// NewPoller and tests.
func newPoller(queryService executor.InternalQueryService, metrics *Metrics, logger *slog.Logger, interval time.Duration) *Poller {
	return &Poller{
		queryService: queryService,
		metrics:      metrics,
		logger:       logger,
		interval:     interval,
		now:          time.Now,
		runner:       timer.NewPeriodicRunner(context.TODO(), interval),
		lastSentLSN:  make(map[string]pgutil.LSN),
		lastAdvance:  make(map[string]time.Time),
	}
}

// Compile-time check that Poller implements switcher.Toggleable.
var _ switcher.Toggleable = (*Poller)(nil)

// Open starts the poller's ticker.
func (p *Poller) Open() {
	p.logger.Info("replicationstats Poller: opening")
	p.runner.Start(p.poll, nil)
}

// Close stops the poller's ticker. After Close returns, no more polls will
// run and any in-flight poll has completed. Also clears the reported
// snapshot: without this, a demoted-but-not-yet-retired poller would keep
// reporting its last-known-good connection stats forever (the async gauges
// only stop reporting a given connection when it drops out of a snapshot —
// an empty snapshot correctly reports "no active connections" instead of
// the last one polled before losing leadership).
func (p *Poller) Close() {
	p.runner.Stop()
	p.metrics.setSnapshot(nil)
	p.logger.Info("replicationstats Poller: closed")
}

// IsOpen returns true if the poller is running.
func (p *Poller) IsOpen() bool {
	return p.runner.Running()
}

// Polls returns the number of successful poll cycles.
func (p *Poller) Polls() int64 {
	return p.polls.Load()
}

// PollErrors returns the number of poll cycles that failed to query Postgres.
func (p *Poller) PollErrors() int64 {
	return p.pollErrors.Load()
}

// PollerStatus is a read-only snapshot of a Poller's health and latest
// polled data, for the multipooler status page.
type PollerStatus struct {
	Open        bool
	Polls       int64
	PollErrors  int64
	Connections []ConnStats
}

// Status returns the poller's current health and latest polled snapshot.
func (p *Poller) Status() PollerStatus {
	return PollerStatus{
		Open:        p.IsOpen(),
		Polls:       p.Polls(),
		PollErrors:  p.PollErrors(),
		Connections: p.metrics.currentSnapshot(),
	}
}

// poll runs one query/record cycle, updating LSN-advance tracking and
// dropping state for connections no longer present.
func (p *Poller) poll(ctx context.Context) {
	pollCtx, cancel := context.WithTimeout(ctx, p.interval)
	defer cancel()

	result, err := p.queryService.QueryAdmin(pollCtx, statsQuery)
	if err != nil {
		p.logger.ErrorContext(ctx, "replicationstats poll failed", "error", err)
		p.pollErrors.Add(1)
		return
	}
	p.polls.Add(1)
	if result == nil {
		return
	}

	now := p.now()
	rows := result.StructuredRows()
	seen := make(map[string]bool, len(rows))
	snapshot := make([]ConnStats, 0, len(rows))

	p.mu.Lock()
	for _, row := range rows {
		s, connID, ok := p.parseRowLocked(ctx, row, now)
		if !ok {
			continue
		}
		seen[connID] = true
		snapshot = append(snapshot, s)
	}

	// Drop advance-tracking state for connections no longer present — they've
	// disconnected, or this pooler is no longer the leader (in which case the
	// switcher.RoleSwitcher driving this poller will also be closing it shortly).
	for connID := range p.lastSentLSN {
		if !seen[connID] {
			delete(p.lastSentLSN, connID)
			delete(p.lastAdvance, connID)
		}
	}
	p.mu.Unlock()

	p.metrics.setSnapshot(snapshot)
}

// parseRowLocked parses one joined pg_stat_replication/pg_replication_slots
// row into ConnStats, updating the LSN-advance tracking maps. Caller must
// hold p.mu. Returns ok=false for a malformed row (application_name missing
// our tag, or unreadable) — skip it, don't abort the tick.
func (p *Poller) parseRowLocked(ctx context.Context, row *sqltypes.Row, now time.Time) (ConnStats, string, bool) {
	appName, err := executor.GetString(row, 0)
	if err != nil {
		p.logger.DebugContext(ctx, "replicationstats: unreadable application_name", "error", err)
		return ConnStats{}, "", false
	}
	connID, ok := strings.CutPrefix(appName, constants.LogicalReplicationConnAppNamePrefix)
	if !ok {
		p.logger.DebugContext(ctx, "replicationstats: application_name missing expected prefix", "application_name", appName)
		return ConnStats{}, "", false
	}

	user, err := executor.GetString(row, 1)
	if err != nil {
		p.logger.DebugContext(ctx, "replicationstats: unreadable usename", "conn_id", connID, "error", err)
		return ConnStats{}, "", false
	}
	s := ConnStats{User: user, ConnID: connID}

	if raw, err := executor.GetString(row, 2); err == nil && raw != "" {
		if age, err := executor.ParseFloat64(raw); err == nil {
			s.LastAckAge, s.HaveAck = age, true
		}
	}

	if raw, err := executor.GetString(row, 3); err == nil && raw != "" {
		if lsn, err := pgutil.ParseLSN(raw); err == nil {
			prev, hadPrev := p.lastSentLSN[connID]
			if !hadPrev || lsn > prev {
				p.lastSentLSN[connID] = lsn
				p.lastAdvance[connID] = now
			}
		}
	}
	if advance, ok := p.lastAdvance[connID]; ok {
		s.LastMsgAge, s.HaveMsgAge = now.Sub(advance).Seconds(), true
	}

	if raw, err := executor.GetString(row, 4); err == nil && raw != "" {
		if lag, err := executor.ParseFloat64(raw); err == nil {
			s.ReplayLag, s.HaveReplayLag = lag, true
		}
	}

	if slotName, err := executor.GetString(row, 5); err == nil && slotName != "" {
		s.SlotName, s.HaveSlot = slotName, true
		if retained, err := executor.GetInt64(row, 6); err == nil {
			s.RetainedWAL = retained
		}
	}

	return s, connID, true
}
