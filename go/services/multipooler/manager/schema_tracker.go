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

package manager

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/executor"
)

// pubsubSubscriber is the subset of *pubsub.Listener used by schemaTracker.
// Extracted as an interface to allow testing without a real PostgreSQL connection.
type pubsubSubscriber interface {
	AwaitRunning(ctx context.Context)
	SubscribeCh(channel string, notifCh chan *sqltypes.Notification)
}

// schemaTrackingQuery returns all user-visible tables and views with a
// definition fingerprint so the tracker can detect structural changes.
//
// For regular tables, materialized views, and partitioned tables the
// fingerprint is the ordered list of column names and type OIDs. For views
// the fingerprint is the normalised view definition so that query-level
// rewrites are also detected.
//
// System schemas (pg_catalog, information_schema, toast, temp) are excluded
// because they are not user-visible and should not influence the plan cache.
const schemaTrackingQuery = `
SELECT
    n.nspname                   AS schema_name,
    c.relname                   AS table_name,
    c.oid::bigint               AS oid,
    CASE
        WHEN c.relkind = 'v' THEN pg_catalog.pg_get_viewdef(c.oid, false)
        ELSE (
            SELECT string_agg(a.attname || ':' || a.atttypid::text, ',' ORDER BY a.attnum)
            FROM   pg_catalog.pg_attribute a
            WHERE  a.attrelid = c.oid
              AND  a.attnum > 0
              AND  NOT a.attisdropped
        )
    END                         AS definition
FROM  pg_catalog.pg_class     c
JOIN  pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%'
  AND n.nspname NOT LIKE 'pg_temp_%'
ORDER BY c.oid`

// tableSignature is the tracked fingerprint for a single table or view.
type tableSignature struct {
	qualifiedName string // "schema.table" — used only for logging
	definition    string // column sig or view def; empty means no columns yet
}

// schemaTracker detects PostgreSQL schema changes and increments a version
// counter that is broadcast to multigateway clients via the health stream.
//
// Detection uses two complementary mechanisms:
//  1. Fast path: a DDL event trigger installed in the multigres sidecar schema
//     sends a pg_notify on the schema-changed channel after every ddl_command_end.
//     The tracker subscribes to this channel via the pubsub listener and polls
//     immediately on notification, providing near-zero-latency detection.
//  2. Fallback polling: a periodic ticker (default 5 min) catches any changes
//     that arrived while the pubsub listener was disconnected.
//
// The schemaTracker implements StateAware and manages its own goroutine
// lifecycle. On PRIMARY+SERVING it starts the polling loop and subscribes to
// the pubsub fast path (waiting for the pubsub listener to start first via
// AwaitRunning). On any other state it stops the loop and unsubscribes. Only
// the serving primary accepts DDL, so replicas leave their schema version at 0
// and the multigateway does not receive redundant invalidation signals.
type schemaTracker struct {
	logger          *slog.Logger
	getQueryService func() executor.InternalQueryService
	pubsubListener  pubsubSubscriber
	notifyVersion   func(int64) // → healthStreamer.UpdateSchemaVersion
	pollInterval    time.Duration

	// ctx is the long-lived manager context used as the parent for the run
	// goroutine. Stored at construction time so OnStateChange does not need
	// the transient state-change context.
	ctx context.Context

	// version is incremented atomically each time a schema change is detected.
	version atomic.Int64

	// notifCh receives pg_notify messages from the pubsub listener.
	notifCh chan *sqltypes.Notification

	// cancel stops the run goroutine; nil when not running.
	cancel context.CancelFunc

	// wg tracks the run goroutine so Stop can wait for it to exit.
	wg sync.WaitGroup

	// closed is set by Close to permanently disable this instance.
	// After Close, OnStateChange becomes a no-op. This is needed because
	// the StateManager does not support unregistering components: when the
	// manager calls reopenConnections, the old schemaTracker instance remains
	// in the component list but must not attempt to use its stale
	// pubsubListener reference.
	closed atomic.Bool
}

// newSchemaTracker creates a schema tracker. ctx should be the manager's
// lifecycle context. Register with StateManager to start/stop tracking
// automatically on PRIMARY+SERVING transitions.
func newSchemaTracker(
	ctx context.Context,
	logger *slog.Logger,
	getQueryService func() executor.InternalQueryService,
	pubsubListener pubsubSubscriber,
	notifyVersion func(int64),
	pollInterval time.Duration,
) *schemaTracker {
	return &schemaTracker{
		logger:          logger.With("component", "schema_tracker"),
		getQueryService: getQueryService,
		pubsubListener:  pubsubListener,
		notifyVersion:   notifyVersion,
		pollInterval:    pollInterval,
		ctx:             ctx,
		notifCh:         make(chan *sqltypes.Notification, 16),
	}
}

// start begins the polling loop. Idempotent — no-op if already running.
// Uses the long-lived manager context stored at construction time.
func (st *schemaTracker) start() {
	if st.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(st.ctx)
	st.cancel = cancel
	st.wg.Add(1)
	go st.run(ctx)
}

// stop shuts down the polling loop and waits for it to exit. Idempotent.
func (st *schemaTracker) stop() {
	if st.cancel == nil {
		return
	}
	st.cancel()
	st.wg.Wait()
	st.cancel = nil
}

// Close permanently disables this schema tracker instance. After Close,
// OnStateChange becomes a no-op so stale instances left in the
// StateManager's component list do not interfere with new instances.
func (st *schemaTracker) Close() {
	st.closed.Store(true)
	st.stop()
}

// OnStateChange implements StateAware.
//
// On PRIMARY+SERVING: starts the polling loop and subscribes to the
// schema-changed NOTIFY channel via the pubsub listener (waiting for it to
// start first).
//
// On any other state: stops the polling loop and unsubscribes. Only a serving
// primary accepts DDL, so tracking on any other state would be redundant and
// would cause duplicate plan cache invalidations on the multigateway.
func (st *schemaTracker) OnStateChange(
	ctx context.Context,
	poolerType clustermetadatapb.PoolerType,
	servingStatus clustermetadatapb.PoolerServingStatus,
) error {
	if st.closed.Load() {
		return nil
	}
	if poolerType == clustermetadatapb.PoolerType_PRIMARY && servingStatus == clustermetadatapb.PoolerServingStatus_SERVING {
		// The pubsub listener also starts on PRIMARY+SERVING and runs concurrently
		// with this call (SetState fans out OnStateChange in parallel). Wait for it
		// to be ready before subscribing so we don't silently drop the subscription.
		st.pubsubListener.AwaitRunning(ctx)
		st.pubsubListener.SubscribeCh(constants.SchemaChangedChannel, st.notifCh)
		st.start()
		st.logger.InfoContext(ctx, "schema tracking started")
	} else {
		st.stop()
		// No need to Unsubscribe — the pubsub listener's run() drops all
		// subscription state when it stops, and it only runs on PRIMARY+SERVING
		// (same as this tracker). Calling Unsubscribe on a stopped listener
		// can deadlock: the buffered requests channel may accept the send,
		// but nobody processes it so <-req.done blocks forever.
		st.logger.InfoContext(ctx, "schema tracking stopped")
	}
	return nil
}

// run is the polling loop. It exits when ctx is cancelled (via stop).
func (st *schemaTracker) run(ctx context.Context) {
	defer st.wg.Done()

	ticker := time.NewTicker(st.pollInterval)
	defer ticker.Stop()

	// snapshot is intentionally local — on stop/restart cycles (e.g. failover
	// PRIMARY→REPLICA→PRIMARY) it resets to nil, establishing a fresh baseline.
	// This is safe because the gateway side resets SchemaVersion to -1 on health
	// stream reconnect, which forces a plan cache invalidation regardless.
	var snapshot map[int64]*tableSignature // nil until first successful poll

	doPoll := func() {
		changed, newSnapshot, err := st.poll(ctx, snapshot)
		if err != nil {
			st.logger.WarnContext(ctx, "schema tracking poll failed", "error", err)
			return
		}
		if changed {
			v := st.version.Add(1)
			st.logger.InfoContext(ctx, "schema change detected, incrementing version", "version", v)
			st.notifyVersion(v)
		}
		snapshot = newSnapshot
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			doPoll()

		case <-st.notifCh:
			// Drain any queued notifications before polling once — multiple rapid
			// DDL statements should produce a single poll, not one per statement.
			for len(st.notifCh) > 0 {
				<-st.notifCh
			}
			doPoll()
		}
	}
}

// poll queries the PostgreSQL catalog and compares it against the previous
// snapshot. Returns true if any change was detected, along with the new
// snapshot to use on the next call.
//
// On the very first call (snapshot == nil) the catalog state is recorded as
// the baseline; no change is reported so the gateway does not invalidate plan
// caches on startup.
func (st *schemaTracker) poll(
	ctx context.Context,
	snapshot map[int64]*tableSignature,
) (changed bool, newSnapshot map[int64]*tableSignature, err error) {
	qs := st.getQueryService()
	if qs == nil {
		// Connection pool not ready yet; keep the existing snapshot.
		return false, snapshot, nil
	}

	result, err := qs.Query(ctx, schemaTrackingQuery)
	if err != nil {
		return false, snapshot, fmt.Errorf("querying pg_catalog: %w", err)
	}

	newSnapshot, err = parseSchemaSnapshot(result)
	if err != nil {
		return false, snapshot, err
	}

	if snapshot == nil {
		// First poll: establish baseline without signalling a change.
		return false, newSnapshot, nil
	}

	// Detect tables that were created, dropped, or structurally altered.
	for oid, sig := range newSnapshot {
		if prev, ok := snapshot[oid]; !ok {
			st.logger.DebugContext(ctx, "table created", "table", sig.qualifiedName)
			changed = true
		} else if prev.definition != sig.definition {
			st.logger.DebugContext(ctx, "table altered", "table", sig.qualifiedName)
			changed = true
		}
	}
	if !changed {
		for oid, sig := range snapshot {
			if _, ok := newSnapshot[oid]; !ok {
				st.logger.DebugContext(ctx, "table dropped", "table", sig.qualifiedName)
				changed = true
				break
			}
		}
	}

	return changed, newSnapshot, nil
}

// parseSchemaSnapshot converts the raw query result into a map keyed by OID.
func parseSchemaSnapshot(result *sqltypes.Result) (map[int64]*tableSignature, error) {
	snapshot := make(map[int64]*tableSignature, len(result.Rows))
	for _, row := range result.Rows {
		if len(row.Values) < 4 {
			continue
		}
		schemaName := string(row.Values[0])
		tableName := string(row.Values[1])
		oidStr := string(row.Values[2])
		definition := string(row.Values[3])

		oid, err := strconv.ParseInt(oidStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing OID %q: %w", oidStr, err)
		}

		snapshot[oid] = &tableSignature{
			qualifiedName: schemaName + "." + tableName,
			definition:    definition,
		}
	}
	return snapshot, nil
}
