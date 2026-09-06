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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const meterName = "github.com/multigres/multigres/go/services/multipooler/internal/manager"

// healthMetrics holds OTel metrics for pooler health/replication observability.
//
// Replication lag is already measured by the manager's heartbeat loop and stored
// in healthStreamer.replicationLagNs for the StreamPoolerHealth gRPC message;
// this publishes the same value as a metric so it can be dashboarded and alerted
// on without subscribing to the health stream. Serving-state transitions give
// failover/recovery visibility that was previously only inferable from logs.
type healthMetrics struct {
	replicationLag metric.Float64ObservableGauge
	transitions    metric.Int64Counter
}

// newHealthMetrics initialises health metrics. lagNsGetter returns the latest
// replication lag in nanoseconds; it is sampled by the observable-gauge callback
// at metric-collection time (not pushed), so it always reflects the most recent
// measurement. Best-effort: an instrument that fails to initialise is skipped
// and the joined error is returned for logging by the caller.
func newHealthMetrics(lagNsGetter func() int64) (*healthMetrics, error) {
	meter := otel.Meter(meterName)
	m := &healthMetrics{transitions: noop.Int64Counter{}}
	var errs []error

	var err error
	m.replicationLag, err = meter.Float64ObservableGauge(
		"mg.pooler.replication.lag",
		metric.WithDescription("PostgreSQL replication lag observed by this pooler (0 on a primary or before the first measurement)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.lag gauge: %w", err))
		m.replicationLag = nil
	}

	m.transitions, err = meter.Int64Counter(
		"mg.pooler.serving.transitions",
		metric.WithDescription("Serving-state transitions by from/to status"),
		metric.WithUnit("{transition}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.serving.transitions counter: %w", err))
		m.transitions = noop.Int64Counter{}
	}

	if m.replicationLag != nil {
		// The registration lives for the streamer's (i.e. the manager's)
		// lifetime. It is intentionally not torn down on manager close: the
		// streamer is reused across reopen, so unregistering would silently stop
		// the gauge after the first close. Per-test isolation comes from each
		// test shutting down its own meter provider.
		if _, err := meter.RegisterCallback(
			func(_ context.Context, o metric.Observer) error {
				// ns → s, matching the seconds unit used across mg.pooler.* durations.
				o.ObserveFloat64(m.replicationLag, float64(lagNsGetter())/1e9)
				return nil
			},
			m.replicationLag,
		); err != nil {
			errs = append(errs, fmt.Errorf("replication lag callback: %w", err))
		}
	}

	return m, errors.Join(errs...)
}

// recordTransition counts a serving-status transition. Callers should only
// invoke it on an actual change (from != to).
func (m *healthMetrics) recordTransition(ctx context.Context, from, to clustermetadatapb.PoolerServingStatus) {
	if m == nil || m.transitions == nil {
		return
	}
	m.transitions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("from", from.String()),
		attribute.String("to", to.String()),
	))
}

// managerMetrics holds the OTel instruments for the pooler manager.
type managerMetrics struct {
	rewindCheckpointWait    metric.Float64Histogram
	rewindExecutionDuration metric.Float64Histogram

	logicalFailoverDuration     metric.Float64Histogram
	logicalFailoverCount        metric.Int64Counter
	logicalFailoverSlotsDropped metric.Int64Counter

	// logicalFailoverSlots is a steady-state gauge of this node's logical
	// failover-slot readiness, labelled by status (ready/unready). Its value
	// comes from failoverSlotReadinessSnapshot rather than a live query, since
	// an ObservableGauge callback runs synchronously during metric collection
	// and must not block a scrape on a DB round-trip; setFailoverSlotReadiness
	// is called periodically by the manager's health heartbeat to keep it
	// fresh. ready/total are published together as one atomic pointer swap —
	// not as two independent atomics — so a concurrent reader can never observe
	// one field updated without the other (which would otherwise make
	// total-ready go negative).
	failoverSlotReadinessSnapshot atomic.Pointer[failoverSlotReadiness]
	logicalFailoverSlots          metric.Int64ObservableGauge
}

// failoverSlotReadiness is a (ready, total) pair published atomically as a
// unit by managerMetrics.setFailoverSlotReadiness.
type failoverSlotReadiness struct {
	ready, total int
}

// logicalFailoverStatus labels the outcome of the logical-replication
// slot-management span of a failover (see manageLogicalFailoverSlots) — not
// the failover/promotion as a whole.
type logicalFailoverStatus string

const (
	logicalFailoverStatusSuccess logicalFailoverStatus = "success"
	logicalFailoverStatusFailure logicalFailoverStatus = "failure"
)

// slotsDroppedReason labels why a logical-replication-related slot was
// dropped during cleanup.
type slotsDroppedReason string

const (
	// slotsDroppedReasonOrphaned is an un-synced failover-slot original left
	// behind on a former primary that has rejoined as a standby.
	slotsDroppedReasonOrphaned slotsDroppedReason = "orphaned"
	// slotsDroppedReasonDepartedFollower is a managed physical slot for a
	// follower no longer in the cohort.
	slotsDroppedReasonDepartedFollower slotsDroppedReason = "departed_follower"
)

// rewindPhase labels a pg_rewind execution-duration sample.
type rewindPhase string

const (
	// rewindPhaseDryRun is the read-only dry-run: connect to source, read the
	// timeline history, find the last common checkpoint, and scan the target WAL
	// (it may also run crash recovery first). No target data is written.
	rewindPhaseDryRun rewindPhase = "dry_run"
	// rewindPhaseRewind is the actual mutating pg_rewind (-R): the phase that
	// copies changed blocks and WAL from the source, whose runtime is dominated by
	// the retained pg_wal it copies.
	rewindPhaseRewind rewindPhase = "rewind"
)

// newManagerMetrics creates and registers the manager's OTel instruments. It
// always returns a non-nil *managerMetrics; a registration error is returned
// alongside, and the affected instrument is left nil (its record helper no-ops).
func newManagerMetrics() (*managerMetrics, error) {
	meter := otel.Meter(meterName)
	// How long a diverged follower's pg_rewind was held off waiting for the new
	// leader to advertise rewind-readiness — i.e. to complete its post-promotion
	// checkpoint onto the current timeline so it is safe to rewind from. Near zero
	// when the leader was already rewind-ready by the time the follower learned of
	// it; seconds when the follower had to wait for the checkpoint. A consistently
	// high distribution would argue for keeping the explicit post-promotion
	// checkpoint over relying on PostgreSQL's lazy one.
	wait, waitErr := meter.Float64Histogram(
		"multipooler.rewind.checkpoint_wait.duration",
		metric.WithDescription("Time a diverged follower's pg_rewind waited for the new leader to become rewind-ready (post-promotion checkpoint completion)"),
		metric.WithUnit("s"),
	)
	// How long pg_rewind itself ran, split by phase (dry_run vs rewind). This is
	// the actual subprocess runtime, distinct from checkpoint_wait above. It
	// matters operationally because pg_rewind runtime scales with retained pg_wal
	// (it copies the whole retained WAL, not just the divergence), so a rewind can
	// take minutes under load — long enough to matter for shutdown grace and for
	// the detached-rewind backstop timeout.
	exec, execErr := meter.Float64Histogram(
		"multipooler.rewind.execution.duration",
		metric.WithDescription("Duration of a pg_rewind invocation, labelled by phase (dry_run vs the mutating rewind)"),
		metric.WithUnit("s"),
	)

	// Duration of the logical-replication slot-management span of a failover
	// (see manageLogicalFailoverSlots): ensureFollowerPhysicalSlots,
	// setSynchronizedStandbySlots, and the advisory unready-slot check.
	failoverDuration, failoverDurationErr := meter.Float64Histogram(
		"mg.pooler.logical_failover.duration",
		metric.WithDescription("Duration of the logical-replication slot-management span of a failover, labelled by outcome"),
		metric.WithUnit("s"),
	)
	// Count of the same span by outcome. A rising failure count or an
	// unexpectedly high total both indicate logical-replication-slot
	// instability worth investigating.
	failoverCount, failoverCountErr := meter.Int64Counter(
		"mg.pooler.logical_failover.count",
		metric.WithDescription("Count of the logical-replication slot-management span of a failover, labelled by outcome"),
		metric.WithUnit("{failover}"),
	)
	// Logical-replication-related slots dropped during cleanup: physical slots
	// for a departed follower during reconcile, and orphaned un-synced
	// failover-slot originals on demote/rejoin. A silent stop in either path
	// would leak slots (retaining WAL) with no obvious symptom until disk
	// pressure shows up.
	slotsDropped, slotsDroppedErr := meter.Int64Counter(
		"mg.pooler.logical_failover.slots_dropped",
		metric.WithDescription("Logical-replication-related slots dropped during cleanup, labelled by reason"),
		metric.WithUnit("{slot}"),
	)

	m := &managerMetrics{
		rewindCheckpointWait:        wait,
		rewindExecutionDuration:     exec,
		logicalFailoverDuration:     failoverDuration,
		logicalFailoverCount:        failoverCount,
		logicalFailoverSlotsDropped: slotsDropped,
	}
	errs := []error{waitErr, execErr, failoverDurationErr, failoverCountErr, slotsDroppedErr}

	// Steady-state failover-slot readiness, labelled by status (ready/unready),
	// so an operator can see whether a node is failover-ready before it is ever
	// asked to fail over. Sampled continuously rather than only at promotion
	// time, so the value at any given failover can be read by correlating
	// timestamps against mg.pooler.logical_failover.duration.
	slots, slotsErr := meter.Int64ObservableGauge(
		"mg.pooler.logical_failover.slots",
		metric.WithDescription("Logical failover slots on this node, labelled by readiness status"),
		metric.WithUnit("{slot}"),
	)
	errs = append(errs, slotsErr)
	if slotsErr == nil {
		m.logicalFailoverSlots = slots
		if _, err := meter.RegisterCallback(
			func(_ context.Context, o metric.Observer) error {
				// One atomic load of the whole (ready, total) pair — see the
				// failoverSlotReadinessSnapshot field doc for why this must not
				// be two independent loads.
				snapshot := m.failoverSlotReadinessSnapshot.Load()
				if snapshot == nil {
					return nil
				}
				o.ObserveInt64(m.logicalFailoverSlots, int64(snapshot.ready), metric.WithAttributes(attribute.String("status", "ready")))
				o.ObserveInt64(m.logicalFailoverSlots, int64(snapshot.total-snapshot.ready), metric.WithAttributes(attribute.String("status", "unready")))
				return nil
			},
			slots,
		); err != nil {
			errs = append(errs, fmt.Errorf("logical failover slots callback: %w", err))
		}
	}

	return m, errors.Join(errs...)
}

// recordRewindCheckpointWait records how long a pg_rewind waited for the source
// leader to become rewind-ready before proceeding. Nil-receiver safe so manager
// values constructed without metrics (e.g. in unit tests) are no-ops.
func (m *managerMetrics) recordRewindCheckpointWait(ctx context.Context, d time.Duration) {
	if m == nil || m.rewindCheckpointWait == nil {
		return
	}
	m.rewindCheckpointWait.Record(ctx, d.Seconds())
}

// recordRewindExecutionDuration records how long a pg_rewind invocation took, for
// the given phase. Nil-receiver safe so manager values constructed without
// metrics (e.g. in unit tests) are no-ops.
func (m *managerMetrics) recordRewindExecutionDuration(ctx context.Context, phase rewindPhase, d time.Duration) {
	if m == nil || m.rewindExecutionDuration == nil {
		return
	}
	m.rewindExecutionDuration.Record(ctx, d.Seconds(),
		metric.WithAttributes(attribute.String("phase", string(phase))))
}

// setFailoverSlotReadiness updates the counts sampled by the
// mg.pooler.logical_failover.slots gauge. Called periodically by the
// manager's health heartbeat with the latest measured counts. Nil-receiver
// safe and safe to call concurrently with metric collection.
func (m *managerMetrics) setFailoverSlotReadiness(ready, total int) {
	if m == nil {
		return
	}
	m.failoverSlotReadinessSnapshot.Store(&failoverSlotReadiness{ready: ready, total: total})
}

// recordSlotsDropped counts logical-replication-related slots dropped during
// cleanup, by reason. Nil-receiver safe; a zero count is a no-op so a cleanup
// that dropped nothing doesn't emit an empty data point.
func (m *managerMetrics) recordSlotsDropped(ctx context.Context, reason slotsDroppedReason, count int64) {
	if m == nil || m.logicalFailoverSlotsDropped == nil || count == 0 {
		return
	}
	m.logicalFailoverSlotsDropped.Add(ctx, count, metric.WithAttributes(attribute.String("reason", string(reason))))
}

// recordLogicalFailover records the duration and outcome of the
// logical-replication slot-management span of a failover. Nil-receiver safe.
func (m *managerMetrics) recordLogicalFailover(ctx context.Context, status logicalFailoverStatus, d time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("status", string(status)))
	if m.logicalFailoverDuration != nil {
		m.logicalFailoverDuration.Record(ctx, d.Seconds(), attrs)
	}
	if m.logicalFailoverCount != nil {
		m.logicalFailoverCount.Add(ctx, 1, attrs)
	}
}
