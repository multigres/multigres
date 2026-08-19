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
}

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
	return &managerMetrics{
		rewindCheckpointWait:    wait,
		rewindExecutionDuration: exec,
	}, errors.Join(waitErr, execErr)
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
