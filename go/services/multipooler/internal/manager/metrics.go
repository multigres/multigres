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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

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
	meter := otel.Meter("github.com/multigres/multigres/go/services/multipooler/internal/manager")
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
