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

package recovery

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/multigres/multigres/go/multiorch/store"
)

// Metrics holds all OpenTelemetry metrics for the recovery engine.
// Following OTel conventions, metrics are defined in a separate file with
// wrapper types that hide attribute key names from instrumented code.
//
// This pattern is inspired by:
// https://github.com/open-telemetry/opentelemetry-go/blob/v1.38.0/semconv/v1.37.0/dbconv/metric.go
type Metrics struct {
	poolerStoreSize          metric.Int64ObservableGauge
	refreshDuration          RefreshDuration
	pollDuration             PollDuration
	healthCheckCycleDuration metric.Float64Histogram
	poolersCheckedPerCycle   metric.Int64Histogram
}

// PollDuration wraps a Float64Histogram for recording pooler health check durations.
// It abstracts away attribute key names so callers don't need to know them.
type PollDuration struct {
	metric.Float64Histogram
}

// Record records a pooler health check duration with proper OTel attributes.
// The dbNamespace and tablegroup parameters are automatically converted to the
// correct attribute keys (db.namespace, tablegroup) internally.
//
// Parameters:
//   - ctx: Context for the metric recording
//   - duration: How long the poll took
//   - dbNamespace: The database name (becomes "db.namespace" attribute per OTel spec)
//   - tablegroup: The table group name
//   - status: "success", "failure", or "exceeded_interval"
func (m PollDuration) Record(
	ctx context.Context,
	duration time.Duration,
	dbNamespace string,
	tablegroup string,
	status string,
) {
	m.Float64Histogram.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String("db.namespace", dbNamespace),
			attribute.String("tablegroup", tablegroup),
			attribute.String("status", status),
		))
}

// RefreshDuration wraps a Float64Histogram for recording cluster metadata refresh durations.
type RefreshDuration struct {
	metric.Float64Histogram
}

// Record records a cluster metadata refresh duration.
func (m RefreshDuration) Record(ctx context.Context, duration time.Duration) {
	m.Float64Histogram.Record(ctx, duration.Seconds())
}

// NewMetrics initializes OpenTelemetry metrics for the recovery engine.
// If meter is nil, returns noop implementations to avoid nil checks in instrumented code.
func NewMetrics(meter metric.Meter, logger *slog.Logger, poolerStore *store.Store[string, *store.PoolerHealth]) (*Metrics, error) {
	m := &Metrics{}

	// Handle nil meter case - return noop implementations
	if meter == nil {
		m.refreshDuration = RefreshDuration{noop.Float64Histogram{}}
		m.pollDuration = PollDuration{noop.Float64Histogram{}}
		m.healthCheckCycleDuration = noop.Float64Histogram{}
		m.poolersCheckedPerCycle = noop.Int64Histogram{}
		return m, nil
	}

	var err error

	// Gauge for current pooler store size
	m.poolerStoreSize, err = meter.Int64ObservableGauge(
		"multiorch.recovery.pooler_store_size",
		metric.WithDescription("Current number of poolers tracked in the recovery engine store"),
		metric.WithUnit("{poolers}"),
	)
	if err != nil {
		logger.Error("failed to create pooler_store_size gauge", "error", err)
		return nil, err
	}

	// Register callback to update the gauge
	_, err = meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			observer.ObserveInt64(m.poolerStoreSize, int64(poolerStore.Len()))
			return nil
		},
		m.poolerStoreSize,
	)
	if err != nil {
		logger.Error("failed to register pooler_store_size callback", "error", err)
		return nil, err
	}

	// Histogram for cluster metadata refresh duration
	refreshDurationHistogram, err := meter.Float64Histogram(
		"multiorch.recovery.refresh.duration",
		metric.WithDescription("Duration of cluster metadata refresh operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		logger.Error("failed to create refresh.duration histogram", "error", err)
		return nil, err
	}
	m.refreshDuration = RefreshDuration{refreshDurationHistogram}

	// Histogram for individual poll duration
	// Following OTel convention: use histogram with status attribute instead of separate counters
	pollDurationHistogram, err := meter.Float64Histogram(
		"multiorch.recovery.poll.duration",
		metric.WithDescription("Duration of individual pooler health checks"),
		metric.WithUnit("s"),
	)
	if err != nil {
		logger.Error("failed to create poll.duration histogram", "error", err)
		return nil, err
	}
	m.pollDuration = PollDuration{pollDurationHistogram}

	// Histogram for health check cycle duration
	m.healthCheckCycleDuration, err = meter.Float64Histogram(
		"multiorch.recovery.health_check_cycle.duration",
		metric.WithDescription("Duration of complete health check cycles"),
		metric.WithUnit("s"),
	)
	if err != nil {
		logger.Error("failed to create health_check_cycle.duration histogram", "error", err)
		return nil, err
	}

	// Histogram for poolers checked per cycle
	m.poolersCheckedPerCycle, err = meter.Int64Histogram(
		"multiorch.recovery.poolers_checked_per_cycle",
		metric.WithDescription("Number of poolers checked per health check cycle"),
		metric.WithUnit("{poolers}"),
	)
	if err != nil {
		logger.Error("failed to create poolers_checked_per_cycle histogram", "error", err)
		return nil, err
	}

	return m, nil
}

// RecordHealthCheckCycleDuration records the duration of a complete health check cycle.
func (m *Metrics) RecordHealthCheckCycleDuration(ctx context.Context, duration time.Duration) {
	m.healthCheckCycleDuration.Record(ctx, duration.Seconds())
}

// RecordPoolersCheckedPerCycle records the number of poolers checked in a health check cycle.
func (m *Metrics) RecordPoolersCheckedPerCycle(ctx context.Context, count int) {
	m.poolersCheckedPerCycle.Record(ctx, int64(count))
}
