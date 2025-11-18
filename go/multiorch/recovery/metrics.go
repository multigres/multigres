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

// PoolerPollStatus represents the possible status values for a pooler health check poll.
type PoolerPollStatus string

const (
	PoolerPollStatusSuccess          PoolerPollStatus = "success"
	PoolerPollStatusFailure          PoolerPollStatus = "failure"
	PoolerPollStatusExceededInterval PoolerPollStatus = "exceeded_interval"
)

// Metrics holds all OpenTelemetry metrics for the recovery engine.
// Following OTel conventions, metrics are defined in a separate file with
// wrapper types that hide attribute key names from instrumented code.
//
// This pattern is inspired by:
// https://github.com/open-telemetry/opentelemetry-go/blob/v1.38.0/semconv/v1.37.0/dbconv/metric.go
type Metrics struct {
	poolerStoreSize                metric.Int64ObservableGauge
	clusterMetadataRefreshDuration ClusterMetadataRefreshDuration
	poolerPollDuration             PoolerPollDuration
	healthCheckCycleDuration       HealthCheckCycleDuration
}

// PoolerPollDuration wraps a Float64Histogram for recording pooler health check durations.
// It abstracts away attribute key names so callers don't need to know them.
type PoolerPollDuration struct {
	metric.Float64Histogram
}

// Record records a pooler health check duration with proper OTel attributes.
// The dbNamespace and tablegroup parameters are automatically converted to the
// correct attribute keys (db.namespace, tablegroup) internally.
//
// Parameters:
//   - ctx: Context for the metric recording
//   - val: How long the poll took (in seconds)
//   - dbNamespace: The database name (becomes "db.namespace" attribute per OTel spec)
//   - tablegroup: The table group name
//   - status: The poll status (success, failure, or exceeded_interval)
//   - attrs: Optional additional attributes to include in the metric
func (m PoolerPollDuration) Record(
	ctx context.Context,
	val float64,
	dbNamespace string,
	tablegroup string,
	status PoolerPollStatus,
	attrs ...attribute.KeyValue,
) {
	m.Float64Histogram.Record(ctx, val,
		metric.WithAttributes(
			append(
				attrs,
				attribute.String("db.namespace", dbNamespace),
				attribute.String("tablegroup", tablegroup),
				attribute.String("status", string(status)),
			)...,
		))
}

// ClusterMetadataRefreshDuration wraps a Float64Histogram for recording cluster metadata refresh durations.
type ClusterMetadataRefreshDuration struct {
	metric.Float64Histogram
}

// Record records a cluster metadata refresh duration.
//
// Parameters:
//   - ctx: Context for the metric recording
//   - val: How long the refresh took (in seconds)
//   - attrs: Optional additional attributes to include in the metric
func (m ClusterMetadataRefreshDuration) Record(ctx context.Context, val float64, attrs ...attribute.KeyValue) {
	if len(attrs) == 0 {
		m.Float64Histogram.Record(ctx, val)
		return
	}
	m.Float64Histogram.Record(ctx, val, metric.WithAttributes(attrs...))
}

// HealthCheckCycleDuration wraps a Float64Histogram for recording health check cycle durations.
type HealthCheckCycleDuration struct {
	metric.Float64Histogram
}

// Record records a health check cycle duration.
//
// Parameters:
//   - ctx: Context for the metric recording
//   - val: How long the cycle took (in seconds)
//   - attrs: Optional additional attributes to include in the metric
func (m HealthCheckCycleDuration) Record(ctx context.Context, val float64, attrs ...attribute.KeyValue) {
	if len(attrs) == 0 {
		m.Float64Histogram.Record(ctx, val)
		return
	}
	m.Float64Histogram.Record(ctx, val, metric.WithAttributes(attrs...))
}

// NewMetrics initializes OpenTelemetry metrics for the recovery engine.
// If meter is nil, returns noop implementations to avoid nil checks in instrumented code.
func NewMetrics(meter metric.Meter, logger *slog.Logger, poolerStore *store.Store[string, *store.PoolerHealth]) (*Metrics, error) {
	m := &Metrics{}

	// Handle nil meter case - return noop implementations
	if meter == nil {
		m.clusterMetadataRefreshDuration = ClusterMetadataRefreshDuration{noop.Float64Histogram{}}
		m.poolerPollDuration = PoolerPollDuration{noop.Float64Histogram{}}
		m.healthCheckCycleDuration = HealthCheckCycleDuration{noop.Float64Histogram{}}
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
		"multiorch.recovery.cluster_metadata_refresh.duration",
		metric.WithDescription("Duration of cluster metadata refresh operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		logger.Error("failed to create refresh.duration histogram", "error", err)
		return nil, err
	}
	m.clusterMetadataRefreshDuration = ClusterMetadataRefreshDuration{refreshDurationHistogram}

	// Histogram for individual poll duration
	// Following OTel convention: use histogram with status attribute instead of separate counters
	pollDurationHistogram, err := meter.Float64Histogram(
		"multiorch.recovery.pooler_poll.duration",
		metric.WithDescription("Duration of individual pooler health checks"),
		metric.WithUnit("s"),
	)
	if err != nil {
		logger.Error("failed to create poll.duration histogram", "error", err)
		return nil, err
	}
	m.poolerPollDuration = PoolerPollDuration{pollDurationHistogram}

	// Histogram for health check cycle duration
	healthCheckCycleDurationHistogram, err := meter.Float64Histogram(
		"multiorch.recovery.health_check_cycle.duration",
		metric.WithDescription("Duration of complete health check cycles"),
		metric.WithUnit("s"),
	)
	if err != nil {
		logger.Error("failed to create health_check_cycle.duration histogram", "error", err)
		return nil, err
	}
	m.healthCheckCycleDuration = HealthCheckCycleDuration{healthCheckCycleDurationHistogram}

	return m, nil
}

// RecordHealthCheckCycleDuration records the duration of a complete health check cycle.
func (m *Metrics) RecordHealthCheckCycleDuration(ctx context.Context, duration time.Duration, attrs ...attribute.KeyValue) {
	m.healthCheckCycleDuration.Record(ctx, duration.Seconds(), attrs...)
}
