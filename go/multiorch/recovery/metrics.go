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
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
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
	meter                          metric.Meter
	poolerStoreSize                PoolerStoreSize
	clusterMetadataRefreshDuration ClusterMetadataRefreshDuration
	poolerPollDuration             PoolerPollDuration
	healthCheckCycleDuration       HealthCheckCycleDuration
	errorsTotal                    ErrorsTotal
}

// PoolerStoreSize wraps an Int64ObservableGauge for observing pooler store size.
// Use the Inst() method to get the underlying gauge for callback registration.
type PoolerStoreSize struct {
	metric.Int64ObservableGauge
}

// Inst returns the underlying metric instrument for callback registration.
func (m PoolerStoreSize) Inst() metric.Int64ObservableGauge {
	return m.Int64ObservableGauge
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

// ErrorsTotal wraps an Int64Counter for counting errors in the recovery engine.
// Use the Add method to increment the counter with source information.
type ErrorsTotal struct {
	metric.Int64Counter
}

// Add increments the error counter with the given source component.
//
// Parameters:
//   - ctx: Context for the metric recording
//   - source: The component that produced the error (e.g., "analyzer", "recovery_action")
//   - attrs: Optional additional attributes to include in the metric
func (m ErrorsTotal) Add(ctx context.Context, source string, attrs ...attribute.KeyValue) {
	m.Int64Counter.Add(ctx, 1,
		metric.WithAttributes(
			append(
				attrs,
				attribute.String("source", source),
			)...,
		))
}

// NewMetrics initializes OpenTelemetry metrics for the recovery engine.
// Individual metrics that fail to initialize will use noop implementations and be included
// in the returned error. For the poolerStoreSize observable gauge, use
// RegisterPoolerStoreSizeCallback() to register a callback.
//
// Returns a Metrics instance (with noop fallbacks for failed metrics) and any initialization
// errors that occurred. The caller should log or handle these errors as appropriate.
func NewMetrics() (*Metrics, error) {
	m := &Metrics{
		meter: otel.Meter("github.com/multigres/multigres/go/multiorch/recovery"),
	}

	var errs []error

	// Gauge for current pooler store size
	poolerStoreSizeGauge, err := m.meter.Int64ObservableGauge(
		"multiorch.recovery.pooler.store.size",
		metric.WithDescription("Current number of poolers tracked in the recovery engine store"),
		metric.WithUnit("{pooler}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multiorch.recovery.pooler.store.size gauge: %w", err))
		m.poolerStoreSize = PoolerStoreSize{noop.Int64ObservableGauge{}}
	} else {
		m.poolerStoreSize = PoolerStoreSize{poolerStoreSizeGauge}
	}

	// Histogram for cluster metadata refresh duration
	refreshDurationHistogram, err := m.meter.Float64Histogram(
		"multiorch.recovery.cluster_metadata.refresh.duration",
		metric.WithDescription("Duration of cluster metadata refresh operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multiorch.recovery.cluster_metadata.refresh.duration histogram: %w", err))
		m.clusterMetadataRefreshDuration = ClusterMetadataRefreshDuration{noop.Float64Histogram{}}
	} else {
		m.clusterMetadataRefreshDuration = ClusterMetadataRefreshDuration{refreshDurationHistogram}
	}

	// Histogram for individual poll duration
	// Following OTel convention: use histogram with status attribute instead of separate counters
	pollDurationHistogram, err := m.meter.Float64Histogram(
		"multiorch.recovery.pooler_poll.duration",
		metric.WithDescription("Duration of individual pooler health checks"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multiorch.recovery.pooler_poll.duration histogram: %w", err))
		m.poolerPollDuration = PoolerPollDuration{noop.Float64Histogram{}}
	} else {
		m.poolerPollDuration = PoolerPollDuration{pollDurationHistogram}
	}

	// Histogram for health check cycle duration
	healthCheckCycleDurationHistogram, err := m.meter.Float64Histogram(
		"multiorch.recovery.health_check_cycle.duration",
		metric.WithDescription("Duration of complete health check cycles"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multiorch.recovery.health_check_cycle.duration histogram: %w", err))
		m.healthCheckCycleDuration = HealthCheckCycleDuration{noop.Float64Histogram{}}
	} else {
		m.healthCheckCycleDuration = HealthCheckCycleDuration{healthCheckCycleDurationHistogram}
	}

	// Counter for errors
	errorsTotalCounter, err := m.meter.Int64Counter(
		"multiorch.recovery.errors.total",
		metric.WithDescription("Total number of errors in the recovery engine"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multiorch.recovery.errors.total counter: %w", err))
		m.errorsTotal = ErrorsTotal{noop.Int64Counter{}}
	} else {
		m.errorsTotal = ErrorsTotal{errorsTotalCounter}
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}

// RegisterPoolerStoreSizeCallback registers a callback for the pooler store size observable gauge.
// The poolerStoreGetter function is called periodically to observe the current store size.
// Returns an error if callback registration fails.
func (m *Metrics) RegisterPoolerStoreSizeCallback(poolerStoreGetter func() int) error {
	if poolerStoreGetter == nil {
		return nil
	}
	_, err := m.meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			observer.ObserveInt64(m.poolerStoreSize.Inst(), int64(poolerStoreGetter()))
			return nil
		},
		m.poolerStoreSize.Inst(),
	)
	return err
}

// RecordHealthCheckCycleDuration records the duration of a complete health check cycle.
func (m *Metrics) RecordHealthCheckCycleDuration(ctx context.Context, duration time.Duration, attrs ...attribute.KeyValue) {
	m.healthCheckCycleDuration.Record(ctx, duration.Seconds(), attrs...)
}
