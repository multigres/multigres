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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Metrics holds all OpenTelemetry metrics for the recovery engine.
// Following OTel conventions, metrics are defined in a separate file with
// wrapper types that hide attribute key names from instrumented code.
//
// This pattern is inspired by:
// https://github.com/open-telemetry/opentelemetry-go/blob/v1.38.0/semconv/v1.37.0/dbconv/metric.go
type Metrics struct {
	meter                  metric.Meter
	poolerStoreSize        PoolerStoreSize
	recoveryActionDuration RecoveryActionDuration
	errorsTotal            ErrorsTotal
	detectedProblems       DetectedProblems
	streamConnected        StreamConnected
	streamSnapshotsTotal   StreamSnapshotsTotal
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

// RecoveryActionStatus represents the possible status values for a recovery action.
type RecoveryActionStatus string

const (
	RecoveryActionStatusSuccess RecoveryActionStatus = "success"
	RecoveryActionStatusFailure RecoveryActionStatus = "failure"
)

// RecoveryActionDuration wraps a Float64Histogram for recording recovery action durations.
type RecoveryActionDuration struct {
	metric.Float64Histogram
}

// Record records a recovery action duration with proper OTel attributes.
//
// Parameters:
//   - ctx: Context for the metric recording
//   - val: How long the action took (in milliseconds)
//   - actionName: The name of the recovery action (e.g., "FixReplication", "BootstrapShard")
//   - problemCode: The problem code being addressed
//   - status: The action status (success or failure)
//   - dbNamespace: The database name (becomes "db.namespace" attribute per OTel spec)
//   - shard: The shard identifier
func (m RecoveryActionDuration) Record(
	ctx context.Context,
	val float64,
	actionName string,
	problemCode string,
	status RecoveryActionStatus,
	dbNamespace string,
	shard string,
) {
	m.Float64Histogram.Record(ctx, val,
		metric.WithAttributes(
			attribute.String("action", actionName),
			attribute.String("problem_code", problemCode),
			attribute.String("status", string(status)),
			attribute.String("db.namespace", dbNamespace),
			attribute.String("shard", shard),
		))
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

// DetectedProblems wraps an Int64ObservableGauge for observing detected problems by type.
// Use the Inst() method to get the underlying gauge for callback registration.
type DetectedProblems struct {
	metric.Int64ObservableGauge
}

// Inst returns the underlying metric instrument for callback registration.
func (m DetectedProblems) Inst() metric.Int64ObservableGauge {
	return m.Int64ObservableGauge
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
		meter: otel.Meter("github.com/multigres/multigres/go/services/multiorch/recovery"),
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

	// Histogram for recovery action duration
	recoveryActionDurationHistogram, err := m.meter.Float64Histogram(
		"multiorch.recovery.action.duration",
		metric.WithDescription("Duration of recovery action executions"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multiorch.recovery.action.duration histogram: %w", err))
		m.recoveryActionDuration = RecoveryActionDuration{noop.Float64Histogram{}}
	} else {
		m.recoveryActionDuration = RecoveryActionDuration{recoveryActionDurationHistogram}
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

	// Gauge for detected problems by type
	detectedProblemsGauge, err := m.meter.Int64ObservableGauge(
		"multiorch.recovery.detected_problems",
		metric.WithDescription("Current number of detected problems by analysis type"),
		metric.WithUnit("{problem}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multiorch.recovery.detected_problems gauge: %w", err))
		m.detectedProblems = DetectedProblems{noop.Int64ObservableGauge{}}
	} else {
		m.detectedProblems = DetectedProblems{detectedProblemsGauge}
	}

	// Gauge for stream connection status per pooler (1 = connected, 0 = disconnected)
	streamConnectedGauge, err := m.meter.Int64ObservableGauge(
		"multiorch.recovery.stream.connected",
		metric.WithDescription("Whether the ManagerHealthStream stream to the pooler is currently connected (1) or not (0)"),
		metric.WithUnit("{bool}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multiorch.recovery.stream.connected gauge: %w", err))
		m.streamConnected = StreamConnected{noop.Int64ObservableGauge{}}
	} else {
		m.streamConnected = StreamConnected{streamConnectedGauge}
	}

	// Gauge for cumulative snapshots received per pooler
	streamSnapshotsTotalGauge, err := m.meter.Int64ObservableGauge(
		"multiorch.recovery.stream.snapshots_received",
		metric.WithDescription("Cumulative number of health snapshots received from the pooler via ManagerHealthStream"),
		metric.WithUnit("{snapshot}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multiorch.recovery.stream.snapshots_received gauge: %w", err))
		m.streamSnapshotsTotal = StreamSnapshotsTotal{noop.Int64ObservableGauge{}}
	} else {
		m.streamSnapshotsTotal = StreamSnapshotsTotal{streamSnapshotsTotalGauge}
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

// StreamConnected wraps an Int64ObservableGauge that reports 1 when a pooler's
// ManagerHealthStream stream is connected, 0 when disconnected.
// Use the Inst() method to get the underlying gauge for callback registration.
type StreamConnected struct {
	metric.Int64ObservableGauge
}

// Inst returns the underlying metric instrument for callback registration.
func (m StreamConnected) Inst() metric.Int64ObservableGauge {
	return m.Int64ObservableGauge
}

// StreamSnapshotsTotal wraps an Int64ObservableGauge that reports the cumulative
// number of health snapshots received per pooler since the stream was started.
// Use the Inst() method to get the underlying gauge for callback registration.
type StreamSnapshotsTotal struct {
	metric.Int64ObservableGauge
}

// Inst returns the underlying metric instrument for callback registration.
func (m StreamSnapshotsTotal) Inst() metric.Int64ObservableGauge {
	return m.Int64ObservableGauge
}

// StreamHealthData holds per-pooler stream health data for metric observation.
type StreamHealthData struct {
	PoolerID          string
	DBNamespace       string
	Shard             string
	Connected         bool
	SnapshotsReceived int64
}

// DetectedProblemData represents a detected problem with its attributes for metric observation.
// Each problem is tracked per affected entity (pooler ID or shard key).
type DetectedProblemData struct {
	AnalysisType string
	DBNamespace  string
	Shard        string
	EntityID     string
}

// RegisterStreamHealthCallback registers a callback for the stream health gauges.
// The getter is called periodically to observe current stream state for all poolers.
// Both multiorch.recovery.stream.connected and multiorch.recovery.stream.snapshots_received
// are updated in the same callback to keep them consistent.
// Returns an error if callback registration fails.
func (m *Metrics) RegisterStreamHealthCallback(getter func() []StreamHealthData) error {
	if getter == nil {
		return nil
	}
	_, err := m.meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			for _, data := range getter() {
				attrs := metric.WithAttributes(
					attribute.String("pooler_id", data.PoolerID),
					attribute.String("db.namespace", data.DBNamespace),
					attribute.String("shard", data.Shard),
				)
				connected := int64(0)
				if data.Connected {
					connected = 1
				}
				observer.ObserveInt64(m.streamConnected.Inst(), connected, attrs)
				observer.ObserveInt64(m.streamSnapshotsTotal.Inst(), data.SnapshotsReceived, attrs)
			}
			return nil
		},
		m.streamConnected.Inst(),
		m.streamSnapshotsTotal.Inst(),
	)
	return err
}

// RegisterDetectedProblemsCallback registers a callback for the detected problems observable gauge.
// The getter function is called periodically to observe current detected problems.
// Each problem is reported with value=1 per pooler.
// Returns an error if callback registration fails.
func (m *Metrics) RegisterDetectedProblemsCallback(getter func() []DetectedProblemData) error {
	if getter == nil {
		return nil
	}
	_, err := m.meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			for _, data := range getter() {
				observer.ObserveInt64(m.detectedProblems.Inst(), 1,
					metric.WithAttributes(
						attribute.String("analysis_type", data.AnalysisType),
						attribute.String("db.namespace", data.DBNamespace),
						attribute.String("shard", data.Shard),
						attribute.String("entity_id", data.EntityID),
					))
			}
			return nil
		},
		m.detectedProblems.Inst(),
	)
	return err
}
