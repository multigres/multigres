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

package connpool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/semconv/v1.37.0/dbconv"
)

// Attribute keys from OTel semantic conventions:
// - semconv.DBClientConnectionPoolNameKey = "db.client.connection.pool.name"
// - semconv.DBClientConnectionStateKey = "db.client.connection.state"
const (
	attrKeyPoolName = "db.client.connection.pool.name"
	attrKeyState    = "db.client.connection.state"
)

// ConnectionCount wraps an Int64UpDownCounter for tracking connection counts by state.
// This is a workaround for a bug in dbconv.ClientConnectionCount where pool name and
// state attributes aren't added when no extra attributes are provided.
// Consider contributing a fix upstream to opentelemetry-go/semconv/v1.37.0/dbconv.
type ConnectionCount struct {
	counter metric.Int64UpDownCounter
}

// NewConnectionCount creates a ConnectionCount instrument using the standard
// db.client.connection.count metric name and description from OTel semconv.
func NewConnectionCount(m metric.Meter) (ConnectionCount, error) {
	// Metric name and description from dbconv.ClientConnectionCount
	counter, err := m.Int64UpDownCounter(
		"db.client.connection.count",
		metric.WithDescription("The number of connections that are currently in state described by the state attribute."),
		metric.WithUnit("{connection}"),
	)
	return ConnectionCount{counter: counter}, err
}

// Add records a connection count change for the given pool and state.
func (c ConnectionCount) Add(ctx context.Context, delta int64, poolName string, state dbconv.ClientConnectionStateAttr) {
	if c.counter == nil {
		return
	}
	c.counter.Add(ctx, delta, metric.WithAttributes(
		attribute.String(attrKeyPoolName, poolName),
		attribute.String(attrKeyState, string(state)),
	))
}

// ServerConnMetrics records PostgreSQL server-connection lifecycle events:
// establishment count, establishment failures, and setup latency. Created once
// by the pool owner (connpoolmanager) and shared across pools; the bounded
// pool_type attribute (regular/reserved/admin) is supplied per call so the raw,
// per-user pool Name (high cardinality) is never used as a metric dimension.
type ServerConnMetrics struct {
	opened        metric.Int64Counter
	openErrors    metric.Int64Counter
	setupDuration metric.Float64Histogram
}

// NewServerConnMetrics creates the server-connection lifecycle instruments.
// Instruments that fail to initialise fall back to noop and the joined error is
// returned for logging.
func NewServerConnMetrics(m metric.Meter) (ServerConnMetrics, error) {
	var s ServerConnMetrics
	var errs []error
	var err error

	s.opened, err = m.Int64Counter(
		"mg.pooler.server_conn.opened",
		metric.WithDescription("New PostgreSQL server connections established"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.server_conn.opened counter: %w", err))
		s.opened = noop.Int64Counter{}
	}

	s.openErrors, err = m.Int64Counter(
		"mg.pooler.server_conn.open_errors",
		metric.WithDescription("Failures establishing a PostgreSQL server connection"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.server_conn.open_errors counter: %w", err))
		s.openErrors = noop.Int64Counter{}
	}

	s.setupDuration, err = m.Float64Histogram(
		"mg.pooler.server_conn.setup.duration",
		metric.WithDescription("Time to establish a PostgreSQL server connection (connect + auth)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.server_conn.setup.duration histogram: %w", err))
		s.setupDuration = noop.Float64Histogram{}
	}

	return s, errors.Join(errs...)
}

// RecordOpen records a successful connection establishment and its setup latency.
func (s ServerConnMetrics) RecordOpen(ctx context.Context, poolType string, d time.Duration) {
	if s.opened == nil {
		return
	}
	attr := metric.WithAttributes(attribute.String("pool_type", poolType))
	s.opened.Add(ctx, 1, attr)
	if s.setupDuration != nil {
		s.setupDuration.Record(ctx, d.Seconds(), attr)
	}
}

// RecordOpenError records a failed connection establishment, classified by reason.
func (s ServerConnMetrics) RecordOpenError(ctx context.Context, poolType string, err error) {
	if s.openErrors == nil {
		return
	}
	s.openErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("pool_type", poolType),
		attribute.String("reason", openErrorReason(err)),
	))
}

// openErrorReason maps a connection-establishment error to a bounded reason
// label: "timeout" for context-deadline/cancel, "error" otherwise.
func openErrorReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "timeout"
	default:
		return "error"
	}
}
