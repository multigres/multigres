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

package handler

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// QueryStatus represents the outcome of a query for metric attribution.
type QueryStatus string

const (
	QueryStatusOK    QueryStatus = "ok"
	QueryStatusError QueryStatus = "error"
)

// HandlerMetrics holds all OTel metrics for the multigateway handler.
// Following the recovery/metrics.go pattern: wrapper types embedding OTel
// instruments with typed Record() methods that hide attribute key names.
type HandlerMetrics struct {
	queryDuration QueryDuration
	queryErrors   QueryErrors
	rowsReturned  RowsReturned
}

// QueryDuration wraps a Float64Histogram for recording query durations.
type QueryDuration struct {
	metric.Float64Histogram
}

// Record records a query duration with proper OTel attributes.
func (m QueryDuration) Record(
	ctx context.Context,
	val float64,
	dbNamespace string,
	operationName string,
	protocol string,
	errorType string,
	status QueryStatus,
) {
	m.Float64Histogram.Record(ctx, val,
		metric.WithAttributes(
			attribute.String("db.namespace", dbNamespace),
			attribute.String("db.operation.name", operationName),
			attribute.String("db.query.protocol", protocol),
			attribute.String("error.type", errorType),
			attribute.String("status", string(status)),
		))
}

// QueryErrors wraps an Int64Counter for counting query errors.
type QueryErrors struct {
	metric.Int64Counter
}

// Add increments the error counter with proper OTel attributes.
func (m QueryErrors) Add(
	ctx context.Context,
	errorType string,
	errorSource string,
	dbNamespace string,
	operationName string,
) {
	m.Int64Counter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("error.type", errorType),
			attribute.String("error.source", errorSource),
			attribute.String("db.namespace", dbNamespace),
			attribute.String("db.operation.name", operationName),
		))
}

// RowsReturned wraps a Float64Histogram for recording row counts.
type RowsReturned struct {
	metric.Float64Histogram
}

// Record records a row count with proper OTel attributes.
func (m RowsReturned) Record(
	ctx context.Context,
	val float64,
	dbNamespace string,
	operationName string,
) {
	m.Float64Histogram.Record(ctx, val,
		metric.WithAttributes(
			attribute.String("db.namespace", dbNamespace),
			attribute.String("db.operation.name", operationName),
		))
}

// NewHandlerMetrics initialises OTel metrics for the handler.
// Individual metrics that fail to initialise use noop implementations
// and are included in the returned error.
func NewHandlerMetrics() (*HandlerMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway/handler")
	m := &HandlerMetrics{}
	var errs []error

	dur, err := meter.Float64Histogram(
		"mg.gateway.query.duration",
		metric.WithDescription("Duration of gateway query execution"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.query.duration histogram: %w", err))
		m.queryDuration = QueryDuration{noop.Float64Histogram{}}
	} else {
		m.queryDuration = QueryDuration{dur}
	}

	errCounter, err := meter.Int64Counter(
		"mg.gateway.query.errors",
		metric.WithDescription("Total number of query errors at the gateway"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.query.errors counter: %w", err))
		m.queryErrors = QueryErrors{noop.Int64Counter{}}
	} else {
		m.queryErrors = QueryErrors{errCounter}
	}

	rows, err := meter.Float64Histogram(
		"mg.gateway.query.rows_returned",
		metric.WithDescription("Number of rows returned by queries"),
		metric.WithUnit("{row}"),
		metric.WithExplicitBucketBoundaries(0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.query.rows_returned histogram: %w", err))
		m.rowsReturned = RowsReturned{noop.Float64Histogram{}}
	} else {
		m.rowsReturned = RowsReturned{rows}
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}
