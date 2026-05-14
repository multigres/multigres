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
	"sync"

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
	queryDuration *QueryDuration
	queryErrors   *QueryErrors
	rowsReturned  *RowsReturned
	tableQueries  *TableQueries
}

// The per-instrument caches below memoise the OTel MeasurementOption built
// from a fixed set of attribute values. Building one requires sorting and
// deduping the attribute slice inside the SDK; the input dimensions for a
// given (db, op, ...) tuple are stable across millions of queries, so we pay
// that cost once per tuple. Cardinality is bounded by the query registry
// (fingerprint labels are capped and collapsed into __other__/__utility__),
// by the finite SQLSTATE space, and by the fact that db/table/op names come
// from authenticated PG protocol fields — no explicit cap needed.

type queryDurationKey struct {
	db, op, proto, errType, status, fp string
}

type queryErrorsKey struct {
	errType, errSource, db, op, fp string
}

type rowsReturnedKey struct {
	db, op, fp string
}

type tableQueriesKey struct {
	db, table, op string
}

// QueryDuration wraps a Float64Histogram for recording query durations.
type QueryDuration struct {
	metric.Float64Histogram
	optsCache sync.Map // queryDurationKey -> metric.MeasurementOption
}

// Record records a query duration with proper OTel attributes.
func (m *QueryDuration) Record(
	ctx context.Context,
	val float64,
	dbNamespace string,
	operationName string,
	protocol string,
	errorType string,
	status QueryStatus,
	queryFingerprint string,
) {
	key := queryDurationKey{
		db:      dbNamespace,
		op:      operationName,
		proto:   protocol,
		errType: errorType,
		status:  string(status),
		fp:      queryFingerprint,
	}
	opt := m.optionFor(key)
	m.Float64Histogram.Record(ctx, val, opt)
}

func (m *QueryDuration) optionFor(key queryDurationKey) metric.MeasurementOption {
	if v, ok := m.optsCache.Load(key); ok {
		return v.(metric.MeasurementOption)
	}
	set := attribute.NewSet(
		attribute.String("db.namespace", key.db),
		attribute.String("db.operation.name", key.op),
		attribute.String("db.query.protocol", key.proto),
		attribute.String("error.type", key.errType),
		attribute.String("status", key.status),
		attribute.String("query.fingerprint", key.fp),
	)
	opt := metric.WithAttributeSet(set)
	actual, _ := m.optsCache.LoadOrStore(key, opt)
	return actual.(metric.MeasurementOption)
}

// QueryErrors wraps an Int64Counter for counting query errors.
type QueryErrors struct {
	metric.Int64Counter
	optsCache sync.Map // queryErrorsKey -> metric.MeasurementOption
}

// Add increments the error counter with proper OTel attributes.
func (m *QueryErrors) Add(
	ctx context.Context,
	errorType string,
	errorSource string,
	dbNamespace string,
	operationName string,
	queryFingerprint string,
) {
	key := queryErrorsKey{
		errType:   errorType,
		errSource: errorSource,
		db:        dbNamespace,
		op:        operationName,
		fp:        queryFingerprint,
	}
	opt := m.optionFor(key)
	m.Int64Counter.Add(ctx, 1, opt)
}

func (m *QueryErrors) optionFor(key queryErrorsKey) metric.MeasurementOption {
	if v, ok := m.optsCache.Load(key); ok {
		return v.(metric.MeasurementOption)
	}
	set := attribute.NewSet(
		attribute.String("error.type", key.errType),
		attribute.String("error.source", key.errSource),
		attribute.String("db.namespace", key.db),
		attribute.String("db.operation.name", key.op),
		attribute.String("query.fingerprint", key.fp),
	)
	opt := metric.WithAttributeSet(set)
	actual, _ := m.optsCache.LoadOrStore(key, opt)
	return actual.(metric.MeasurementOption)
}

// RowsReturned wraps a Float64Histogram for recording row counts.
type RowsReturned struct {
	metric.Float64Histogram
	optsCache sync.Map // rowsReturnedKey -> metric.MeasurementOption
}

// Record records a row count with proper OTel attributes.
func (m *RowsReturned) Record(
	ctx context.Context,
	val float64,
	dbNamespace string,
	operationName string,
	queryFingerprint string,
) {
	key := rowsReturnedKey{db: dbNamespace, op: operationName, fp: queryFingerprint}
	opt := m.optionFor(key)
	m.Float64Histogram.Record(ctx, val, opt)
}

func (m *RowsReturned) optionFor(key rowsReturnedKey) metric.MeasurementOption {
	if v, ok := m.optsCache.Load(key); ok {
		return v.(metric.MeasurementOption)
	}
	set := attribute.NewSet(
		attribute.String("db.namespace", key.db),
		attribute.String("db.operation.name", key.op),
		attribute.String("query.fingerprint", key.fp),
	)
	opt := metric.WithAttributeSet(set)
	actual, _ := m.optsCache.LoadOrStore(key, opt)
	return actual.(metric.MeasurementOption)
}

// TableQueries wraps an Int64Counter for counting queries per table.
type TableQueries struct {
	metric.Int64Counter
	optsCache sync.Map // tableQueriesKey -> metric.MeasurementOption
}

// Add increments the per-table query counter with proper OTel attributes.
func (m *TableQueries) Add(
	ctx context.Context,
	dbNamespace string,
	tableName string,
	operationName string,
) {
	key := tableQueriesKey{db: dbNamespace, table: tableName, op: operationName}
	opt := m.optionFor(key)
	m.Int64Counter.Add(ctx, 1, opt)
}

func (m *TableQueries) optionFor(key tableQueriesKey) metric.MeasurementOption {
	if v, ok := m.optsCache.Load(key); ok {
		return v.(metric.MeasurementOption)
	}
	set := attribute.NewSet(
		attribute.String("db.namespace", key.db),
		attribute.String("db.collection.name", key.table),
		attribute.String("db.operation.name", key.op),
	)
	opt := metric.WithAttributeSet(set)
	actual, _ := m.optsCache.LoadOrStore(key, opt)
	return actual.(metric.MeasurementOption)
}

// NewHandlerMetrics initialises OTel metrics for the handler.
// Individual metrics that fail to initialise use noop implementations
// and are included in the returned error.
func NewHandlerMetrics() (*HandlerMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway/handler")
	m := &HandlerMetrics{
		queryDuration: &QueryDuration{},
		queryErrors:   &QueryErrors{},
		rowsReturned:  &RowsReturned{},
		tableQueries:  &TableQueries{},
	}
	var errs []error

	dur, err := meter.Float64Histogram(
		"mg.gateway.query.duration",
		metric.WithDescription("Duration of gateway query execution"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.query.duration histogram: %w", err))
		m.queryDuration.Float64Histogram = noop.Float64Histogram{}
	} else {
		m.queryDuration.Float64Histogram = dur
	}

	errCounter, err := meter.Int64Counter(
		"mg.gateway.query.errors",
		metric.WithDescription("Total number of query errors at the gateway"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.query.errors counter: %w", err))
		m.queryErrors.Int64Counter = noop.Int64Counter{}
	} else {
		m.queryErrors.Int64Counter = errCounter
	}

	rows, err := meter.Float64Histogram(
		"mg.gateway.query.rows_returned",
		metric.WithDescription("Number of rows returned by queries"),
		metric.WithUnit("{row}"),
		metric.WithExplicitBucketBoundaries(0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.query.rows_returned histogram: %w", err))
		m.rowsReturned.Float64Histogram = noop.Float64Histogram{}
	} else {
		m.rowsReturned.Float64Histogram = rows
	}

	tq, err := meter.Int64Counter(
		"mg.gateway.query.table_queries",
		metric.WithDescription("Query count per table"),
		metric.WithUnit("{query}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.query.table_queries counter: %w", err))
		m.tableQueries.Int64Counter = noop.Int64Counter{}
	} else {
		m.tableQueries.Int64Counter = tq
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}
