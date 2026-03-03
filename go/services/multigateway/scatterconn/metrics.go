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

package scatterconn

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// ScatterStatus represents the outcome of a shard execution for metric attribution.
type ScatterStatus string

const (
	ScatterStatusOK    ScatterStatus = "ok"
	ScatterStatusError ScatterStatus = "error"
)

// ScatterMetrics holds all OTel metrics for scatterconn shard execution.
// Follows the same wrapper-type pattern as recovery/metrics.go.
type ScatterMetrics struct {
	executeDuration ExecuteDuration
	executeErrors   ExecuteErrors
}

// ExecuteDuration wraps a Float64Histogram for recording shard execution durations.
type ExecuteDuration struct {
	metric.Float64Histogram
}

// Record records a shard execution duration with proper OTel attributes.
func (m ExecuteDuration) Record(
	ctx context.Context,
	val float64,
	dbNamespace string,
	tablegroup string,
	shard string,
	status ScatterStatus,
) {
	m.Float64Histogram.Record(ctx, val,
		metric.WithAttributes(
			attribute.String("db.namespace", dbNamespace),
			attribute.String("tablegroup", tablegroup),
			attribute.String("shard", shard),
			attribute.String("status", string(status)),
		))
}

// ExecuteErrors wraps an Int64Counter for counting shard execution errors.
type ExecuteErrors struct {
	metric.Int64Counter
}

// Add increments the shard error counter with proper OTel attributes.
func (m ExecuteErrors) Add(
	ctx context.Context,
	tablegroup string,
	shard string,
	errorType string,
) {
	m.Int64Counter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("tablegroup", tablegroup),
			attribute.String("shard", shard),
			attribute.String("error.type", errorType),
		))
}

// NewScatterMetrics initialises OTel metrics for scatterconn.
// Individual metrics that fail to initialise use noop implementations
// and are included in the returned error.
func NewScatterMetrics() (*ScatterMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway/scatterconn")
	m := &ScatterMetrics{}
	var errs []error

	dur, err := meter.Float64Histogram(
		"mg.scatter.execute.duration",
		metric.WithDescription("Duration of shard-level query execution"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5, 10, 30, 60),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.scatter.execute.duration histogram: %w", err))
		m.executeDuration = ExecuteDuration{noop.Float64Histogram{}}
	} else {
		m.executeDuration = ExecuteDuration{dur}
	}

	errCounter, err := meter.Int64Counter(
		"mg.scatter.execute.errors",
		metric.WithDescription("Total number of shard-level execution errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.scatter.execute.errors counter: %w", err))
		m.executeErrors = ExecuteErrors{noop.Int64Counter{}}
	} else {
		m.executeErrors = ExecuteErrors{errCounter}
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}
