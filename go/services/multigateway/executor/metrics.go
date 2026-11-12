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

package executor

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// executorMetrics holds the executor's OTel metrics.
type executorMetrics struct {
	fenceWaitDuration fenceWaitDuration
}

// fenceWaitDuration wraps a Float64Histogram for recording how long a
// statement a fenced pooler refused spent waiting and running again, and how
// it ended.
type fenceWaitDuration struct {
	metric.Float64Histogram
}

// Record records one fenced statement's wait with proper OTel attributes.
func (m fenceWaitDuration) Record(ctx context.Context, val float64, dbNamespace, outcome string) {
	m.Float64Histogram.Record(ctx, val,
		metric.WithAttributes(
			attribute.String("db.namespace", dbNamespace),
			attribute.String("outcome", outcome),
		))
}

// newExecutorMetrics initialises the executor's OTel metrics. A metric that
// fails to initialise uses a noop implementation and is reported in the
// returned error.
func newExecutorMetrics() (*executorMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway/executor")
	m := &executorMetrics{}
	var errs []error

	fenceWait, err := meter.Float64Histogram(
		"mg.executor.fence.wait.duration",
		metric.WithDescription("Time a statement refused by a fenced pooler spent waiting for a newer routing snapshot and running again, by outcome"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.executor.fence.wait.duration histogram: %w", err))
		m.fenceWaitDuration = fenceWaitDuration{noop.Float64Histogram{}}
	} else {
		m.fenceWaitDuration = fenceWaitDuration{fenceWait}
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}
