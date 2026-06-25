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
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
)

// pool_type attribute values: which kind of backend connection served the query.
const (
	poolTypeRegular  = "regular"
	poolTypeReserved = "reserved"
)

// queryStats holds OpenTelemetry metrics for pooler-side query observability.
//
// These are the per-query counterpart to the gateway's mg.gateway.query.*
// metrics: where the gateway sees total wall-clock time (including the gRPC
// hop), these isolate the pooler's own execution time and error counts. The
// gap between mg.gateway.query.exec.duration and mg.pooler.query.duration
// approximates the network/queue cost of the gateway→pooler hop. The standard
// rpc.server.* metrics (from the servenv otelgrpc stats handler) cover the gRPC
// surface; these add the query-level breakdown otelgrpc cannot: pool_type,
// SQLSTATE, and error source.
type queryStats struct {
	meter metric.Meter

	duration    metric.Float64Histogram // mg.pooler.query.duration               — attrs: pool_type, status
	errors      metric.Int64Counter     // mg.pooler.query.errors                 — attrs: sqlstate, error_source, pool_type
	rows        metric.Float64Histogram // mg.pooler.query.rows                   — attrs: pool_type
	poolAcquire metric.Float64Histogram // mg.pooler.query.pool_acquire.duration  — attrs: pool_type, outcome
}

func newQueryStats() *queryStats {
	s := &queryStats{
		meter: otel.Meter("github.com/multigres/multigres/go/services/multipooler/internal/executor"),
	}

	var err error

	s.duration, err = s.meter.Float64Histogram(
		"mg.pooler.query.duration",
		metric.WithDescription("Pooler-side query execution time (excludes gRPC framing)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)
	if err != nil {
		s.duration = noop.Float64Histogram{}
	}

	s.errors, err = s.meter.Int64Counter(
		"mg.pooler.query.errors",
		metric.WithDescription("Pooler-side query errors by SQLSTATE and source"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		s.errors = noop.Int64Counter{}
	}

	s.rows, err = s.meter.Float64Histogram(
		"mg.pooler.query.rows",
		metric.WithDescription("Rows produced per query at the pooler"),
		metric.WithUnit("{row}"),
		metric.WithExplicitBucketBoundaries(0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000),
	)
	if err != nil {
		s.rows = noop.Float64Histogram{}
	}

	s.poolAcquire, err = s.meter.Float64Histogram(
		"mg.pooler.query.pool_acquire.duration",
		metric.WithDescription("Time to acquire a server connection from the pool before query execution"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
	)
	if err != nil {
		s.poolAcquire = noop.Float64Histogram{}
	}

	return s
}

// acquireOutcome classifies a pool-acquire result for the outcome attribute:
// "acquired" on success, "timeout" when the pool wait or context deadline
// expired, "error" otherwise.
func acquireOutcome(err error) string {
	switch {
	case err == nil:
		return "acquired"
	case errors.Is(err, connpool.ErrTimeout),
		errors.Is(err, connpool.ErrCtxTimeout),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		return "timeout"
	default:
		return "error"
	}
}

// recordPoolAcquire records the latency and outcome of acquiring a server
// connection from poolType's pool.
func (s *queryStats) recordPoolAcquire(ctx context.Context, poolType string, d time.Duration, err error) {
	s.poolAcquire.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("pool_type", poolType),
		attribute.String("outcome", acquireOutcome(err)),
	))
}

// recordQuery records the outcome of a single query served on poolType. rows is
// the number of rows produced (0 on error or for non-row-returning statements).
// err is the post-execution error, if any; nil means success.
func (s *queryStats) recordQuery(ctx context.Context, poolType string, d time.Duration, rows int64, err error) {
	status := "ok"
	if err != nil {
		status = "error"
		s.errors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("sqlstate", mterrors.ExtractSQLSTATE(err)),
			attribute.String("error_source", mterrors.ClassifyErrorSource(err)),
			attribute.String("pool_type", poolType),
		))
	}

	s.duration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("pool_type", poolType),
		attribute.String("status", status),
	))
	s.rows.Record(ctx, float64(rows), metric.WithAttributes(
		attribute.String("pool_type", poolType),
	))
}
